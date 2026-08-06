import time
import os as _os
_ARIAL = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), 'utils', 'arial.ttf')
import os
import sys
import statistics
from typing import Tuple
from collections import defaultdict, deque

# 이 스크립트는 원래 /workspace/sangrak/perception_models 에서 실행되도록 작성됨.
# laboratory/ 하위에서 실행하더라도 동일하게 동작하도록 작업 디렉토리를 프로젝트 루트로 고정.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
if os.getcwd() != PROJECT_ROOT:
    os.chdir(PROJECT_ROOT)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import torch
import torchvision
import cv2
from PIL import Image, ImageDraw, ImageFont
import numpy as np
import argparse
import math

import utils.utils as utils
from utils.trt_load import TRTInference
from utils.trt_utils import preprocess_image

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

# 카테고리별 색상 (utils.GRAPH_COLOR_MAP / draw color_map과 동일하게 유지)
CATEGORY_COLOR_MAP = {
    0: (255, 255, 255),
    1: (0, 255, 0),
    2: (255, 0, 0),
    3: (0, 0, 255),
    4: (210, 105, 30),
    5: (127, 127, 127),
    6: (255, 0, 255),
}

NORMAL_CATEGORY_ID = 0


def compute_std_iou(mean_a: float, std_a: float, mean_b: float, std_b: float) -> float:
    """
    1D 구간 [mean-std, mean+std] 두 개의 IoU.
    교집합/합집합 모두 0이거나, 두 구간이 겹치지 않으면 0.0 반환.
    """
    lo_a, hi_a = mean_a - std_a, mean_a + std_a
    lo_b, hi_b = mean_b - std_b, mean_b + std_b
    inter = max(0.0, min(hi_a, hi_b) - max(lo_a, lo_b))
    union = (hi_a - lo_a) + (hi_b - lo_b) - inter
    return float(inter / union) if union > 0 else 0.0


def compute_hist_iou(hist_a: np.ndarray, hist_b: np.ndarray) -> float:
    """
    동일한 bin edges를 가진 두 히스토그램(비율 정규화 권장)의 면적 기반 IoU.
    교집합 = sum(min), 합집합 = sum(max). 두 분포가 완전히 분리되면 0.
    """
    if hist_a is None or hist_b is None:
        return 0.0
    inter = float(np.minimum(hist_a, hist_b).sum())
    union = float(np.maximum(hist_a, hist_b).sum())
    return (inter / union) if union > 0 else 0.0


def _to_chw(frame: torch.Tensor) -> torch.Tensor:
    """(H,W,C) 또는 (C,H,W) 텐서를 (C,H,W)로 강제 변환."""
    if frame.ndim != 3:
        raise ValueError("frame must be 3D.")
    if frame.shape[0] in (1, 3, 4):
        return frame.contiguous()
    if frame.shape[-1] in (1, 3, 4):
        return frame.permute(2, 0, 1).contiguous()
    raise ValueError("frame must be (C,H,W) or (H,W,C).")


def torch_crop_region(frame: torch.Tensor, poly_np: np.array, pad_color: Tuple[int] = (114, 114, 114)) -> torch.Tensor:
    """ROI의 bounding box로 잘라낸 뒤, 잘라낸 crop 내부에서 ROI 바깥(~mask)을 pad_color로 채움."""
    chw = _to_chw(frame)
    C, H, W = chw.shape
    device, dtype = chw.device, chw.dtype

    poly = np.asarray(poly_np, dtype=np.float32)
    if poly.ndim != 2 or poly.shape[1] != 2:
        raise ValueError("region polygon must have shape (N,2)")

    x_min = int(np.floor(max(0, poly[:, 0].min())))
    y_min = int(np.floor(max(0, poly[:, 1].min())))
    x_max = int(np.ceil(min(float(W - 1), poly[:, 0].max())))
    y_max = int(np.ceil(min(float(H - 1), poly[:, 1].max())))
    if x_max < x_min or y_max < y_min:
        return chw

    crop = chw[:, y_min:y_max + 1, x_min:x_max + 1].clone()
    h, w = crop.shape[1], crop.shape[2]

    poly_x = torch.as_tensor(poly[:, 0] - x_min, device=device, dtype=torch.float32)
    poly_y = torch.as_tensor(poly[:, 1] - y_min, device=device, dtype=torch.float32)
    n = poly_x.numel()

    xs = torch.arange(w, device=device, dtype=torch.float32)
    ys = torch.arange(h, device=device, dtype=torch.float32)
    grid_x, grid_y = torch.meshgrid(xs, ys, indexing='xy')
    px = grid_x.reshape(-1)
    py = grid_y.reshape(-1)

    inside = torch.zeros_like(px, dtype=torch.bool)
    xj, yj = poly_x[-1], poly_y[-1]
    eps = 1e-12
    for i in range(n):
        xi, yi = poly_x[i], poly_y[i]
        cond = ((yi > py) != (yj > py)) & (px < (xj - xi) * (py - yi) / (yj - yi + eps) + xi)
        inside ^= cond
        xj, yj = xi, yi
    mask = inside.reshape(h, w)

    bg = torch.tensor(pad_color, device=device, dtype=dtype)
    if C == 1:
        crop[0][~mask] = bg[0]
    else:
        for c in range(min(C, 3)):
            crop[c][~mask] = bg[c]

    return crop


# ============================================================================
# 알림 로직 (그대로 유지)
# ============================================================================
class PEEventStateManager:
    def __init__(self, categories, queue_size=4, threshold=2):
        self.queue_size = queue_size
        self.threshold = threshold
        self.duration_queue = {cat: deque([0]*queue_size, maxlen=self.queue_size) for cat in categories}

    def update(self, predicts):
        alarms = []
        for cat, dq in self.duration_queue.items():
            dq.append(1 if cat in predicts and cat != 0 else 0)
            if sum(dq) >= self.threshold:
                alarms.append(cat)
        return alarms
# ============================================================================


def render_distribution_panel(
    scores_by_category: dict,
    class_dict: dict,
    width: int,
    height: int,
    num_bins: int = 80,
    ious_per_cat: dict = None,
    overlaps_per_cat: dict = None,
    iou_threshold: float = None,
    iou_mode: str = 'std',
    normal_id: int = NORMAL_CATEGORY_ID,
) -> Image.Image:
    """
    프레임별 카테고리별 유사도 분포를 하나의 검정 배경 패널에 그려서 반환.
    - 모든 카테고리를 같은 축 위에 라인 히스토그램으로 오버레이.
    - 카테고리별 색은 CATEGORY_COLOR_MAP을 그대로 사용.
    - y축은 '카테고리 내 비율'(counts / total_in_category) 로 정규화되어 카테고리간 직접 비교 가능.
    - 카테고리별 mean / mean±std 를 그래프 내부에 시각적으로 오버레이
        · mean: 카테고리 색의 점선 수직선 (플롯 전구간)
        · mean±std: 플롯 상단의 가로 휘스커(─o─, 카테고리별 색)
    - normal과의 std-bar 겹침 영역(overlaps_per_cat)을 플롯 하단 안쪽에 카테고리 색의 띠로 표시.
    - 우측 상단 통계에는 카테고리별 (n, mean, std, min, max) + 이벤트 카테고리는 IoU(vs normal) 도 표기.
    - 통계 텍스트는 영상 너비를 넘지 않도록 폰트 크기를 자동으로 줄여 그림 (잘림 방지).
    """
    ious_per_cat = ious_per_cat or {}
    overlaps_per_cat = overlaps_per_cat or {}
    img = Image.new('RGB', (max(width, 1), max(height, 1)), color=(0, 0, 0))
    if width <= 1 or height <= 1:
        return img
    drawer = ImageDraw.Draw(img)

    try:
        font_title = ImageFont.truetype(_ARIAL, size=max(12, int(height / 22)))
        font_axis = ImageFont.truetype(_ARIAL, size=max(10, int(height / 32)))
        font_stat = ImageFont.truetype(_ARIAL, size=max(11, int(height / 28)))
    except Exception:
        font_title = ImageFont.load_default()
        font_axis = ImageFont.load_default()
        font_stat = ImageFont.load_default()

    valid = [s for s in scores_by_category.values() if len(s) > 0]
    if len(valid) == 0:
        drawer.text((10, 10), "No scores yet", fill=(255, 255, 255), font=font_title)
        return img
    flat = np.concatenate(valid)
    x_min = float(flat.min())
    x_max = float(flat.max())
    if x_max - x_min < 1e-6:
        x_max = x_min + 1e-6
    rng = x_max - x_min
    x_min -= rng * 0.05
    x_max += rng * 0.05

    # 통계 텍스트(IoU 포함)가 들어갈 우측 컬럼 폭. 잘림 방지를 위해 최소 폭을 넉넉히.
    stat_w = max(380, int(width * 0.34))
    pad_l, pad_r, pad_t, pad_b = 56, stat_w + 24, 30, 40
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b
    if plot_w <= 5 or plot_h <= 5:
        return img

    bin_edges = np.linspace(x_min, x_max, num_bins + 1)

    # 카테고리 내 비율 히스토그램 (sum -> 1)
    hist_by_cat = {}
    stats_by_cat = {}
    max_ratio = 0.0
    for cat_id, scores in scores_by_category.items():
        if len(scores) == 0:
            continue
        counts, _ = np.histogram(scores, bins=bin_edges)
        total = counts.sum()
        ratios = (counts / total) if total > 0 else counts.astype(float)
        hist_by_cat[cat_id] = ratios
        stats_by_cat[cat_id] = {
            'mean': float(np.mean(scores)),
            'std': float(np.std(scores)),
            'min': float(np.min(scores)),
            'max': float(np.max(scores)),
            'n': int(len(scores)),
        }
        if ratios.size and ratios.max() > max_ratio:
            max_ratio = float(ratios.max())
    if max_ratio <= 0:
        max_ratio = 1.0
    # y축 상단 여유 (라벨/휘스커 공간)
    y_top = max_ratio * 1.15

    # 좌표 변환 헬퍼
    def x_for(v: float) -> float:
        return pad_l + (v - x_min) / (x_max - x_min) * plot_w

    def y_for(ratio: float) -> float:
        return pad_t + plot_h - (ratio / y_top) * plot_h

    # 축
    axis_color = (180, 180, 180)
    drawer.line([(pad_l, pad_t + plot_h), (pad_l + plot_w, pad_t + plot_h)], fill=axis_color, width=1)
    drawer.line([(pad_l, pad_t), (pad_l, pad_t + plot_h)], fill=axis_color, width=1)

    # x 눈금
    n_xticks = 6
    for i in range(n_xticks):
        v = x_min + (x_max - x_min) * i / (n_xticks - 1)
        x = pad_l + plot_w * i / (n_xticks - 1)
        drawer.line([(x, pad_t + plot_h), (x, pad_t + plot_h + 4)], fill=axis_color)
        drawer.text((x - 18, pad_t + plot_h + 6), f"{v:.2f}", fill=axis_color, font=font_axis)

    # y 눈금 (비율)
    n_yticks = 5
    for i in range(n_yticks):
        v = y_top * i / (n_yticks - 1)
        y = pad_t + plot_h - plot_h * i / (n_yticks - 1)
        drawer.line([(pad_l - 4, y), (pad_l, y)], fill=axis_color)
        drawer.text((6, y - 6), f"{v:.2f}", fill=axis_color, font=font_axis)

    drawer.text((pad_l + plot_w / 2 - 36, height - 20), "similarity", fill=axis_color, font=font_axis)
    drawer.text((6, max(2, pad_t - 16)), "ratio", fill=axis_color, font=font_axis)

    # === iou_mode='hist' 인 경우: normal과 이벤트 분포의 면적 IoU에 해당하는 영역
    #     (per-bin min(h_normal, h_event))을 카테고리 색으로 alpha=0.3 정도의 반투명으로 채움.
    #     step line 보다 먼저 그려서 히스토그램 라인이 fill 위에 표시되도록 함. ===
    if iou_mode == 'hist' and normal_id in hist_by_cat:
        bin_w_fill = plot_w / num_bins
        plot_baseline = pad_t + plot_h
        h_norm = hist_by_cat[normal_id]
        fill_alpha = int(round(255 * 0.3))  # 약 30% 투명도
        for cat_id, h_evt in hist_by_cat.items():
            if cat_id == normal_id:
                continue
            rgb = CATEGORY_COLOR_MAP.get(cat_id, (255, 255, 255))
            rgba = (rgb[0], rgb[1], rgb[2], fill_alpha)

            # 카테고리별 RGBA 오버레이에 사각형을 그린 뒤 본 이미지에 alpha composite.
            # (여러 카테고리가 같은 bin 영역에서 겹쳐도 자연스럽게 누적되도록)
            overlay = Image.new('RGBA', img.size, (0, 0, 0, 0))
            overlay_drawer = ImageDraw.Draw(overlay)
            any_filled = False
            for i in range(num_bins):
                o = min(float(h_norm[i]), float(h_evt[i]))
                if o <= 0:
                    continue
                x0 = pad_l + i * bin_w_fill
                x1 = pad_l + (i + 1) * bin_w_fill
                y_o = y_for(o)
                overlay_drawer.rectangle([x0, y_o, x1, plot_baseline], fill=rgba)
                any_filled = True
            if any_filled:
                composite = Image.alpha_composite(img.convert('RGBA'), overlay)
                img.paste(composite.convert('RGB'))
        # 채움 사각형이 하단 축 라인을 덮을 수 있어 다시 그어줌
        drawer.line([(pad_l, plot_baseline), (pad_l + plot_w, plot_baseline)], fill=axis_color, width=1)

    # 라인 히스토그램(계단형)
    bin_w = plot_w / num_bins
    for cat_id, ratios in hist_by_cat.items():
        color = CATEGORY_COLOR_MAP.get(cat_id, (255, 255, 255))
        pts = []
        for i, r in enumerate(ratios):
            x0 = pad_l + i * bin_w
            x1 = pad_l + (i + 1) * bin_w
            y = y_for(r)
            pts.append((x0, y))
            pts.append((x1, y))
        if len(pts) >= 2:
            drawer.line(pts, fill=color, width=2)

    # 평균/표준편차 시각화 오버레이
    cats_sorted = sorted(hist_by_cat.keys())
    n_cats = len(cats_sorted)
    # 휘스커 영역: 플롯 상단 안쪽으로 약간 들여쓰기
    whisker_top = pad_t + 6
    whisker_step = max(8, min(14, (plot_h * 0.18) / max(1, n_cats)))
    plot_bottom = pad_t + plot_h
    for idx, cat_id in enumerate(cats_sorted):
        st = stats_by_cat[cat_id]
        color = CATEGORY_COLOR_MAP.get(cat_id, (255, 255, 255))
        x_mean = x_for(st['mean'])
        x_lo = x_for(max(x_min, st['mean'] - st['std']))
        x_hi = x_for(min(x_max, st['mean'] + st['std']))

        # mean 위치 점선 수직선 (플롯 영역 안)
        y_cur = pad_t
        seg = 4
        while y_cur < plot_bottom:
            y_next = min(y_cur + seg, plot_bottom)
            drawer.line([(x_mean, y_cur), (x_mean, y_next)], fill=color, width=1)
            y_cur += seg * 2

        # mean±std 휘스커 (─| ● |─)
        y_w = whisker_top + idx * whisker_step
        cap = 4
        drawer.line([(x_lo, y_w), (x_hi, y_w)], fill=color, width=2)
        drawer.line([(x_lo, y_w - cap), (x_lo, y_w + cap)], fill=color, width=1)
        drawer.line([(x_hi, y_w - cap), (x_hi, y_w + cap)], fill=color, width=1)
        r = 3
        drawer.ellipse([x_mean - r, y_w - r, x_mean + r, y_w + r], fill=color, outline=color)

    # === normal vs 이벤트 카테고리 std-bar 겹침 영역(플롯 하단 안쪽 색띠) ===
    if overlaps_per_cat:
        strip_h = max(3, int(plot_h * 0.020))
        strip_gap = max(1, int(strip_h * 0.4))
        for idx, eid in enumerate(sorted(overlaps_per_cat.keys())):
            lo, hi = overlaps_per_cat[eid]
            color = CATEGORY_COLOR_MAP.get(eid, (255, 255, 255))
            x_lo = x_for(max(x_min, lo))
            x_hi = x_for(min(x_max, hi))
            if x_hi <= x_lo:
                continue
            y_top = pad_t + plot_h - 2 - (idx + 1) * (strip_h + strip_gap)
            y_bot = y_top + strip_h
            drawer.rectangle([x_lo, y_top, x_hi, y_bot], fill=color, outline=color)

    # === 우측 상단 통계 텍스트 (IoU 정보 포함) ===
    stat_x = pad_l + plot_w + 16
    stat_right_margin = 8
    available_stat_w = max(50, width - stat_x - stat_right_margin)

    title_text = "Per-category statistics"
    if iou_threshold is not None:
        title_text += f"  (IoU mode={iou_mode}, thr={iou_threshold:.2f})"

    # 카테고리별 통계 라인 미리 구성
    stat_entries = []  # list of (color, text)
    for cat_id in sorted(scores_by_category.keys()):
        scores = scores_by_category[cat_id]
        label = class_dict.get(cat_id, str(cat_id))
        color = CATEGORY_COLOR_MAP.get(cat_id, (255, 255, 255))
        if len(scores) == 0:
            text = f"{label}: n=0"
        else:
            st = stats_by_cat[cat_id]
            iou_text = ""
            if cat_id != normal_id:
                iou_val = float(ious_per_cat.get(cat_id, 0.0))
                iou_text = f"  IoU(N)={iou_val:.3f}"
            text = (
                f"{label}: n={st['n']}  mean={st['mean']:.3f}  std={st['std']:.3f}  "
                f"min={st['min']:.3f}  max={st['max']:.3f}{iou_text}"
            )
        stat_entries.append((color, text))

    def _measure_w(font, txt):
        try:
            bbox = drawer.textbbox((0, 0), txt, font=font)
            return bbox[2] - bbox[0]
        except Exception:
            return font.getsize(txt)[0] if hasattr(font, 'getsize') else len(txt) * 6

    def _fit_font(texts, max_size, min_size=7):
        """주어진 텍스트들이 available_stat_w 안에 들어가는 가장 큰 폰트 크기를 반환."""
        for size in range(int(max_size), max(min_size, 1) - 1, -1):
            try:
                font = ImageFont.truetype(_ARIAL, size=size)
            except Exception:
                font = ImageFont.load_default()
                return font, size
            if texts and max(_measure_w(font, t) for t in texts) <= available_stat_w:
                return font, size
        try:
            return ImageFont.truetype(_ARIAL, size=min_size), min_size
        except Exception:
            return ImageFont.load_default(), min_size

    base_stat_size = max(14, int(height / 23))
    stat_lines = [t for _, t in stat_entries]
    fitted_stat_font, fitted_stat_size = _fit_font(stat_lines, base_stat_size, min_size=11)

    base_title_size = max(12, int(height / 22))
    fitted_title_font, fitted_title_size = _fit_font(
        [title_text], max(base_title_size, fitted_stat_size + 1), min_size=fitted_stat_size
    )

    stat_y = pad_t
    drawer.text((stat_x, stat_y), title_text, fill=(255, 255, 255), font=fitted_title_font)
    stat_y += fitted_title_size + 6

    line_step = fitted_stat_size + 4
    for color, text in stat_entries:
        drawer.text((stat_x, stat_y), text, fill=color, font=fitted_stat_font)
        stat_y += line_step

    return img


def main(
    video_path,
    text_json_path,
    model_path,
    class_dict,
    queue_size,
    alarm_duration_threshold,
    model_input_fps,
    save_speed,
    output_path,
    split,
    save_video,
    display,
    classes,
    iou_threshold,
    iou_mode,
    iou_hist_bins,
    analysis,
):

    model = TRTInference(model_path)
    image_size = 336

    if classes is None:
        _, class_list, _, text_features = utils.load_text_feature(text_json_path)
        categories = list(class_dict.keys())
    else:
        categories = [int(c) for c in classes]
        _, class_list, _, text_features = utils.load_text_feature(text_json_path, categories)

    text_features = text_features.cuda()
    class_list_np = np.asarray(class_list)

    print(list(class_dict.keys()))
    print(categories)

    os.makedirs(output_path, exist_ok=True)
    os.makedirs(os.path.join(output_path, 'inference'), exist_ok=True)
    pred_path = os.path.join(output_path, 'pred')
    os.makedirs(pred_path, exist_ok=True)
    if analysis:
        analysis_path = os.path.join(output_path, 'analysis')
        os.makedirs(analysis_path, exist_ok=True)

    video_files = utils.get_video_files(video_path)
    if split is not None:
        total_files = len(video_files)
        chunk_size = math.ceil(total_files / split[0])
        start_idx = (split[1] - 1) * chunk_size
        end_idx = min(split[1] * chunk_size, total_files)
        video_files = video_files[start_idx:end_idx]

    for video_file in video_files:
        event_manager = PEEventStateManager(categories, queue_size=queue_size, threshold=alarm_duration_threshold)

        file_name = os.path.splitext(os.path.basename(video_file))[0]
        output_dict = defaultdict(list)   # 프레임별 알람 0/1 로그 (CSV 저장용)
        analysis_dict = defaultdict(list) if analysis else None  # 프레임별 이벤트 IoU 로그
        ratio_dict = defaultdict(list)
        preprocess_time = []
        inference_time = []
        total_time = []

        vid = cv2.VideoCapture(video_file)
        orig_fps = vid.get(cv2.CAP_PROP_FPS)
        max_frames = int(vid.get(cv2.CAP_PROP_FRAME_COUNT))
        if orig_fps <= 0:
            continue
        duration_sec = max_frames / orig_fps

        count = max(1, model_input_fps * int(max_frames / orig_fps))
        if count <= 0:
            print(f"Skipping {video_file}: sampled frame count is 0")
            vid.release()
            continue
        frame_indices = np.floor(np.arange(count) * max_frames / count).astype(int)
        print(frame_indices)

        # 출력 fps = model_input_fps * save_speed
        # (save_speed=1 → 원본과 동일 시간 흐름, save_speed=N → N배속 재생)
        video_maker = utils.VideoFromPIL(
            output_path=os.path.join(output_path, 'inference', f"{file_name}.mp4"),
            model_input_fps=model_input_fps,
            save_speed=save_speed,
        )
        to_pil_image = torchvision.transforms.ToPILImage()

        frame_idx = -1
        while True:
            frame_idx += 1
            ret, frame = vid.read()
            if not ret:
                break
            if frame_idx in frame_indices:
                t0 = time.time()

                roi_polygon = []
                roi_np = np.array(roi_polygon, dtype=np.float32) if len(roi_polygon) > 0 else None

                img_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)

                frame_t = torch.from_numpy(img_rgb).to("cuda")
                if roi_np is not None and roi_np.size > 0:
                    crop_chw = torch_crop_region(frame_t, roi_np, pad_color=(114, 114, 114))
                else:
                    crop_chw = frame_t.permute(2, 0, 1).contiguous()

                image_cuda = preprocess_image([crop_chw], size=image_size, device="cuda")

                with torch.inference_mode():
                    visual_vec = model(image_cuda)
                    features = visual_vec / visual_vec.norm(dim=-1)
                t1 = time.time()

                # 현재 프레임 시각 피처 vs 전체 텍스트 프롬프트 코사인 유사도
                scores = (features @ text_features.T)[0]
                scores_np = scores.detach().cpu().numpy()

                # 카테고리별 유사도 분포 (프레임 단위) + 평균/표준편차
                scores_by_category = {}
                stats_by_cat = {}
                for cat_id in class_dict.keys():
                    mask = class_list_np == cat_id
                    s = scores_np[mask]
                    scores_by_category[cat_id] = s
                    if len(s) > 0:
                        stats_by_cat[cat_id] = {
                            'mean': float(np.mean(s)),
                            'std': float(np.std(s)),
                        }

                # === IoU 기반 이벤트 판별 ===
                # 시각화는 항상 std-bar 겹침 구간 사용. IoU 수치 자체는 iou_mode 설정에 따라 std/hist 중 선택.
                ious_per_cat = {}
                overlaps_per_cat = {}

                hist_for_iou = {}
                if iou_mode == 'hist':
                    valid_lists = [s for s in scores_by_category.values() if len(s) > 0]
                    if valid_lists:
                        flat = np.concatenate(valid_lists)
                        x_lo = float(flat.min())
                        x_hi = float(flat.max())
                        if x_hi - x_lo < 1e-6:
                            x_hi = x_lo + 1e-6
                        edges = np.linspace(x_lo, x_hi, int(iou_hist_bins) + 1)
                        for cid, s in scores_by_category.items():
                            if len(s) == 0:
                                continue
                            c, _ = np.histogram(s, bins=edges)
                            tot = c.sum()
                            hist_for_iou[cid] = (c / tot) if tot > 0 else c.astype(float)

                if NORMAL_CATEGORY_ID in stats_by_cat:
                    n_st = stats_by_cat[NORMAL_CATEGORY_ID]
                    for eid, e_st in stats_by_cat.items():
                        if eid == NORMAL_CATEGORY_ID:
                            continue
                        # std-bar 기반 겹침 구간 (시각화 + std 모드 IoU)
                        lo = max(n_st['mean'] - n_st['std'], e_st['mean'] - e_st['std'])
                        hi = min(n_st['mean'] + n_st['std'], e_st['mean'] + e_st['std'])
                        if hi > lo:
                            overlaps_per_cat[eid] = (lo, hi)
                        if iou_mode == 'std':
                            iou_val = compute_std_iou(
                                n_st['mean'], n_st['std'], e_st['mean'], e_st['std']
                            )
                        else:  # 'hist'
                            iou_val = compute_hist_iou(
                                hist_for_iou.get(NORMAL_CATEGORY_ID), hist_for_iou.get(eid)
                            )
                        ious_per_cat[eid] = float(iou_val)

                # 알림 발생: IoU < threshold 인 이벤트만 후보로 간주
                predicts = [eid for eid, iv in ious_per_cat.items() if iv < iou_threshold]
                alarms = event_manager.update(predicts)
                dq_state = {cat: sum(dq) for cat, dq in event_manager.duration_queue.items()}

                t2 = time.time()

                # 프레임별 알람 로그 누적 (normal=0 은 항상 0으로 기록)
                output_dict['frame'].append(int(frame_idx))
                for cls_num, cls_name in class_dict.items():
                    output_dict[cls_name].append(1 if cls_num in alarms and cls_num != NORMAL_CATEGORY_ID else 0)

                # 프레임별 이벤트 IoU 누적 (analysis=True일 때만)
                if analysis_dict is not None:
                    analysis_dict['frame'].append(int(frame_idx))
                    for cls_num, cls_name in class_dict.items():
                        if cls_num == NORMAL_CATEGORY_ID:
                            continue
                        analysis_dict[cls_name].append(float(ious_per_cat.get(cls_num, 0.0)))

                preprocess_time.append(t1 - t0)
                inference_time.append(t2 - t1)
                total_time.append(t2 - t0)

                if save_video:
                    crop_hwc = crop_chw.permute(1, 2, 0).contiguous().cpu().numpy()
                    tensor = torch.from_numpy(crop_hwc).permute(2, 0, 1).contiguous()
                    pil_img = to_pil_image(tensor)

                    # 알림 UI: 기존 utils.draw의 빨간 테두리 + ALARM 텍스트만 사용
                    pil_img = utils.draw(
                        pil_img,
                        prompt_list=np.array([]),
                        class_list=np.array([]),
                        text_probs=torch.zeros(0),
                        predicts=predicts,
                        alarms=alarms,
                        dq_state=dq_state,
                        queue_size=queue_size,
                        output_path=None,
                        show_prompt=False,
                        class_dict=class_dict,
                    )

                    if display:
                        panel_h = max(240, pil_img.height // 3)
                        # 좁은 영상에서 우측 통계(IoU 등) 텍스트가 잘리지 않도록 패널 최소 폭 보장.
                        # stack_vertical이 더 넓은 쪽 폭에 맞추므로 패널이 영상보다 넓어도 안전.
                        panel_w = max(pil_img.width, 1000)
                        if panel_w % 2:  # 인코더 호환(짝수 폭)
                            panel_w += 1
                        hist_panel = render_distribution_panel(
                            scores_by_category=scores_by_category,
                            class_dict=class_dict,
                            width=panel_w,
                            height=panel_h,
                            ious_per_cat=ious_per_cat,
                            overlaps_per_cat=overlaps_per_cat,
                            iou_threshold=iou_threshold,
                            iou_mode=iou_mode,
                            normal_id=NORMAL_CATEGORY_ID,
                        )
                        out_img = utils.stack_vertical(pil_img, hist_panel)
                    else:
                        out_img = pil_img
                    video_maker.add_frame(out_img)

                # 콘솔 로그: 프레임별 카테고리 분포 통계 + IoU
                print("==============================")
                print(f"File                : {video_file}")
                print(f"Split               : split_size={split[0]} split_k={split[1]}")
                print(f"Frame               : {frame_idx}/{max_frames}")
                print(f"Number of prompts   : {len(text_features)}")
                print(f"Pre-processing time : {preprocess_time[-1]:.3f} sec")
                print(f"Inference time      : {inference_time[-1]:.3f} sec")
                print(f"Total time          : {total_time[-1]:.3f} sec")
                for cat_id, vals in scores_by_category.items():
                    label = class_dict.get(cat_id, str(cat_id))
                    if len(vals) == 0:
                        print(f"  {label:>10s}: n=0")
                    else:
                        iou_str = ""
                        if cat_id != NORMAL_CATEGORY_ID and cat_id in ious_per_cat:
                            iou_str = f" IoU(N)={ious_per_cat[cat_id]:.3f}"
                        print(f"  {label:>10s}: n={len(vals)} mean={np.mean(vals):.3f} std={np.std(vals):.3f} var={np.var(vals):.4f}{iou_str}")
                print(f"IoU mode={iou_mode}  threshold={iou_threshold}  predicts={sorted(predicts)}  alarms={sorted(alarms)}")
                print("==============================")

        if save_video:
            video_maker.save()
        vid.release()

        # 추론 프레임 이외 구간 누락 프레임 양방향 fill 후 CSV / config 저장
        for key, vals in output_dict.items():
            if key == 'frame':
                continue
            ratio_dict[key] = float(np.mean(vals)) if len(vals) > 0 else 0.0

        if len(output_dict.get('frame', [])) > 0:
            output_dict = utils.interpolate_missing_frames(output_dict, orig_fps, max_frames)
            perf = {
                'video_length': duration_sec,
                'max_frames': max_frames,
                'Preprocess time (avg)': statistics.mean(preprocess_time) if preprocess_time else 0.0,
                'Inference time (avg)': statistics.mean(inference_time) if inference_time else 0.0,
                'Pre + Inf time (avg)': statistics.mean(total_time) if total_time else 0.0,
                'Predict class ratio': ratio_dict,
            }
            config = {
                'model': {'name': 'Perception Encoder'},
                'path': {
                    'video_path': video_path,
                    'csv_save_path': pred_path,
                    'config_save_path': pred_path,
                    'video_file': video_file,
                },
                'alarm': {
                    'iou_mode': iou_mode,
                    'iou_threshold': iou_threshold,
                    'iou_hist_bins': iou_hist_bins,
                    'queue_size': queue_size,
                    'alarm_duration_threshold': alarm_duration_threshold,
                    'model_input_fps': model_input_fps,
                    'save_speed': save_speed,
                },
                'performance': perf,
                'dummy': {'split': split},
            }
            utils.save_config(config, os.path.join(pred_path, file_name))
            utils.save_model_output(output_dict, os.path.join(pred_path, file_name))

        # analysis CSV: 측정된 프레임만 그대로 저장 (빈 프레임 간격 채우지 않음)
        if analysis_dict is not None and len(analysis_dict.get('frame', [])) > 0:
            utils.save_model_output(analysis_dict, os.path.join(analysis_path, file_name))


def str2list(v):
    return v.split(",")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--video_path', required=True)
    parser.add_argument('-o', '--output_path', default='./results', required=True)
    parser.add_argument('-q', '--queue_size', type=int, default=4)
    parser.add_argument('-m', '--model_path', type=str, default="./model/PE-Core-L14-336.engine")

    parser.add_argument('-a', '--alarm_duration_threshold', type=int, default=2)
    parser.add_argument('-i', '--model_input_fps', type=int, default=2)
    parser.add_argument('-ss', '--save_speed', type=float, default=1,
                        help='저장 영상 재생 속도 배율. save_speed=1이면 원본과 동일한 시간 흐름, '
                             'save_speed=N이면 N배속으로 저장 (output fps = model_input_fps * save_speed)')
    parser.add_argument('-t', '--text_json_path', type=str, default='./data/text_features.json')

    parser.add_argument('-c', '--category', required=True)
    parser.add_argument('-d', '--detail', default='', nargs='?')
    parser.add_argument('-s', '--save_video', action='store_true')
    parser.add_argument('--display', action='store_true',
                        help='지정 시 영상 아래에 카테고리별 유사도 히스토그램 패널을 함께 그린다. '
                             '미지정 시 알림 UI(빨간 테두리 + ALARM 텍스트)만 그린다.')
    parser.add_argument("--classes", type=str2list, default=None, help="숫자 리스트 입력")
    parser.add_argument('--split_size', type=int, default=1)
    parser.add_argument('--split_k', type=int, default=1)

    # IoU 기반 알림 옵션
    parser.add_argument('--iou_threshold', type=float, default=0.1,
                        help='IoU < threshold 일 때 이벤트 후보로 간주하여 알림 카운트 증가 (default: 0.1)')
    parser.add_argument('--iou_mode', type=str, default='std', choices=['std', 'hist'],
                        help="IoU 계산 방식: 'std'=평균±표준편차 구간 IoU, 'hist'=히스토그램 면적 IoU (default: std)")
    parser.add_argument('--iou_hist_bins', type=int, default=80,
                        help="iou_mode='hist' 일 때 IoU 계산용 히스토그램 bin 수 (default: 80)")
    parser.add_argument('--analysis', action='store_true',
                        help='지정 시 output_path/analysis 폴더에 프레임별 이벤트 IoU를 CSV로 저장한다.')

    args = parser.parse_args()
    class_dict = {0: 'normal', 1: 'falldown', 2: 'fire', 3: 'smoke', 4: 'smoking', 5: 'esfalldown', 6: 'elvfalldown'}
    main(
        video_path=args.video_path,
        text_json_path=args.text_json_path,
        model_path=args.model_path,
        class_dict=class_dict,
        queue_size=args.queue_size,
        alarm_duration_threshold=args.alarm_duration_threshold,
        model_input_fps=args.model_input_fps,
        save_speed=args.save_speed,
        output_path=os.path.join(args.output_path, f"{args.category}_{args.detail}"),
        split=(args.split_size, args.split_k),
        save_video=args.save_video,
        display=args.display,
        classes=args.classes,
        iou_threshold=args.iou_threshold,
        iou_mode=args.iou_mode,
        iou_hist_bins=args.iou_hist_bins,
        analysis=args.analysis,
    )
