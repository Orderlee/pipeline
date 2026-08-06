import os
import os as _os
_ARIAL = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), 'arial.ttf')
import json
import glob
import subprocess
import pandas as pd
import cv2
import numpy as np
import torch
from PIL import Image, ImageDraw, ImageFont
import pdb

def load_text_feature(json_path, target_class=None):
    """
    Load prompts and features from a JSON file, optionally filtering by target_class.

    Returns:
        class_list (np.ndarray), prompt_list (np.ndarray), text_features (torch.Tensor)
    """
    with open(json_path, 'r', encoding='utf-8') as f:
        items = json.load(f)
    if target_class is not None:
        target_class = [int(x) for x in target_class]
        if target_class:
            items = [it for it in items if it['class'] in target_class]

    # ID 추가
    ID_list = np.array([it['ID'] for it in items])
    class_list = np.array([it['class'] for it in items])
    prompt_list = np.array([it['prompt'] for it in items])
    features_list = [it['feature'] for it in items]

    text_features = torch.tensor(features_list, dtype=torch.float32)
    return ID_list, class_list, prompt_list, text_features


def sliding_window_indices(num_frames: int, window_size: int, interval: int, stride: int) -> list[list[int]]:
    """
    Generate lists of frame indices for sliding windows over a sequence.
    """
    half = window_size // 2
    offset = -half * interval
    max_i = (num_frames - 1) // stride
    windows = []
    for i in range(max_i + 1):
        start = i * stride + offset
        idxs = []
        for j in range(window_size):
            idx = start + j * interval
            if 0 <= idx < num_frames:
                idxs.append(idx)
        windows.append(idxs)
    return windows


def get_video_files(folder_path: str, extensions: tuple[str, ...] = ('.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.MOV')) -> list[str]:
    """
    Return a list of all video file paths in a folder matching given extensions.
    """
    if os.path.isfile(folder_path):
        _, ext = os.path.splitext(folder_path)
        return [folder_path] if ext in extensions else []

    video_files = []
    for ext in extensions:
        video_files.extend(glob.glob(os.path.join(folder_path, f'*{ext}')))
    return video_files

def interpolate_missing_frames(data: dict, fps: float, max_frame: int) -> dict:
    """
    Fill missing frame entries by forward/backward fill based on original fps.
    """
    df = pd.DataFrame.from_dict(data)
    all_frames = pd.DataFrame({'frame': range(df['frame'].min(), max_frame)})
    df_filled = all_frames.merge(df, on='frame', how='left')
    # pandas는 limit=0 / 음수를 허용하지 않으므로 최소 1로 클램프.
    # (저fps 입력에서 int(fps/2)==0 이 되어 ValueError 발생하던 것 방지)
    limit = max(1, int(fps / 2))
    # fillna(method='ffill'/'bfill') 는 deprecated → ffill/bfill 직접 호출.
    df_interp = df_filled.ffill(limit=limit).bfill(limit=limit)
    df_interp = df_interp.ffill().bfill().astype(int)
    df_interp = df_interp[:max_frame]
    return df_interp.to_dict(orient='list')

def save_config(config: dict, output_path: str):
    """
    Save configuration dict as a JSON file.
    """
    os.makedirs(os.path.dirname(output_path + '.json'), exist_ok=True)
    with open(output_path + '.json', 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=4, ensure_ascii=False)

def save_model_output(output: dict, output_path: str):
    """
    Save model output dict as a CSV file.
    """
    os.makedirs(os.path.dirname(output_path + '.csv'), exist_ok=True)
    df = pd.DataFrame.from_dict(output)
    df.to_csv(output_path + '.csv', index=False)

class VideoFromPIL:
    """
    Stream PIL images to a video file encoded with H.264/AVC via ffmpeg.
    """
    def __init__(
        self,
        output_path: str,
        model_input_fps: int = 2,
        save_speed: float = 1,
        output_fps: float | None = None,
        preserve_source_timing: bool = False,
        total_source_frames: int | None = None,
    ):
        self.output_path = output_path
        self.model_input_fps = model_input_fps
        self.save_speed = save_speed
        self.output_fps = output_fps
        self.preserve_source_timing = preserve_source_timing
        self.total_source_frames = total_source_frames
        self.size = None
        self._ffmpeg_process = None
        self._pending_frame_bytes = None
        self._pending_source_frame_idx = None
        self._segment_start_idx = 0
        self._has_frames = False

    def _resolve_output_fps(self) -> float:
        if self.output_fps is not None:
            fps = float(self.output_fps)
        else:
            fps = float(self.model_input_fps) * float(self.save_speed)
        if fps <= 0:
            raise ValueError("output fps must be greater than 0.")
        return fps

    def _start_ffmpeg(self):
        if self.size is None:
            raise RuntimeError("Video size is not initialized.")

        width, height = self.size
        os.makedirs(os.path.dirname(self.output_path) or ".", exist_ok=True)
        cmd = [
            "ffmpeg",
            "-y",
            "-loglevel",
            "error",
            "-f",
            "rawvideo",
            "-vcodec",
            "rawvideo",
            "-pix_fmt",
            "rgb24",
            "-s",
            f"{width}x{height}",
            "-r",
            f"{self._resolve_output_fps():.6f}",
            "-i",
            "-",
            "-an",
            "-vf",
            "pad=ceil(iw/2)*2:ceil(ih/2)*2",
            "-c:v",
            "libx264",
            "-profile:v",
            "high",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            self.output_path,
        ]
        try:
            self._ffmpeg_process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except FileNotFoundError as exc:
            raise RuntimeError("ffmpeg executable was not found.") from exc

    def _prepare_frame_bytes(self, pil_image: Image.Image) -> bytes:
        if pil_image.mode != "RGB":
            pil_image = pil_image.convert("RGB")
        if self.size is None:
            self.size = pil_image.size
            self._start_ffmpeg()
        elif pil_image.size != self.size:
            pil_image = pil_image.resize(self.size)
        return np.asarray(pil_image, dtype=np.uint8).tobytes()

    def _ffmpeg_error_message(self) -> str:
        if self._ffmpeg_process is None or self._ffmpeg_process.stderr is None:
            return "unknown ffmpeg error"
        stderr = self._ffmpeg_process.stderr.read().decode("utf-8", errors="replace").strip()
        return stderr or "unknown ffmpeg error"

    def _write_frame_bytes(self, frame_bytes: bytes, repeat: int = 1):
        if self._ffmpeg_process is None or self._ffmpeg_process.stdin is None:
            raise RuntimeError("ffmpeg writer is not initialized.")
        try:
            for _ in range(repeat):
                self._ffmpeg_process.stdin.write(frame_bytes)
        except BrokenPipeError as exc:
            raise RuntimeError(self._ffmpeg_error_message()) from exc

    def add_frame(self, pil_image: Image.Image, source_frame_idx: int | None = None):
        frame_bytes = self._prepare_frame_bytes(pil_image)
        self._has_frames = True

        if not self.preserve_source_timing:
            self._write_frame_bytes(frame_bytes)
            return

        if source_frame_idx is None:
            raise ValueError("source_frame_idx is required when preserve_source_timing=True.")

        source_frame_idx = int(source_frame_idx)
        if source_frame_idx < 0:
            raise ValueError("source_frame_idx must be non-negative.")

        if self._pending_frame_bytes is None:
            self._pending_frame_bytes = frame_bytes
            self._pending_source_frame_idx = source_frame_idx
            self._segment_start_idx = 0
            return

        if source_frame_idx <= self._pending_source_frame_idx:
            self._pending_frame_bytes = frame_bytes
            self._pending_source_frame_idx = source_frame_idx
            return

        repeat_count = source_frame_idx - self._segment_start_idx
        if repeat_count > 0:
            self._write_frame_bytes(self._pending_frame_bytes, repeat=repeat_count)

        self._pending_frame_bytes = frame_bytes
        self._pending_source_frame_idx = source_frame_idx
        self._segment_start_idx = source_frame_idx

    def save(self):
        if not self._has_frames:
            raise RuntimeError("No frames to save.")

        if self.preserve_source_timing:
            if self._pending_frame_bytes is None:
                raise RuntimeError("No frames to save.")
            end_frame_idx = (
                int(self.total_source_frames)
                if self.total_source_frames is not None
                else self._segment_start_idx + 1
            )
            repeat_count = max(1, end_frame_idx - self._segment_start_idx)
            self._write_frame_bytes(self._pending_frame_bytes, repeat=repeat_count)

        if self._ffmpeg_process is None:
            raise RuntimeError("ffmpeg writer is not initialized.")

        if self._ffmpeg_process.stdin is not None:
            self._ffmpeg_process.stdin.close()

        return_code = self._ffmpeg_process.wait()
        if return_code != 0:
            raise RuntimeError(self._ffmpeg_error_message())

# frame 시각화
def draw(
    visual_img: Image.Image,
    prompt_list, class_list, text_probs,
    predicts: int,
    alarms: list[int] = None,
    dq_state: dict[int,int] = None,
    queue_size: int = None,
    output_path=None,
    show_prompt=False,
    class_dict: dict[int, str] = None,
) -> Image.Image:
    """
    Draw top prompts, predictions, sliding-queue state, and alarms on a PIL image.
    """
    img = Image.fromarray(visual_img) if isinstance(visual_img, np.ndarray) else visual_img.copy()
    draw_ctx = ImageDraw.Draw(img)
    w, h = img.size
    label_map = class_dict or {0:'normal',1:'falldown',2:'fire',3:'smoke',4:'smoking'}

    # sliding queue 구조 시각화
    if show_prompt:
        if dq_state is not None and queue_size is not None:
            font_q = ImageFont.truetype(_ARIAL, size=int(h/40))
            y0 = 5
            for cat, count in dq_state.items():
                label = label_map.get(cat, 'unknown')
                text = f"{label}:{count}/{queue_size}"
                draw_ctx.text((5, y0), text, font=font_q, fill=(255,255,0))
                bbox_q = draw_ctx.textbbox((5, y0), text, font=font_q)
                y0 += (bbox_q[3] - bbox_q[1]) + 2

    probs = text_probs.detach().cpu().numpy()
    idxs = probs.argsort()[::-1]
    sorted_items = [(prompt_list[i], probs[i], class_list[i]) for i in idxs]

    font = ImageFont.truetype(_ARIAL, size=int(h/45))
    padding = int(h/120)
    y = padding
    color_map = {0: (255,255,255), 1: (0,255,0), 2: (255,0,0), 3: (0,0,255), 4: (210,105,30), 5: (127,127,127), 6: (255,0,255)}

    if show_prompt:
        # topk 텍스트 시각화
        for desc, prob, cls in sorted_items:
            text = f"{desc}: {prob:.4f}"
            bbox = draw_ctx.textbbox((0, 0), text, font=font)
            text_w, text_h = bbox[2] - bbox[0], bbox[3] - bbox[1]
            x = w - padding - text_w
            # draw_ctx.rectangle([x, y, x+text_w, y+text_h], fill=(0,0,0))
            draw_ctx.text((x, y), text, font=font, fill=color_map.get(cls, (255,255,255)),stroke_width=1,stroke_fill=(0, 0, 0))
            y += text_h + 5
        
    # 카테고리 알림 및 테두리 시각화
    if len(alarms) != 0:
        border = int(h/120)
        for i in range(border):
            draw_ctx.rectangle([i, i, w-1-i, h-1-i], outline="red")

        font_a = ImageFont.truetype(_ARIAL, size=int(h/30))
        y1 = 5 + (len(dq_state) if dq_state else 0) * int(h/40) + 5

        text = "[ALARM] "
        for cat in alarms:
            label = label_map.get(cat, "unknown")
            text += f"{label.upper()} "
        draw_ctx.text((5, y1), text, font=font_a, fill=(255,0,0))
        bbox_a = draw_ctx.textbbox((5, y1), text, font=font_a)
        y1 += (bbox_a[3] - bbox_a[1]) + 2

    return img


GRAPH_COLOR_MAP = {
    0: (255, 255, 255),
    1: (0, 255, 0),
    2: (255, 0, 0),
    3: (0, 0, 255),
    4: (210, 105, 30),
    5: (127, 127, 127),
    6: (255, 0, 255),
}


def stack_vertical(top_img: Image.Image, bottom_img: Image.Image) -> Image.Image:
    """두 PIL 이미지를 세로로 이어붙임(좌측 정렬, 배경 검정)."""
    w = max(top_img.width, bottom_img.width)
    h = top_img.height + bottom_img.height
    out = Image.new('RGB', (w, h), color=(0, 0, 0))
    out.paste(top_img, (0, 0))
    out.paste(bottom_img, (0, top_img.height))
    return out


def render_score_graph(
    score_buffer,
    current_frame_idx: int,
    orig_fps: float,
    width: int,
    height: int,
    event_threshold: float,
    normal_id: int = 0,
    time_window_sec: float = 10.0,
) -> Image.Image:
    """
    실시간 시계열 점수 그래프 렌더링.

    score_buffer: list of (orig_frame_idx, dict) 형태 엔트리.
        dict 구조:
            'normal_sim'  : float   (현재 프레임 vs normal 프롬프트 유사도)
            'event_sims'  : {event_id: float}  (현재 프레임 vs 각 event 프롬프트 유사도)
            'event_scores' : {event_id: float}  (캘리브레이션으로 [0,1] 스케일링된 최종 이벤트 스코어)

    - 가로축: 좌(t = current - window) -> 우(t = current). 즉 좌 -> 우로 시간 흐름.
    - 세로축: 0(아래) ~ 1(위). 범위 밖 값은 클램프 없이 그대로 그려져 캔버스를 벗어나면 PIL이 클립.
    - 배경: 검정. event_threshold 위치에 흰색 두께 1 가로선.
    - 라인 색: GRAPH_COLOR_MAP. 카테고리별 raw 유사도는 두께 1, 스케일링된 이벤트 스코어는 두께 5.
    """
    img = Image.new('RGB', (max(width, 1), max(height, 1)), color=(0, 0, 0))
    if width <= 1 or height <= 1:
        return img
    drawer = ImageDraw.Draw(img)

    def y_for(value: float) -> int:
        return int(round((height - 1) - value * (height - 1)))

    # threshold 가로선 (흰색 두께 1)
    y_thr = y_for(event_threshold)
    drawer.line([(0, y_thr), (width - 1, y_thr)], fill=(255, 255, 255), width=1)

    if not score_buffer:
        return img

    window = max(1.0, float(time_window_sec) * float(orig_fps))
    t_left = float(current_frame_idx) - window

    def x_for(t: float) -> int:
        return int(round((t - t_left) / window * (width - 1)))

    # normal 프롬프트 유사도 (두께 1)
    pts_normal = [
        (x_for(f), y_for(s['normal_sim']))
        for f, s in score_buffer if 'normal_sim' in s
    ]
    if len(pts_normal) >= 2:
        drawer.line(pts_normal, fill=GRAPH_COLOR_MAP.get(normal_id, (255, 255, 255)), width=1)

    # 이벤트 카테고리별 raw 유사도(두께 1) + 스케일링된 이벤트 스코어(두께 5)
    event_ids_in_buf = set()
    for _, s in score_buffer:
        event_ids_in_buf.update(s.get('event_sims', {}).keys())
        event_ids_in_buf.update(s.get('event_scores', {}).keys())

    for eid in sorted(event_ids_in_buf):
        color = GRAPH_COLOR_MAP.get(int(eid), (200, 200, 200))
        pts_sim = [
            (x_for(f), y_for(s['event_sims'][eid]))
            for f, s in score_buffer
            if eid in s.get('event_sims', {})
        ]
        if len(pts_sim) >= 2:
            drawer.line(pts_sim, fill=color, width=1)

        pts_score = [
            (x_for(f), y_for(s['event_scores'][eid]))
            for f, s in score_buffer
            if eid in s.get('event_scores', {})
        ]
        if len(pts_score) >= 2:
            drawer.line(pts_score, fill=color, width=5)

    return img
