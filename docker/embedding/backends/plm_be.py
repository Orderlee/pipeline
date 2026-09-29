"""PLM(Perception-LM) 백엔드 — 이미지 → 서술 문장. PE-Lang 비전 타워 + Llama 디코더.

왜 PE-Lang 이 아니라 PLM 인가: PE-Lang 은 `forward_features(image)` 뿐이고 텍스트 타워도
디코더도 없다. 문장을 뱉으려면 디코더가 붙은 PLM(`facebook/Perception-LM-{1B,3B,8B}`)
이어야 한다. PLM 의 비전 타워가 곧 PE-Lang 이다.

왜 채점기와 같은 컨테이너인가: "생성 → 즉시 채점 → 버림"을 한 패스로 하기 위해서다.
생성과 채점이 분리돼 있어서 문장 12,511개를 눈감고 만들어 놓고 나중에 가지치기하던 게
지금 방식이고, 그 결과가 규칙으로 쓴 499문장에 졌다.

⚠️ 배치 규칙 (어기면 SAM3 가 죽는다)
  · PE-Core = cuda:0 (호스트 GPU0) · PLM = cuda:1 (호스트 GPU1)
  · **GPU1 은 SAM3(prod 서빙) 소유다.** PLM 은 재시도 가능한 배치라 항상 양보한다 —
    load() 전에 free VRAM 을 재고 모자라면 로드하지 않는다(gpu_guard.require_free).
  · 작업이 끝나면 unload() 로 즉시 반납한다. idle watcher 도 별도 타이머로 회수한다.

⚠️ transformers 의 PerceptionLM 지원 API 는 버전마다 이름이 흔들린다. 아래는 표준
   image-text-to-text 경로이고, 최초 기동 검증은 `/caption` selftest 로 한다
   (`apo_loop.py selftest`). 실패하면 고칠 곳은 `_build_inputs` 한 함수뿐이다.
"""

from __future__ import annotations

import io
import os

DEFAULT_MODEL_ID = "facebook/Perception-LM-3B"


class PlmBackend:
    """이미지 → 텍스트. EmbeddingBackend 를 상속하지 않는다 (embed_* 계약이 없음)."""

    def __init__(self, device: str = "cuda:1") -> None:
        self.device = device
        self.model_id = os.environ.get("PLM_MODEL_ID", DEFAULT_MODEL_ID).strip() or DEFAULT_MODEL_ID
        self.name = self.model_id
        self.max_new_tokens = int(os.environ.get("PLM_MAX_NEW_TOKENS", "128"))
        self._model = None
        self._processor = None
        self._torch = None

    # ── 수명주기 ────────────────────────────────────────────────────────────
    def load(self) -> None:
        import torch
        from transformers import AutoModelForImageTextToText, AutoProcessor

        self._torch = torch
        self._processor = AutoProcessor.from_pretrained(self.model_id)
        model = AutoModelForImageTextToText.from_pretrained(
            self.model_id,
            torch_dtype=torch.bfloat16,
            low_cpu_mem_usage=True,
        )
        self._model = model.to(self.device).eval()

    def is_loaded(self) -> bool:
        return self._model is not None

    def unload(self) -> None:
        """참조를 끊고 CUDA 캐시 반납. GPU1 을 SAM3 에 돌려주는 경로라 조용히 실패시키지 않는다."""
        self._model = None
        self._processor = None
        if self._torch is not None and self._torch.cuda.is_available():
            try:
                self._torch.cuda.empty_cache()
            except Exception:
                pass

    # ── 추론 ────────────────────────────────────────────────────────────────
    def _build_inputs(self, img, prompt: str):
        """채팅 템플릿 + 이미지 → 모델 입력. transformers API 변동 시 여기만 고친다."""
        messages = [{
            "role": "user",
            "content": [{"type": "image"}, {"type": "text", "text": prompt}],
        }]
        text = self._processor.apply_chat_template(messages, add_generation_prompt=True, tokenize=False)
        return self._processor(images=img, text=text, return_tensors="pt").to(self.device)

    def caption(self, image_bytes: bytes, prompt: str, max_new_tokens: int | None = None) -> str:
        from PIL import Image

        img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        inputs = self._build_inputs(img, prompt)
        with self._torch.no_grad():
            out = self._model.generate(
                **inputs,
                max_new_tokens=max_new_tokens or self.max_new_tokens,
                do_sample=False,           # 결정론 — 같은 이미지가 매번 같은 문장을 내야 A/B 가 성립
            )
        # 프롬프트 토큰을 잘라내고 생성분만 디코딩
        gen = out[0][inputs["input_ids"].shape[-1]:]
        return self._processor.decode(gen, skip_special_tokens=True).strip()
