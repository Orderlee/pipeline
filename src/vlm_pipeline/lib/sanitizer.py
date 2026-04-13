"""파일명 정규화 — NFD→NFC, 한글→로마자, 비ASCII→ASCII, 소문자 통일.

Layer 1: 순수 Python, Dagster 의존 없음.
"""

import os
import re
import unicodedata

try:
    from korean_romanizer.romanizer import Romanizer
except Exception:  # noqa: BLE001
    Romanizer = None  # type: ignore[assignment, misc]

_ALLOWED = re.compile(r"[^a-zA-Z0-9._-]")
_MULTI_UNDERSCORE = re.compile(r"_+")
_HANGUL_SYLLABLE = re.compile(r"[\uac00-\ud7a3]")

# Hangul syllable decomposition (U+AC00..U+D7A3) -> RR-like romanization.
_L_MAP = [
    "g", "kk", "n", "d", "tt", "r", "m", "b", "pp", "s",
    "ss", "", "j", "jj", "ch", "k", "t", "p", "h",
]
_V_MAP = [
    "a", "ae", "ya", "yae", "eo", "e", "yeo", "ye", "o", "wa",
    "wae", "oe", "yo", "u", "wo", "we", "wi", "yu", "eu", "ui", "i",
]
_T_MAP = [
    "", "k", "k", "ks", "n", "nj", "nh", "t", "l", "lk",
    "lm", "lb", "ls", "lt", "lp", "lh", "m", "p", "ps", "t",
    "t", "ng", "t", "t", "k", "t", "p", "h",
]


def _romanize_hangul_char(ch: str) -> str:
    code = ord(ch)
    if code < 0xAC00 or code > 0xD7A3:
        return ch

    s_index = code - 0xAC00
    l_index = s_index // 588
    v_index = (s_index % 588) // 28
    t_index = s_index % 28
    return _L_MAP[l_index] + _V_MAP[v_index] + _T_MAP[t_index]


def _romanize_korean(text: str) -> str:
    """한글이 포함되어 있으면 로마자로 변환."""
    if not _HANGUL_SYLLABLE.search(text):
        return text

    # Prefer external romanizer when available, but always keep fallback
    # so behavior is stable even if dependency is missing.
    if Romanizer is not None:
        try:
            romanized = Romanizer(text).romanize()
            if not _HANGUL_SYLLABLE.search(romanized):
                return romanized
            text = romanized
        except Exception:  # noqa: BLE001
            pass

    return "".join(_romanize_hangul_char(ch) for ch in text)


def sanitize_path_component(name: str) -> str:
    """경로 세그먼트(폴더명 등)를 ASCII-safe slug로 정규화."""
    text = unicodedata.normalize("NFC", str(name or ""))
    text = _romanize_korean(text)
    text = text.replace(" ", "_")
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = _ALLOWED.sub("", text)
    text = _MULTI_UNDERSCORE.sub("_", text).strip("_.")
    text = text.lower()
    return text or "unnamed"


def make_unique_key(candidate: str, existing: set[str]) -> str:
    """candidate가 existing에 이미 있으면 stem에 _2, _3 ... suffix를 붙여 고유 키 반환.

    반환된 키는 existing에 자동 추가되므로, 연속 호출 시 배치 내 중복도 방지된다.
    """
    if candidate not in existing:
        existing.add(candidate)
        return candidate

    stem, ext = os.path.splitext(candidate)
    counter = 2
    while True:
        new_key = f"{stem}_{counter}{ext}"
        if new_key not in existing:
            existing.add(new_key)
            return new_key
        counter += 1


def sanitize_filename(name: str) -> str:
    """파일명을 ASCII-safe slug로 정규화. 확장자 보존.

    처리 순서:
      ① macOS NFD → NFC
      ② 한글 → 로마자 (Revised Romanization)
      ③ 띄어쓰기 → 언더바
      ③-b 비ASCII 라틴 문자 정규화 (é→e, ñ→n)
      ④ 특수문자 제거 (영문, 숫자, 언더바, 하이픈, 점만 허용)
      ⑤ 소문자 통일
      ⑥ 확장자 정규화 (.jpeg→.jpg, .tiff→.tif)
    """
    stem, ext = os.path.splitext(name)

    stem = sanitize_path_component(stem)

    # ⑥ 확장자 정규화
    ext = ext.lower()
    ext = {".jpeg": ".jpg", ".tiff": ".tif"}.get(ext, ext)

    return f"{stem}{ext}"
