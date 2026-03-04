import os
import re
import unicodedata

try:
    from korean_romanizer.romanizer import Romanizer
except Exception:  # noqa: BLE001
    Romanizer = None

_ALLOWED = re.compile(r"[^a-zA-Z0-9._-]")
_HANGUL_SYLLABLE = re.compile(r"[\uac00-\ud7a3]")

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
    if not _HANGUL_SYLLABLE.search(text):
        return text

    if Romanizer is not None:
        try:
            romanized = Romanizer(text).romanize()
            if not _HANGUL_SYLLABLE.search(romanized):
                return romanized
            text = romanized
        except Exception:  # noqa: BLE001
            pass

    return "".join(_romanize_hangul_char(ch) for ch in text)


def sanitize_filename(name: str) -> str:
    """
    Normalize filename to an ASCII-safe slug while preserving extension.
    """
    stem, ext = os.path.splitext(name)

    # macOS NFD -> NFC
    stem = unicodedata.normalize("NFC", stem)
    stem = _romanize_korean(stem)

    # spaces to underscore
    stem = stem.replace(" ", "_")

    # strip latin marks (é -> e)
    stem = unicodedata.normalize("NFD", stem)
    stem = "".join(c for c in stem if unicodedata.category(c) != "Mn")

    # keep safe characters only
    stem = _ALLOWED.sub("", stem)
    stem = re.sub(r"_+", "_", stem).strip("_").lower() or "unnamed"

    ext = ext.lower()
    ext = {".jpeg": ".jpg", ".tiff": ".tif"}.get(ext, ext)

    return f"{stem}{ext}"
