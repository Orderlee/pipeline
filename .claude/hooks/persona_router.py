#!/usr/bin/env python3
"""UserPromptSubmit hook: 프롬프트에 페르소나 트리거 키워드가 보이면 위임 힌트를 주입.

키워드 정본은 .claude/agents/*.md frontmatter 의 `triggers:` 한 줄(쉼표 구분). description 과
분리한 이유: description 은 매 세션 Agent 도구 목록으로 주입되지만 triggers 는 이 훅만 읽는다
(Claude Code 는 모르는 frontmatter 키를 무시). `triggers:` 가 없는 페르소나는 라우팅 대상이 아니다.

스코어링: full-phrase 일치 2점, 다단어 키워드의 구성 토큰 일치 1점(한글 ≥2자 substring,
ASCII ≥5자 word-boundary). 합계 2점 이상인 페르소나만 상위 3개 제안 — 1점짜리 단독
매치("index" 등 범용 단어)는 노이즈라 침묵한다.
"""
import json
import re
import sys
from pathlib import Path

MIN_ASCII_FULL = 3     # full keyword 최소 길이 (OOM, PSI 살리기)
MIN_ASCII_TOKEN = 5    # 구성 토큰 최소 길이 (use/new/slow 등 범용어 배제)
FIRE_THRESHOLD = 2

_HANGUL = re.compile(r"[가-힣]")


def _extract_triggers(head: str) -> list[str]:
    m = re.search(r"^triggers:\s*(.+)$", head, re.M)
    if not m:
        return []
    return [kw for kw in (raw.strip() for raw in m.group(1).split(",")) if kw]


def _ascii_word_hit(prompt_l: str, token: str) -> bool:
    return re.search(r"(?<![a-z0-9_])" + re.escape(token.lower()) + r"(?![a-z0-9_])", prompt_l) is not None


def _score(prompt: str, prompt_l: str, keywords: list[str]) -> tuple[int, list[str]]:
    score, hits, seen = 0, [], set()
    for kw in keywords:
        is_korean = bool(_HANGUL.search(kw))
        full_hit = (kw in prompt) if is_korean else (
            len(kw) >= MIN_ASCII_FULL
            and ((kw.lower() in prompt_l) if " " in kw else _ascii_word_hit(prompt_l, kw))
        )
        if full_hit and kw not in seen:
            seen.add(kw)
            score += 2
            hits.append(kw)
            continue
        # 다단어 키워드는 구성 토큰으로도 완화 매칭 (한글 조사/어미 변형 대응)
        if " " in kw:
            for tok in kw.split():
                if tok in seen:
                    continue
                if _HANGUL.search(tok):
                    ok = len(tok) >= 2 and tok in prompt
                else:
                    ok = len(tok) >= MIN_ASCII_TOKEN and _ascii_word_hit(prompt_l, tok)
                if ok:
                    seen.add(tok)
                    score += 1
                    hits.append(tok)
    return score, hits


def main() -> None:
    data = json.load(sys.stdin)
    prompt = (data.get("prompt") or "").strip()
    if len(prompt) < 6 or prompt.startswith("/"):
        return
    prompt_l = prompt.lower()
    agents_dir = Path(__file__).resolve().parent.parent / "agents"
    ranked = []
    for f in sorted(agents_dir.glob("*.md")):
        head = f.read_text(encoding="utf-8", errors="ignore")[:4096]
        keywords = _extract_triggers(head)
        if not keywords:
            continue
        score, hits = _score(prompt, prompt_l, keywords)
        if score >= FIRE_THRESHOLD:
            ranked.append((score, f.stem, hits))
    if not ranked:
        return
    ranked.sort(key=lambda t: (-t[0], t[1]))
    parts = [f"{name} (근거: {', '.join(hits[:3])})" for _, name, hits in ranked[:3]]
    print(
        "[persona-router] 트리거 매칭 페르소나: " + " / ".join(parts)
        + " — 이 작업이 해당 도메인이면 그 페르소나로 위임(Agent tool)을 우선 검토하세요."
        + " 라우팅표: docs/references/agent-teams.md §2"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception:
        pass  # ponytail: 라우터 오류가 프롬프트를 막으면 안 됨 — 조용히 통과
