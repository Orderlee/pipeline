"""JSON 파일 원자적 쓰기 — 부분 상태가 관측되지 않게 한다.

pending manifest 처럼 **다른 프로세스가 같은 디렉토리를 폴링하는 파일**에 쓸 때 필수다.
`Path.write_text` 는 truncate-then-write 라 쓰는 동안 파일이 빈/부분 상태로 보이고,
incoming_manifest_sensor 는 매 tick `pending/*.json` 을 읽으므로 그 창에 걸리면 정상
manifest 가 JSON 파싱 실패로 보인다 — 손상 manifest 격리와 만나면 멀쩡한 작업을 잃는다.

같은 디렉토리에 임시파일을 쓰고 `os.replace` 로 갈아끼운다. 같은 파일시스템이라
EXDEV 가 없고, POSIX 에서 rename 은 원자적이다. 임시 이름에 `.tmp` 를 붙여 호출부의
`glob("*.json")` 에 잡히지 않게 한다.
"""

from __future__ import annotations

import json
import os
from pathlib import Path


def write_json_atomic(path: str | Path, payload: object, *, indent: int = 2) -> None:
    target = Path(path)
    # 같은 디렉토리여야 os.replace 가 EXDEV 없이 원자적으로 동작한다.
    # pid 를 넣어 동시 writer 끼리 임시파일을 덮어쓰지 않게 한다.
    tmp = target.with_name(f"{target.name}.{os.getpid()}.tmp")
    try:
        tmp.write_text(
            json.dumps(payload, ensure_ascii=False, indent=indent),
            encoding="utf-8",
        )
        os.replace(tmp, target)
    finally:
        # replace 성공 시엔 이미 사라져 no-op. OSError 뿐 아니라 인터럽트
        # (DagsterExecutionInterruptedError 는 BaseException) 경로까지 덮으려면 finally 여야 한다.
        tmp.unlink(missing_ok=True)


def demo() -> None:
    """원자성 자체 점검 — write_text 와 달리 대상 inode 가 교체되어야 한다."""
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        target = Path(d) / "unit.json"
        target.write_text('{"v": 1}', encoding="utf-8")
        before = target.stat().st_ino

        write_json_atomic(target, {"v": 2})

        assert json.loads(target.read_text(encoding="utf-8")) == {"v": 2}
        assert target.stat().st_ino != before, "inode 가 그대로면 제자리 truncate 쓰기다"
        assert sorted(p.name for p in target.parent.iterdir()) == ["unit.json"], "임시파일 잔존"
    print("atomic_json demo OK")


if __name__ == "__main__":
    demo()
