#!/usr/bin/env python3
"""마이그레이션 `@ASSERT_AFTER` 전수 스윕 — 스키마 drift 조기 경보 (읽기 전용).

왜 있나: 러너(`postgres_migration._verify_assertions`)는 **이미 적용된 파일의 ASSERT 도 매 프로세스
부팅마다 재검증**하고, 하나라도 실패하면 `PostgresSchemaBaselineError` 로 호출자를 죽인다. 그건
fail-soft 가 아니라서 센서·인제스트 op 이 통째로 멈추고, 그 뒤 파일의 마이그레이션도 영영 적용되지
않는다. 2026-08-31 에 누군가 HNSW 인덱스 2개를 DROP 했을 때 정확히 이 일이 벌어졌고 —
`production_agent_dispatch_sensor` 가 30초마다 죽고 018~025 가 차단된 상태로 **5일간 아무도 몰랐다**
(실패가 daemon 로그 안에만 있었기 때문).

이 스크립트는 그 상태를 **1초 안에** 밖으로 드러낸다. 러너와 같은 정규식·같은 truthy 판정을 쓰되
psql 읽기 전용 세션으로만 조회하므로 아무것도 바꾸지 않는다.

사용 (호스트에서, repo 루트 기준):
    python3 docker/analysis/check_migration_asserts.py            # 사람이 읽는 요약, 실패 시 exit 1
    python3 docker/analysis/check_migration_asserts.py --json     # 기계용
컨테이너 안이 아니라 **호스트**에서 도는 이유: 마이그레이션 SQL 은 `src/vlm_pipeline/...` 에 있고
analysis 컨테이너에는 그 경로가 마운트돼 있지 않다. DB 접근은 `docker exec … psql` 로 한다
(DSN·비밀번호 불필요, 로컬 소켓 trust).
"""
from __future__ import annotations

import argparse
import json
import pathlib
import re
import subprocess
import sys

MIGRATIONS = pathlib.Path(__file__).resolve().parents[2] / "src/vlm_pipeline/sql/migrations/postgres"
ASSERT_RE = re.compile(r"--\s*@ASSERT_AFTER:\s*(.+)$", re.MULTILINE)  # 러너와 동일


def psql(sql: str, container: str, db: str) -> tuple[bool, str]:
    """단일 SQL → (성공, 첫 컬럼 문자열). 읽기 전용은 PGOPTIONS 로 건다.

    ⚠️ `-tAc "SET …; SELECT …"` 처럼 SET 을 같은 -c 에 넣으면 psql 이 **"SET" 을 먼저 출력**해서
    그게 결과값으로 잡힌다 → 빈 결과(=ASSERT 실패)가 "SET" 이라는 truthy 값으로 둔갑해 **실패를
    통과로 오판**한다(이 스크립트를 처음 돌렸을 때 실제로 겪음). 세션 설정은 반드시 env 로.
    """
    proc = subprocess.run(
        ["docker", "exec", "-e", "PGOPTIONS=-c default_transaction_read_only=on",
         container, "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "airflow", "-d", db, "-tAc", sql],
        capture_output=True, text=True, timeout=60,
    )
    if proc.returncode != 0:
        lines = [ln for ln in proc.stderr.strip().splitlines() if ln.strip()]
        msg = next((ln for ln in lines if "ERROR" in ln), lines[0] if lines else "psql 실패")
        return False, msg.strip()
    return True, proc.stdout.strip()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--container", default="docker-postgres-1")
    ap.add_argument("--db", default="vlm_pipeline")
    ap.add_argument("--json", action="store_true", dest="as_json")
    args = ap.parse_args()

    if not MIGRATIONS.is_dir():
        print(f"마이그레이션 디렉토리 없음: {MIGRATIONS}", file=sys.stderr)
        return 2
    ok, applied_raw = psql("SELECT name FROM _pg_migrations", args.container, args.db)
    if not ok:
        print(f"_pg_migrations 조회 실패: {applied_raw}", file=sys.stderr)
        return 2
    applied = {line.strip() for line in applied_raw.splitlines() if line.strip()}

    total = 0
    failures: list[dict] = []
    for path in sorted(MIGRATIONS.glob("*.sql")):
        for assert_sql in (m.group(1).strip().rstrip(";") for m in ASSERT_RE.finditer(path.read_text(encoding="utf-8"))):
            total += 1
            ran, value = psql(assert_sql, args.container, args.db)
            # 러너의 truthy 판정과 같게: 빈 결과·'f'·'0' 은 실패
            passed = ran and value not in ("", "f", "0")
            if not passed:
                failures.append({
                    "file": path.name,
                    "applied": path.name in applied,
                    "assert": assert_sql,
                    "value": value if ran else f"ERROR: {value}",
                })

    if args.as_json:
        print(json.dumps({"total": total, "failing": len(failures), "failures": failures}, ensure_ascii=False, indent=2))
    else:
        for f in failures:
            state = "적용됨" if f["applied"] else "미적용"
            print(f"FAIL [{state}] {f['file']}: {f['assert'][:100]} → {f['value']!r}")
        print(f"ASSERT {total}건 중 실패 {len(failures)}건")
        if failures:
            print("→ 실패가 '적용됨' 파일이면 **스키마 drift**(누가 객체를 지웠다)이고, 러너가 그 지점에서 멈춰")
            print("   이후 마이그레이션 전부와 dispatch 센서가 함께 죽는다. 즉시 원인 객체를 복구할 것.")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
