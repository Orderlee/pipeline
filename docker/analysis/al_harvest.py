#!/usr/bin/env python3
"""① 루프 닫기 — Label Studio 확정 라벨을 선별로 되돌리고 al_frames 에 재유입한다.

    docker exec docker-dagster-daemon-1 python3 /tmp/al_harvest.py --round <round_id> [--apply]

지금까지 루프는 **한 방향**이었다: 선별 → LS → 사람 검수 → 거기서 끝. 확정 라벨이
`al_frames` 로 돌아오지 않으니 다음 라운드의 프로브는 **첫 라운드와 똑같은 GT** 로 학습됐다.
그러면 AL 은 1회성이고, competence 가 오르며 전략이 바뀐다는 DCoM 의 전제 자체가 성립 못 한다.

이 스크립트가 그 되돌아오는 길이다:
  LS annotations → al_selections.labeled_cls/labeled_at  (어느 선별이 무엇으로 확정됐나)
                 → al_frames upsert (label_source='human')  (다음 학습이 쓸 GT)
                 → al_rounds.status='labeled'

⚠️ **완료된(annotation 이 달린) 태스크만** 가져온다. 미검수 태스크를 normal 로 채우면
   "사람이 아무것도 못 봤다"와 "아직 안 봤다"가 뒤섞여 GT 가 오염된다.
⚠️ 박스가 0개인 확정은 **정상 값**이다 — 사람이 보고 아무것도 없다고 판정한 것이라
   `normal` 로 기록한다. 이 저장소가 반복해서 잃어버린 정보다(사람 확인 negative).
⚠️ 기본 dry-run.
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import Counter

import psycopg2
import requests
from psycopg2.extras import execute_values

PG = dict(host="docker-postgres-1", port=5432, user="airflow",
          password=os.environ.get("POSTGRES_PASSWORD", "airflow"), dbname="vlm_pipeline")


def ls_tasks(ls_url: str, token: str, project_id: int) -> dict[int, list]:
    """project 의 태스크 → {task_id: [annotation result...]}. 페이지네이션 처리."""
    hdr = {"Authorization": f"Token {token}"}
    out, page = {}, 1
    while True:
        r = requests.get(f"{ls_url}/api/tasks", headers=hdr,
                         params={"project": project_id, "page": page, "page_size": 200}, timeout=60)
        if r.status_code == 404:
            break
        r.raise_for_status()
        body = r.json()
        items = body.get("tasks") if isinstance(body, dict) else body
        if not items:
            break
        for t in items:
            anns = t.get("annotations") or []
            if anns:                                  # 검수 완료분만
                res = []
                for a in anns:
                    if a.get("was_cancelled"):
                        continue                      # skip 처리된 것은 확정이 아니다
                    res.extend(a.get("result") or [])
                out[int(t["id"])] = res
        if isinstance(body, dict) and not body.get("next"):
            break
        page += 1
        if page > 200:
            break
    return out


def result_to_cls(result: list, fallback: str = "normal") -> tuple[str, int]:
    """LS RectangleLabels result → (대표 클래스, 박스 수). 박스 0 = 사람이 없다고 판정 → normal."""
    labels: list[str] = []
    for r in result:
        for v in (r.get("value") or {}).get("rectanglelabels") or []:
            labels.append(str(v))
    if not labels:
        return fallback, 0
    return Counter(labels).most_common(1)[0][0], len(labels)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--round", required=True)
    ap.add_argument("--ls-url", default=os.environ.get("LS_URL", "http://labelstudio:8080"))
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    conn = psycopg2.connect(**PG); cur = conn.cursor()
    cur.execute("SELECT pool_cohort, ls_project_id, status FROM al_rounds WHERE round_id=%s", (args.round,))
    row = cur.fetchone()
    if not row:
        raise SystemExit(f"round 없음: {args.round}")
    pool, pid, status = row
    if not pid:
        raise SystemExit("이 라운드는 아직 LS 로 보내지지 않았다(ls_project_id 없음)")
    print(f"round={args.round} · pool={pool} · ls_project={pid} · status={status}")

    cur.execute("SELECT frame_key, ls_task_id FROM al_selections "
                "WHERE round_id=%s AND ls_task_id IS NOT NULL", (args.round,))
    by_task = {int(t): fk for fk, t in cur.fetchall()}
    print(f"추적 중인 태스크 {len(by_task)}")

    anns = ls_tasks(args.ls_url, os.environ.get("LS_API_KEY", ""), int(pid))
    print(f"LS 검수 완료 태스크 {len(anns)}")

    upd, stats = [], Counter()
    for tid, res in anns.items():
        fk = by_task.get(tid)
        if not fk:
            stats["추적밖"] += 1; continue
        cls, nbox = result_to_cls(res)
        upd.append((cls, args.round, fk))
        stats[cls] += 1
        stats["박스0(사람확인 negative)"] += (nbox == 0)
    print(f"회수 대상 {len(upd)} · {dict(stats)}")

    if not args.apply:
        print("\n[DRY-RUN] --apply 로 실제 반영 (al_selections + al_frames + status)")
        return
    if not upd:
        print("반영할 것이 없다"); return

    execute_values(cur, "UPDATE al_selections SET labeled_cls=v.cls, labeled_at=NOW() "
                        "FROM (VALUES %s) AS v(cls, rid, fk) "
                        "WHERE al_selections.round_id=v.rid AND al_selections.frame_key=v.fk", upd,
                   page_size=500)
    # ★ 재유입: 풀 코호트의 그 프레임을 사람 확정 라벨로 승격한다 → 다음 라운드가 이걸 학습한다
    cur.execute("""
        UPDATE al_frames f SET cls = s.labeled_cls, label_source='human',
               label_rule='ls_finalized:' || s.round_id
        FROM al_selections s
        WHERE s.round_id=%s AND s.labeled_cls IS NOT NULL
          AND f.cohort=%s AND f.frame_key=s.frame_key
    """, (args.round, pool))
    promoted = cur.rowcount
    cur.execute("UPDATE al_rounds SET status='labeled' WHERE round_id=%s", (args.round,))
    conn.commit()
    print(f"[DONE] al_selections {len(upd)} 갱신 · al_frames {promoted} 승격(label_source=human) · status=labeled")
    cur.close(); conn.close()


if __name__ == "__main__":
    main()
