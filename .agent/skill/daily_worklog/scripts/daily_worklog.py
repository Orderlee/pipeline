#!/usr/bin/env python3
"""
daily_worklog.py — 매일 오후 5:30 (월~금) 자동 실행
작업 내용을 수집하여 WORKLOG.md, CLAUDE2.md에 기록합니다.

수집 대상:
  - git log/diff (당일 커밋, 변경 파일, 코드 변경량)
  - docker 컨테이너 상태 (파이프라인 서비스 상태)
  - IDE 작업 이력 (VSCode, Cursor, Antigravity 등)
  - 최근 수정된 파일 (git 외 변경 포함)

사용법:
  python3 .agent/skill/daily_worklog/scripts/daily_worklog.py              # 오늘 날짜 기준 실행
  python3 .agent/skill/daily_worklog/scripts/daily_worklog.py --date 2026-03-17  # 특정 날짜 기준
  python3 .agent/skill/daily_worklog/scripts/daily_worklog.py --dry-run    # 미리보기 (파일 수정 안 함)
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Optional

# ──────────────────────────────────────────────────────────────────────
# 설정
# ──────────────────────────────────────────────────────────────────────
# .agent/skill/daily_worklog/scripts/daily_worklog.py → 리포지토리 루트 (4단계 위)
REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
WORKLOG_PATH = REPO_ROOT / "WORKLOG.md"
CLAUDE_PATH = REPO_ROOT / "CLAUDE2.md"
CONTEXT_PATH = REPO_ROOT / ".agent" / "CONTEXT.md"

# IDE 감지 경로
IDE_DETECTION = {
    "Cursor": [
        Path.home() / ".cursor" / "projects",
        Path.home() / ".cursor-server",
    ],
    "VSCode": [
        Path.home() / ".vscode-server",
        Path.home() / ".config" / "Code",
    ],
    "Antigravity": [
        Path.home() / ".antigravity",
        Path.home() / ".config" / "antigravity",
    ],
}

# 문제/해결 패턴 감지 키워드
PROBLEM_KEYWORDS = [
    "fix", "bug", "error", "issue", "resolve", "patch", "hotfix",
    "복구", "수정", "해결", "오류", "장애", "버그", "문제",
]
TROUBLESHOOT_KEYWORDS = [
    "troubleshoot", "debug", "diagnose", "workaround",
    "트러블슈팅", "디버깅", "진단", "우회",
]


# ──────────────────────────────────────────────────────────────────────
# 유틸리티
# ──────────────────────────────────────────────────────────────────────
def run_cmd(cmd: list[str], cwd: Optional[Path] = None) -> str:
    """명령 실행 후 stdout 반환. 실패 시 빈 문자열."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            cwd=cwd or REPO_ROOT,
        )
        return result.stdout.strip()
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return ""


def get_today_str(date_override: Optional[str] = None) -> str:
    if date_override:
        return date_override
    return datetime.date.today().isoformat()


def get_weekday_kr(date_str: str) -> str:
    d = datetime.date.fromisoformat(date_str)
    days = ["월", "화", "수", "목", "금", "토", "일"]
    return days[d.weekday()]


# ──────────────────────────────────────────────────────────────────────
# 데이터 수집
# ──────────────────────────────────────────────────────────────────────
def collect_git_commits(date_str: str) -> list[dict]:
    """당일 git 커밋 수집 (모든 브랜치)."""
    since = f"{date_str} 00:00:00"
    until_date = datetime.date.fromisoformat(date_str) + datetime.timedelta(days=1)
    until = f"{until_date.isoformat()} 00:00:00"

    raw = run_cmd([
        "git", "log", "--all",
        f"--since={since}", f"--until={until}",
        "--format=%H|%an|%ae|%ai|%s",
    ])
    if not raw:
        return []

    commits = []
    for line in raw.splitlines():
        parts = line.split("|", 4)
        if len(parts) < 5:
            continue
        commits.append({
            "hash": parts[0],
            "author": parts[1],
            "email": parts[2],
            "date": parts[3],
            "message": parts[4],
        })
    return commits


def collect_git_diff_stat(date_str: str) -> dict:
    """당일 변경 파일 통계."""
    since = f"{date_str} 00:00:00"
    until_date = datetime.date.fromisoformat(date_str) + datetime.timedelta(days=1)
    until = f"{until_date.isoformat()} 00:00:00"

    # 당일 커밋 해시 목록
    hashes_raw = run_cmd([
        "git", "log", "--all",
        f"--since={since}", f"--until={until}",
        "--format=%H",
    ])
    if not hashes_raw:
        return {"files_changed": 0, "insertions": 0, "deletions": 0, "changed_files": []}

    hashes = hashes_raw.splitlines()
    all_files = set()
    total_ins = 0
    total_del = 0

    for h in hashes:
        stat = run_cmd(["git", "diff", "--numstat", f"{h}~1", h])
        if not stat:
            # 첫 커밋이거나 부모가 없는 경우
            stat = run_cmd(["git", "diff", "--numstat", "--root", h])
        for line in stat.splitlines():
            parts = line.split("\t")
            if len(parts) >= 3:
                ins = int(parts[0]) if parts[0] != "-" else 0
                dels = int(parts[1]) if parts[1] != "-" else 0
                total_ins += ins
                total_del += dels
                all_files.add(parts[2])

    return {
        "files_changed": len(all_files),
        "insertions": total_ins,
        "deletions": total_del,
        "changed_files": sorted(all_files),
    }


def collect_recently_modified_files(date_str: str) -> list[str]:
    """당일 수정된 파일 (git 추적 여부 무관)."""
    result = run_cmd([
        "find", str(REPO_ROOT),
        "-maxdepth", "4",
        "-name", "*.py", "-o", "-name", "*.yaml", "-o", "-name", "*.yml",
        "-o", "-name", "*.sql", "-o", "-name", "*.sh", "-o", "-name", "*.toml",
        "-o", "-name", "*.md",
    ])
    if not result:
        return []

    today = datetime.date.fromisoformat(date_str)
    modified = []
    for fpath in result.splitlines():
        try:
            p = Path(fpath)
            if not p.exists():
                continue
            mtime = datetime.date.fromtimestamp(p.stat().st_mtime)
            if mtime == today:
                rel = str(p.relative_to(REPO_ROOT))
                if not any(skip in rel for skip in [
                    "__pycache__", ".git", "node_modules", ".pytest_cache"
                ]):
                    modified.append(rel)
        except (OSError, ValueError):
            continue

    return sorted(modified)


def collect_docker_status() -> list[dict]:
    """Docker 컨테이너 상태 수집."""
    raw = run_cmd([
        "docker", "ps", "--format", "{{.Names}}|{{.Status}}|{{.Image}}",
    ])
    if not raw:
        return []

    containers = []
    for line in raw.splitlines():
        parts = line.split("|", 2)
        if len(parts) >= 2 and "pipeline" in parts[0]:
            containers.append({
                "name": parts[0],
                "status": parts[1],
                "image": parts[2] if len(parts) > 2 else "",
            })
    return containers


def detect_ide_usage(date_str: str) -> list[str]:
    """당일 사용된 IDE 감지."""
    today = datetime.date.fromisoformat(date_str)
    used_ides = []

    for ide_name, paths in IDE_DETECTION.items():
        for check_path in paths:
            if not check_path.exists():
                continue
            try:
                # 디렉토리 내 최근 수정 파일 확인
                for item in check_path.iterdir():
                    try:
                        mtime = datetime.date.fromtimestamp(item.stat().st_mtime)
                        if mtime == today:
                            used_ides.append(ide_name)
                            break
                    except (OSError, ValueError):
                        continue
            except (PermissionError, OSError):
                continue
        if ide_name in used_ides:
            continue

    # Cursor 터미널 세션 확인
    cursor_terminals = Path.home() / ".cursor" / "projects" / "home-user-work-p-Datapipeline-Data-data-pipeline" / "terminals"
    if cursor_terminals.exists():
        try:
            for f in cursor_terminals.iterdir():
                mtime = datetime.date.fromtimestamp(f.stat().st_mtime)
                if mtime == today and "Cursor" not in used_ides:
                    used_ides.append("Cursor")
                    break
        except (OSError, PermissionError):
            pass

    # recently-used.xbel 확인 (GNOME 기반 IDE 활동)
    xbel = Path.home() / ".local" / "share" / "recently-used.xbel"
    if xbel.exists():
        try:
            mtime = datetime.date.fromtimestamp(xbel.stat().st_mtime)
            if mtime == today and not used_ides:
                used_ides.append("IDE(미확인)")
        except (OSError, ValueError):
            pass

    return list(set(used_ides)) if used_ides else ["IDE 감지 불가"]


def classify_commit_type(message: str) -> str:
    """커밋 메시지에서 작업 유형 분류."""
    msg_lower = message.lower()
    if any(kw in msg_lower for kw in PROBLEM_KEYWORDS):
        return "fix"
    if any(kw in msg_lower for kw in ["feat", "add", "추가", "구현", "신규"]):
        return "feature"
    if any(kw in msg_lower for kw in ["refactor", "리팩토링", "정리", "개선"]):
        return "refactor"
    if any(kw in msg_lower for kw in ["test", "테스트"]):
        return "test"
    if any(kw in msg_lower for kw in ["doc", "문서", "docs"]):
        return "docs"
    if any(kw in msg_lower for kw in ["config", "설정", "env"]):
        return "config"
    return "other"


def detect_problems_and_solutions(commits: list[dict], changed_files: list[str]) -> list[dict]:
    """커밋 메시지와 변경 파일에서 문제/해결 패턴 감지."""
    problems = []

    for commit in commits:
        msg = commit["message"]
        msg_lower = msg.lower()

        is_fix = any(kw in msg_lower for kw in PROBLEM_KEYWORDS)
        is_troubleshoot = any(kw in msg_lower for kw in TROUBLESHOOT_KEYWORDS)

        if is_fix or is_troubleshoot:
            # 커밋 상세 diff에서 추가 정보 추출
            diff_detail = run_cmd(["git", "log", "-1", "--format=%b", commit["hash"]])

            problems.append({
                "commit_hash": commit["hash"][:8],
                "message": msg,
                "detail": diff_detail,
                "type": "troubleshoot" if is_troubleshoot else "fix",
                "author": commit["author"],
            })

    # 센서/ops/리소스 파일 변경 감지 (잠재적 문제 해결)
    problem_paths = [
        "sensor.py", "ops.py", "duckdb", "minio.py",
        "docker-compose", ".env",
    ]
    for f in changed_files:
        if any(pp in f for pp in problem_paths):
            # 이미 커밋 기반으로 감지된 것과 중복 방지
            already_detected = any(f in str(p.get("message", "")) for p in problems)
            if not already_detected:
                problems.append({
                    "commit_hash": "",
                    "message": f"인프라/설정 파일 변경: {f}",
                    "detail": "",
                    "type": "config_change",
                    "author": "",
                })

    return problems


def categorize_changed_files(files: list[str]) -> dict[str, list[str]]:
    """변경 파일을 카테고리별로 분류."""
    categories = {
        "파이프라인 코드": [],
        "인프라/설정": [],
        "스크립트/도구": [],
        "테스트": [],
        "문서": [],
        "기타": [],
    }

    for f in files:
        if f.startswith("src/vlm_pipeline/"):
            categories["파이프라인 코드"].append(f)
        elif f.startswith("docker/") or f.endswith((".yaml", ".yml", ".env")):
            categories["인프라/설정"].append(f)
        elif f.startswith("scripts/") or f.startswith("gcp/"):
            categories["스크립트/도구"].append(f)
        elif f.startswith("tests/") or "test" in f.lower():
            categories["테스트"].append(f)
        elif f.endswith(".md"):
            categories["문서"].append(f)
        else:
            categories["기타"].append(f)

    return {k: v for k, v in categories.items() if v}


def summarize_paths(paths: list[str], limit: int = 6) -> list[str]:
    """중복 제거 후 경로 일부만 요약."""
    seen = []
    for path in paths:
        if path not in seen:
            seen.append(path)
    if len(seen) <= limit:
        return seen
    return seen[:limit] + [f"... 외 {len(seen) - limit}개"]


def select_changed_files(changed_files: list[str], keywords: list[str]) -> list[str]:
    """키워드에 해당하는 변경 파일만 추출."""
    matched = []
    for path in changed_files:
        lower = path.lower()
        if any(keyword.lower() in lower for keyword in keywords):
            matched.append(path)
    return summarize_paths(matched)


def build_worklog_themes(commits: list[dict], changed_files: list[str]) -> list[dict]:
    """WORKLOG용 주제별 문제/원인/조치 묶음 생성."""
    commit_messages = " ".join(c["message"].lower() for c in commits)

    def has_any(*keywords: str) -> bool:
        return any(
            keyword.lower() in commit_messages
            or any(keyword.lower() in f.lower() for f in changed_files)
            for keyword in keywords
        )

    themed_items: list[dict] = []

    if has_any(
        "raw_ingest",
        "runtime_settings.py",
        "runtime_policy.py",
        "duckdb_base.py",
        "duckdb_ingest.py",
        "defs/process/assets.py",
    ):
        themed_items.append({
            "title": "운영 ingest / process 공통 구조 정리",
            "problem": "ingest와 process 공통 책임이 여러 모듈로 흩어져 있어, 운영 로직을 수정할 때 영향 범위와 설정 반영 지점을 한 번에 보기 어려웠음.",
            "cause": "raw ingest 실행 흐름, runtime env 해석, DuckDB schema 보정과 조회 helper가 각각 다른 레이어에 퍼져 있어 공통 동작을 건드릴 때 수정 포인트가 많아졌음.",
            "actions": [
                "raw ingest에서 상태 생성과 실제 실행 파이프라인을 분리해 진입 구조를 명확히 정리함.",
                "runtime 설정 로더를 추가하고 ingest runtime policy, stuck guard가 같은 설정 해석 경로를 재사용하도록 맞춤.",
                "DuckDB resource에 dispatch/model/raw/image 조회 및 insert helper를 보강하고 schema ensure 흐름을 운영 기준으로 정돈함.",
            ],
            "files": select_changed_files(changed_files, [
                "defs/ingest/assets.py",
                "defs/ingest/runtime_policy.py",
                "resources/runtime_settings.py",
                "resources/duckdb_base.py",
                "resources/duckdb_ingest.py",
                "defs/process/assets.py",
            ]),
        })

    if has_any(
        "video_frames.py",
        "image_caption",
        "clip_to_frame",
        "caption",
        "schema.sql",
    ):
        themed_items.append({
            "title": "프레임 추출 안정화 및 이미지 캡션 메타 확장",
            "problem": "영상 말단 구간에서 프레임 추출이 비거나 불안정할 수 있었고, 이미지 캡션 결과도 텍스트만 남아 후속 추적에 필요한 저장 메타가 부족했음.",
            "cause": "ffmpeg 프레임 추출은 요청 시점이 영상 끝에 가까우면 empty output이 날 수 있었고, image caption 저장은 bucket/key/generated_at 같은 정본 추적 필드가 충분하지 않았음.",
            "actions": [
                "ffmpeg frame extract에 fallback seek 후보와 retry 흐름을 넣어 말단 구간 empty output 상황을 완화함.",
                "image caption JSON을 별도 key로 저장하고 bucket, key, 생성 시각 메타를 image/process 경로에 함께 기록하도록 확장함.",
                "process 중 실패 시 업로드한 frame/image caption JSON을 함께 정리하도록 rollback 경로도 보강함.",
            ],
            "files": select_changed_files(changed_files, [
                "lib/video_frames.py",
                "defs/process/assets.py",
                "resources/duckdb_base.py",
                "resources/duckdb_ingest.py",
                "sql/schema.sql",
            ]),
        })

    if has_any(
        "manual_import.py",
        "import_support.py",
        "prelabeled_import.py",
        "definitions_staging.py",
        "stem",
    ):
        themed_items.append({
            "title": "수동 / 사전 라벨 import 경로 정리",
            "problem": "수동 라벨 import와 사전 라벨링 데이터 적재가 서로 다른 코드 경로에서 중복 구현되어 있었고, staging에서 기존 라벨 결과를 재적재하는 전용 진입점도 부족했음.",
            "cause": "event label JSON 파싱, raw asset 매칭, label key 생성 로직이 manual import 안에 묶여 있었고, prelabeled 데이터는 raw ingest 이후 bbox/image caption까지 연결하는 공통 흐름이 정리되지 않았음.",
            "actions": [
                "manual label import에서 공통 helper를 분리해 label 파일 순회, asset 매칭, event 추출, label 저장 규칙을 재사용 가능하게 정리함.",
                "staging 전용 prelabeled import job을 추가해 raw ingest 후 event / bbox / image caption artifact import까지 한 경로에서 처리하게 구성함.",
                "사전 라벨링 이미지명에서 원본 raw stem을 찾는 정규식 패턴을 보강해 stem 추론 누락 케이스를 보완함.",
            ],
            "files": select_changed_files(changed_files, [
                "defs/label/manual_import.py",
                "defs/label/import_support.py",
                "defs/label/prelabeled_import.py",
                "definitions_staging.py",
            ]),
        })

    if has_any(
        "defs/dispatch/",
        "staging_dispatch.py",
        "dispatch/sensor.py",
        "service.py",
        "sensor_run_status.py",
        "archive.py",
    ):
        themed_items.append({
            "title": "Staging dispatch 서비스 분리 및 흐름 정리",
            "problem": "staging dispatch 처리 로직이 sensor 안에 몰려 있어 중복 요청 체크, 실패 기록, manifest 작성, run 상태 연동을 한 번에 파악하기 어려웠음.",
            "cause": "dispatch request 준비, archive/manifest 경로 계산, DB 기록, in-flight run 검사 로직이 sensor 본문과 run status 처리 코드에 분산되어 유지보수성이 떨어졌음.",
            "actions": [
                "dispatch request 준비, manifest 작성, DB 기록, run request 생성 로직을 service 레이어로 분리해 sensor 책임을 줄임.",
                "중복 request_id, 같은 folder의 진행 중 run, 실패 request upsert 흐름을 DB helper와 공통 함수로 정리함.",
                "dispatch run status와 archive 판단 경로가 같은 tag 해석 함수를 사용하도록 맞춰 상태 전파를 일관되게 정리함.",
            ],
            "files": select_changed_files(changed_files, [
                "defs/dispatch/sensor.py",
                "defs/dispatch/service.py",
                "defs/dispatch/sensor_run_status.py",
                "lib/staging_dispatch.py",
                "defs/ingest/archive.py",
            ]),
        })

    if has_any(
        "docker/.env.staging",
        "docker-compose.yaml",
        "sensor_stuck_guard.py",
        "runtime_settings.py",
        "motherduck",
    ):
        themed_items.append({
            "title": "Staging 환경값 및 운영 보조 설정 정리",
            "problem": "staging 실행 시 DuckDB/MinIO/NAS 경로와 sensor guard 설정이 비어 있거나 분산되어 있어, 실제 테스트 환경을 재현할 때 수동 보정이 많이 필요했음.",
            "cause": "staging env 기본값, compose 공통 설정, stuck run guard / MotherDuck / GCS 관련 옵션이 파일마다 흩어져 있어 환경별 기준을 한 번에 맞추기 어려웠음.",
            "actions": [
                "staging DuckDB, MinIO, incoming/archive/manifest 경로와 주요 timeout / in-flight / guard 옵션을 `.env.staging`에 구체값으로 정리함.",
                "docker compose에서 production dagster 공통 anchor를 분리해 prod/staging 공통점과 차이를 명확히 정리함.",
                "stuck run guard와 ingest feature flag가 runtime settings를 통해 같은 방식으로 로드되도록 맞춰 운영 보조 설정을 단일화함.",
            ],
            "files": select_changed_files(changed_files, [
                "docker/.env.staging",
                "docker/docker-compose.yaml",
                "defs/ingest/sensor_stuck_guard.py",
                "defs/ingest/runtime_policy.py",
                "resources/runtime_settings.py",
            ]),
        })

    if has_any("AGENTS.md", "ANTIGRAVITY.md", "CLAUDE2.md"):
        themed_items.append({
            "title": "운영 / 에이전트 문서 보강",
            "problem": "운영과 staging 구분, 파이프라인 흐름, 자동화 에이전트 참고 문서가 서로 다른 수준으로 흩어져 있어 작업자마다 참조 경로가 달라질 수 있었음.",
            "cause": "짧은 운영 컨텍스트와 장문 레퍼런스, 외부 에이전트용 진입 문서가 분리되어 있었지만 최근 변경사항이 한 번에 정리돼 있지 않았음.",
            "actions": [
                "Codex/에이전트용 `AGENTS.md`를 추가해 운영 규칙, staging/prod 차이, 자주 쓰는 명령을 빠르게 확인할 수 있게 정리함.",
                "`ANTIGRAVITY.md`와 `CLAUDE2.md`를 보강해 에이전트 진입점과 장문 레퍼런스를 역할별로 구분해 둠.",
            ],
            "files": select_changed_files(changed_files, [
                "AGENTS.md",
                "ANTIGRAVITY.md",
                "CLAUDE2.md",
            ]),
        })

    if themed_items:
        return themed_items

    categories = categorize_changed_files(changed_files)
    summary_actions: list[str] = []
    for category, files in categories.items():
        shown = summarize_paths(files, limit=4)
        summary_actions.append(f"{category} 변경을 정리함: {', '.join(f'`{item}`' for item in shown)}")

    commit_lines = [
        f"`{commit['hash'][:8]}` {commit['message']}"
        for commit in commits[:4]
    ]
    if commit_lines:
        summary_actions.append("관련 커밋: " + ", ".join(commit_lines))

    return [{
        "title": "당일 코드 및 설정 정리",
        "problem": "당일 변경이 여러 영역에 걸쳐 있어, 커밋 목록만 보면 실제 수정 범위와 운영 영향 지점을 파악하기 어려웠음.",
        "cause": "자동 기록이 파일/커밋 나열 중심으로 작성되면 코드, 설정, 문서 변경이 어떤 의도로 묶였는지 드러나지 않음.",
        "actions": summary_actions or ["당일 변경 파일과 커밋을 기준으로 작업 내용을 정리함."],
        "files": summarize_paths(changed_files, limit=8),
    }]


# ──────────────────────────────────────────────────────────────────────
# WORKLOG.md 생성
# ──────────────────────────────────────────────────────────────────────
def generate_worklog_entry(
    date_str: str,
    commits: list[dict],
    diff_stat: dict,
    modified_files: list[str],
    docker_status: list[dict],
    ides: list[str],
    problems: list[dict],
) -> str:
    """WORKLOG.md에 추가할 엔트리 생성. 해결방안은 문제/원인/조치 형식으로 상세 작성."""
    lines = []
    lines.append(f"\n## {date_str}")
    lines.append("")

    if not commits and not modified_files:
        lines.append("- (당일 커밋/파일 변경 없음)")
        return "\n".join(lines)

    section_num = 1
    themed_items = build_worklog_themes(commits, diff_stat.get("changed_files", []))

    for item in themed_items[:6]:
        lines.append(f"### {section_num}. {item['title']}")
        lines.append(f"- **문제**: {item['problem']}")
        lines.append(f"- **원인**: {item['cause']}")
        lines.append("- **조치**:")
        for action in item.get("actions", []):
            lines.append(f"    - {action}")
        if item.get("files"):
            lines.append("    - 관련 파일:")
            for f in item["files"]:
                lines.append(f"      - `{f}`")
        lines.append("")
        section_num += 1

    lines.append(f"### {section_num}. 당일 정리")
    if diff_stat["files_changed"] > 0:
        lines.append("- **변경 통계**:")
        lines.append(
            f"    - 변경 파일 **{diff_stat['files_changed']}개**, "
            f"+{diff_stat['insertions']}/-{diff_stat['deletions']}줄."
        )
    if commits:
        lines.append("- **관련 커밋**:")
        for commit in commits[:5]:
            lines.append(f"    - `{commit['hash'][:8]}`: {commit['message']}")
    if docker_status:
        up_count = sum(1 for c in docker_status if "Up" in c["status"])
        lines.append(f"- **서비스 상태**: 파이프라인 서비스 {len(docker_status)}개 컨테이너 중 {up_count}개 정상 가동.")
    if ides and ides != ["IDE 감지 불가"]:
        lines.append(f"- **작업 환경**: {', '.join(sorted(set(ides)))}")
    lines.append("")

    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────
# CLAUDE.md 생성 (트러블슈팅 런북 추가)
# ──────────────────────────────────────────────────────────────────────
def generate_claude_entry(
    date_str: str,
    problems: list[dict],
    commits: list[dict],
    diff_stat: dict,
) -> Optional[str]:
    """CLAUDE.md에 추가할 트러블슈팅/아키텍처 변경 엔트리 생성.
    문제 해결이나 아키텍처 변경이 없으면 None 반환."""

    # 의미 있는 변경이 있는지 확인
    has_meaningful_change = False

    # 문제 해결 커밋이 있는 경우
    fix_commits = [c for c in commits if classify_commit_type(c["message"]) in ("fix", "refactor")]
    if fix_commits or problems:
        has_meaningful_change = True

    # 핵심 파일 변경 감지
    critical_files = [
        f for f in diff_stat.get("changed_files", [])
        if any(kw in f for kw in [
            "sensor.py", "ops.py", "assets.py", "duckdb",
            "minio.py", "schema.sql", "definitions.py",
            "docker-compose", "dagster.yaml",
        ])
    ]
    if critical_files:
        has_meaningful_change = True

    if not has_meaningful_change:
        return None

    changed_files = diff_stat.get("changed_files", [])
    commit_messages = " ".join(c["message"].lower() for c in commits)

    def has_any(*keywords: str) -> bool:
        return any(
            keyword.lower() in commit_messages
            or any(keyword.lower() in f.lower() for f in changed_files)
            for keyword in keywords
        )

    themed_items: list[dict] = []

    if has_any("repair_image_metadata_schema", "image_metadata", "duckdb_base.py", "schema.sql"):
        themed_items.append({
            "title": "운영/스키마 계층 정비",
            "problem": "DuckDB 스키마 보정 또는 카탈로그 안정성이 중요한 변경이 포함되어, 런타임 중 스키마 불일치 재발 가능성을 줄일 필요가 있었음.",
            "cause": "DuckDB 관련 변경은 단순 컬럼 추가를 넘어서 `ensure_schema`, ingest 저장 경로, schema DDL이 함께 맞물리기 때문에, 일부만 수정하면 운영/테스트 환경에서 서로 다른 상태가 남을 수 있음.",
            "actions": [
                "DuckDB 관련 리소스, ingest 저장 로직, schema 정의를 함께 정리해 런타임 스키마 보정 흐름을 일관되게 맞춤.",
                "필요 시 수동 복구 절차나 점검 스크립트를 함께 두어 운영 장애 대응 시 즉시 사용할 수 있도록 준비.",
            ],
            "files": select_changed_files(changed_files, [
                "duckdb_base.py", "duckdb_ingest.py", "duckdb_labeling.py",
                "duckdb_spec.py", "schema.sql", "repair_image_metadata_schema.py",
            ]),
        })

    if has_any("staging_dispatch", "dispatch/sensor", "sensor_bootstrap", "archive.py", "incoming"):
        themed_items.append({
            "title": "Staging dispatch 및 ingest 흐름 정리",
            "problem": "staging에서 trigger JSON 여부, incoming 대기, archive 이동, MinIO 업로드 순서가 테스트 의도와 다르게 동작할 수 있었음.",
            "cause": "dispatch sensor, bootstrap sensor, ingest/archive 경로가 동시에 개입하면 JSON 없는 입력까지 자동 처리되거나, archive 전후 순서가 기대와 어긋날 수 있음.",
            "actions": [
                "dispatch 관련 sensor, archive helper, ingest sensor를 함께 조정해 staging에서 원하는 트리거 조건과 이동 순서를 명확히 분리함.",
                "trigger JSON 유무에 따라 대기/이동/업로드 흐름이 달라지도록 분기 규칙을 정리함.",
            ],
            "files": select_changed_files(changed_files, [
                "defs/dispatch/", "sensor_bootstrap.py", "sensor_incoming.py",
                "sensor_helpers.py", "archive.py", "manifest.py", "staging_dispatch.py",
            ]),
        })

    if has_any("staging_vertex", "image_caption", "captioning", "clip_to_frame", "vertex"):
        themed_items.append({
            "title": "Staging Vertex/VQA 캡셔닝 구조 보강",
            "problem": "staging에서 영상 이벤트 캡션과 이미지/VQA 캡션의 저장 책임이 분리되지 않거나, 프레임 선택 기준이 불명확할 수 있었음.",
            "cause": "captioning 관련 변경은 label/event 레벨과 image/frame 레벨을 함께 건드리므로, 라우팅과 저장 컬럼 의미를 동시에 정리하지 않으면 후속 조회 시 혼선이 생김.",
            "actions": [
                "Vertex helper와 process asset을 함께 수정해 event caption과 image caption의 책임을 분리함.",
                "프레임 relevance 판단, top-1 선택, 이미지 캡션 저장 흐름을 staging 기준으로 정리함.",
            ],
            "files": select_changed_files(changed_files, [
                "staging_vertex.py", "defs/process/assets.py", "defs/label/assets.py",
                "duckdb_ingest.py", "schema.sql",
            ]),
        })

    if has_any("defs/spec/", "duckdb_spec.py", "definitions_staging.py", "staging_sensor.py"):
        themed_items.append({
            "title": "Staging spec 모듈 및 센서 분리",
            "problem": "staging 실험용 spec 흐름과 공용 파이프라인 흐름이 섞이면 테스트 반복성과 책임 경계가 흐려질 수 있었음.",
            "cause": "staging 전용 sensor, asset, spec config가 충분히 분리되지 않으면 운영 경로와 테스트 경로가 같은 job/asset 정의를 공유하게 됨.",
            "actions": [
                "staging 전용 definitions, spec asset, sensor 계층을 분리해 테스트 흐름을 독립적으로 유지함.",
                "spec 관련 DB 저장 구조와 config 해석 경로를 함께 보정함.",
            ],
            "files": select_changed_files(changed_files, [
                "definitions_staging.py", "defs/spec/", "duckdb_spec.py", "spec_config.py",
            ]),
        })

    if has_any("defs/yolo/", "video_frames.py", "image classification", "bbox"):
        themed_items.append({
            "title": "YOLO 실행 조건 및 순서 조정",
            "problem": "bbox/image classification 요청이 있을 때 YOLO가 너무 이른 시점에 실행되거나, 다른 라우팅과 충돌할 수 있었음.",
            "cause": "YOLO는 frame 생성 이후 실행돼야 하는데, dispatch selection 또는 공용 asset 연결 구조상 독립적으로 먼저 뜰 수 있었음.",
            "actions": [
                "YOLO 관련 asset과 routing 조건을 조정해 frame 생성 이후 실행되도록 순서를 맞춤.",
                "bbox, image classification, captioning 조합별로 staging 분기 규칙을 명확히 정리함.",
            ],
            "files": select_changed_files(changed_files, [
                "defs/yolo/", "video_frames.py", "defs/process/assets.py", "env_utils.py",
            ]),
        })

    if has_any(".agent/", ".gitignore", "doc_2", ".env.example", "daily_worklog"):
        themed_items.append({
            "title": "로컬 전용 자산 및 문서/자동화 정리",
            "problem": "로컬 메모, 개인용 스킬, env 예시 파일 같은 자산이 공용 저장소 이력과 섞이면 협업 범위가 모호해질 수 있었음.",
            "cause": "개인 환경에서만 의미 있는 파일까지 추적되면 저장소 이력에 불필요한 변경이 누적되고, 자동화 문서도 실제 운영 상태와 어긋날 수 있음.",
            "actions": [
                "`.agent`, `doc_2`, env 예시, daily worklog 관련 파일을 목적에 맞게 추적/비추적 대상으로 정리함.",
                "자동화 스크립트와 문서 포맷을 실제 운영/테스트 경험에 맞춰 보정함.",
            ],
            "files": select_changed_files(changed_files, [
                ".agent/", ".gitignore", ".env.example", "daily_worklog", "doc_2",
            ]),
        })

    lines = []
    lines.append("")
    lines.append(f"  --- {date_str} 자동 기록 (상세) ---")

    if themed_items:
        for i, item in enumerate(themed_items[:5], 1):
            lines.append(f"  {i}) {item['title']}")
            lines.append(f"    - 문제: {item['problem']}")
            lines.append(f"    - 원인: {item['cause']}")
            lines.append("    - 조치:")
            for action in item["actions"]:
                lines.append(f"      - {action}")
            if item["files"]:
                lines.append("    - 관련 파일:")
                for f in item["files"]:
                    lines.append(f"      - `{f}`")
    elif problems:
        for i, prob in enumerate(problems[:5], 1):
            lines.append(f"  {i}) {prob['message']}")
            lines.append("    - 문제: 당일 변경 중 재발 방지 또는 구조 보정이 필요한 항목으로 분류됨.")
            if prob["detail"]:
                lines.append(f"    - 원인: {prob['detail'][:300]}{'...' if len(prob['detail']) > 300 else ''}")
            else:
                lines.append("    - 원인: 커밋 메시지와 변경 파일 기준으로 트러블슈팅 성격의 작업으로 감지됨.")
            lines.append("    - 조치:")
            lines.append("      - 관련 커밋과 변경 파일을 기준으로 런북 항목에 반영함.")
            if prob["commit_hash"]:
                lines.append(f"      - 참고 커밋: `{prob['commit_hash']}`")

    if critical_files:
        lines.append("  핵심 파일 변경:")
        for f in summarize_paths(critical_files, limit=12):
            lines.append(f"    - `{f}`")

    if commits:
        lines.append("  관련 커밋 요약:")
        for c in commits[:6]:
            lines.append(f"    - [{c['hash'][:8]}] {c['message']}")

    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────
# 파일 업데이트
# ──────────────────────────────────────────────────────────────────────
def update_worklog(entry: str, date_str: str, dry_run: bool = False) -> bool:
    """WORKLOG.md에 엔트리 추가. 이미 해당 날짜 엔트리가 있으면 건너뜀."""
    if not WORKLOG_PATH.exists():
        print(f"[WARN] WORKLOG.md 파일 없음: {WORKLOG_PATH}")
        return False

    content = WORKLOG_PATH.read_text(encoding="utf-8")

    # 이미 해당 날짜 엔트리가 있는지 확인
    date_header = f"## {date_str}"
    if date_header in content:
        if dry_run:
            print(f"[DRY-RUN] WORKLOG.md에 이미 {date_str} 엔트리가 있지만 새로 생성될 내용을 미리보기합니다:")
            print(entry)
            return True
        print(f"[SKIP] WORKLOG.md에 이미 {date_str} 엔트리 존재")
        return False

    # "# WORKLOG" 헤더 다음, 첫 번째 "## 20XX-" 앞에 삽입
    # 가장 최근 날짜 엔트리 앞에 추가
    pattern = r"(# WORKLOG\s*\n)"
    match = re.search(pattern, content)
    if match:
        insert_pos = match.end()
        new_content = content[:insert_pos] + entry + "\n" + content[insert_pos:]
    else:
        # 헤더가 없으면 파일 맨 앞에 추가
        new_content = "# WORKLOG\n" + entry + "\n" + content

    if dry_run:
        print("[DRY-RUN] WORKLOG.md 업데이트 미리보기:")
        print(entry)
        return True

    WORKLOG_PATH.write_text(new_content, encoding="utf-8")
    print(f"[OK] WORKLOG.md 업데이트 완료 ({date_str})")
    return True


def update_claude(entry: str, date_str: str, dry_run: bool = False) -> bool:
    """CLAUDE2.md 마지막에 엔트리 추가."""
    if not entry:
        print("[SKIP] CLAUDE2.md 추가할 내용 없음")
        return False

    if not CLAUDE_PATH.exists():
        print(f"[WARN] CLAUDE2.md 파일 없음: {CLAUDE_PATH}")
        return False

    content = CLAUDE_PATH.read_text(encoding="utf-8")

    # 이미 해당 날짜 자동 기록이 있는지 확인
    if f"{date_str} 자동 기록" in content:
        if dry_run:
            print(f"[DRY-RUN] CLAUDE2.md에 이미 {date_str} 자동 기록이 있지만 새로 생성될 내용을 미리보기합니다:")
            print(entry)
            return True
        print(f"[SKIP] CLAUDE2.md에 이미 {date_str} 자동 기록 존재")
        return False

    # 파일 맨 끝에 추가
    new_content = content.rstrip() + "\n" + entry + "\n"

    if dry_run:
        print("[DRY-RUN] CLAUDE.md 업데이트 미리보기:")
        print(entry)
        return True

    CLAUDE_PATH.write_text(new_content, encoding="utf-8")
    print(f"[OK] CLAUDE2.md 업데이트 완료 ({date_str})")
    return True


# ──────────────────────────────────────────────────────────────────────
# CONTEXT.md 업데이트 (범용 컨텍스트 — 현재 상태 섹션 자동 갱신)
# ──────────────────────────────────────────────────────────────────────
def update_context(
    date_str: str,
    commits: list[dict],
    diff_stat: dict,
    docker_status: list[dict],
    dry_run: bool = False,
) -> bool:
    """CONTEXT.md의 '현재 상태' 섹션을 최신 정보로 갱신."""
    if not CONTEXT_PATH.exists():
        print(f"[WARN] CONTEXT.md 파일 없음: {CONTEXT_PATH}")
        return False

    content = CONTEXT_PATH.read_text(encoding="utf-8")

    # DuckDB에서 현재 row count 가져오기 (가능하면)
    raw_count = ""
    video_count = ""
    query_script = REPO_ROOT / "scripts" / "query_local_duckdb.py"
    duckdb_path = REPO_ROOT / "docker" / "data" / "pipeline.duckdb"
    if query_script.exists() and duckdb_path.exists():
        raw_count = run_cmd([
            "python3", str(query_script),
            "--sql", "SELECT COUNT(*) FROM raw_files;",
        ])
        video_count = run_cmd([
            "python3", str(query_script),
            "--sql", "SELECT COUNT(*) FROM video_metadata;",
        ])
        # 숫자만 추출
        for m in re.findall(r"\d+", raw_count or ""):
            raw_count = m
            break
        for m in re.findall(r"\d+", video_count or ""):
            video_count = m
            break

    # Docker 서비스 상태 요약
    service_summary = "정상 가동 중"
    if docker_status:
        down_services = [c for c in docker_status if "Up" not in c["status"]]
        if down_services:
            names = ", ".join(c["name"] for c in down_services)
            service_summary = f"일부 서비스 중단: {names}"
    else:
        service_summary = "Docker 상태 확인 불가"

    # 오늘 커밋 요약
    commit_summary = f"커밋 {len(commits)}건" if commits else "커밋 없음"
    if diff_stat["files_changed"] > 0:
        commit_summary += f", 파일 {diff_stat['files_changed']}개 변경"

    # 새 "현재 상태" 섹션 생성
    new_status_lines = [
        f"## 9. 현재 상태 (최신 업데이트 시 갱신)",
        "",
    ]
    if raw_count:
        new_status_lines.append(f"- **운영 데이터**: raw_files ~{raw_count}건, video_metadata ~{video_count}건")
    new_status_lines.extend([
        f"- **Production**: {service_summary}",
        f"- **마지막 작업일**: {date_str} ({commit_summary})",
        f"- **자동화**: daily_worklog (평일 17:30 자동 기록), staging_reset, duckdb_staging_wiper",
    ])

    new_status_section = "\n".join(new_status_lines)

    # 기존 "현재 상태" 섹션 교체
    pattern = r"## 9\. 현재 상태 \(최신 업데이트 시 갱신\).*?(?=\n## \d+\.|$)"
    if re.search(pattern, content, re.DOTALL):
        new_content = re.sub(pattern, new_status_section, content, flags=re.DOTALL)
    else:
        print("[WARN] CONTEXT.md에서 '현재 상태' 섹션을 찾을 수 없음")
        return False

    if dry_run:
        print("[DRY-RUN] CONTEXT.md 업데이트 미리보기:")
        print(new_status_section)
        return True

    CONTEXT_PATH.write_text(new_content, encoding="utf-8")
    print(f"[OK] CONTEXT.md 업데이트 완료 ({date_str})")
    return True


# ──────────────────────────────────────────────────────────────────────
# 메인
# ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="일일 작업 로그 자동 생성")
    parser.add_argument("--date", help="대상 날짜 (YYYY-MM-DD), 기본: 오늘")
    parser.add_argument("--dry-run", action="store_true", help="미리보기 (파일 수정 안 함)")
    parser.add_argument("--worklog-only", action="store_true", help="WORKLOG.md만 업데이트")
    parser.add_argument("--claude-only", action="store_true", help="CLAUDE2.md만 업데이트")
    args = parser.parse_args()

    date_str = get_today_str(args.date)
    weekday = get_weekday_kr(date_str)

    print(f"{'='*60}")
    print(f"  일일 작업 로그 생성 — {date_str} ({weekday})")
    print(f"  리포지토리: {REPO_ROOT}")
    print(f"{'='*60}")

    # 주말 체크
    d = datetime.date.fromisoformat(date_str)
    if d.weekday() >= 5:
        print(f"[INFO] {date_str}은 주말({weekday})입니다. 실행을 건너뜁니다.")
        return

    # 데이터 수집
    print("\n[1/6] Git 커밋 수집...")
    commits = collect_git_commits(date_str)
    print(f"  → 커밋 {len(commits)}건")

    print("[2/6] Git diff 통계 수집...")
    diff_stat = collect_git_diff_stat(date_str)
    print(f"  → 변경 파일 {diff_stat['files_changed']}개, +{diff_stat['insertions']}/-{diff_stat['deletions']}")

    print("[3/6] 최근 수정 파일 수집...")
    modified_files = collect_recently_modified_files(date_str)
    print(f"  → 수정 파일 {len(modified_files)}개")

    print("[4/6] Docker 상태 수집...")
    docker_status = collect_docker_status()
    print(f"  → 컨테이너 {len(docker_status)}개")

    print("[5/6] IDE 사용 감지...")
    ides = detect_ide_usage(date_str)
    print(f"  → 감지된 IDE: {', '.join(ides)}")

    print("[6/6] 문제/해결 패턴 분석...")
    problems = detect_problems_and_solutions(commits, diff_stat.get("changed_files", []))
    print(f"  → 문제/해결 패턴 {len(problems)}건")

    # WORKLOG.md 업데이트
    if not args.claude_only:
        print(f"\n{'─'*40}")
        print("WORKLOG.md 엔트리 생성...")
        worklog_entry = generate_worklog_entry(
            date_str, commits, diff_stat, modified_files,
            docker_status, ides, problems,
        )
        update_worklog(worklog_entry, date_str, dry_run=args.dry_run)

    # CLAUDE2.md 업데이트
    if not args.worklog_only:
        print(f"\n{'─'*40}")
        print("CLAUDE2.md 엔트리 생성...")
        claude_entry = generate_claude_entry(date_str, problems, commits, diff_stat)
        update_claude(claude_entry, date_str, dry_run=args.dry_run)

    # CONTEXT.md 업데이트 (범용 컨텍스트 — 항상 갱신)
    print(f"\n{'─'*40}")
    print("CONTEXT.md 현재 상태 갱신...")
    update_context(date_str, commits, diff_stat, docker_status, dry_run=args.dry_run)

    print(f"\n{'='*60}")
    print("  완료!")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
