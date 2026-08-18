#!/usr/bin/env python3
"""프롬프트 뱅크 발행(publish) — 원장 산출물 → MinIO `vlm-dataset/_prompt_banks/` 정본화.

[[prompt_bank_ledger.py]] 가 **추출**(NAS/로컬 사본 → 원장 3종)을 하고, 이 스크립트는
**발행**을 한다. 경계는 이렇다:

    python3 prompt_bank_ledger.py ledger --out DIR --checksum      # 추출
    python3 prompt_bank_publish.py publish --ledger-dir DIR --version v1.0.8.4          # 계획만
    python3 prompt_bank_publish.py publish --ledger-dir DIR --version v1.0.8.4 --apply  # 발행
    python3 prompt_bank_publish.py sync-eval --profile sourceh --apply                     # 평가원장 백업

왜 write-once 인가:
  뱅크는 "그때 그 문장 집합"이 재현돼야 과거 평가가 의미를 갖는다. 같은 키를 덮어쓰면
  runs.jsonl / `ds.info["bank_run"]` 이 가리키던 뱅크가 조용히 바뀐다 — pseudo-label QA 의
  C-1(라이브 JSON 을 LS 검수가 덮어써 pseudo==GT 오염) 과 같은 종류의 사고다. 그래서 모든
  발행 PUT 은 `IfNoneMatch='*'` 조건부이고, 이미 있으면 **덮어쓰지 않고 대조**한다:
    동일 → "이미 발행됨"(성공, 멱등)
    상이 → 충돌(에러, 양쪽 해시 출력) + **즉시 중단** — 사람이 새 버전 태그를 끊어야 한다.
  업로드 순서도 불변식이다: **CSV/npz 먼저, manifest 마지막**. manifest 존재 = 그 prefix 가
  완결됐다는 마커다. 반대로 올리면 manifest 만 남기고 죽은 발행자 A 의 prefix 에 발행자 B 가
  데이터만 채워 "manifest A + data B" 혼합본이 생긴다 (write-once 인데도 내용이 섞인다).

manifest 필드 출처 (정본 스키마 8개 — 이 밖의 키는 쓰지 않는다):
  source_file           banks_inventory.json `origin_uri` (원본 경로 그대로 = 추적용 문자열)
  source_sha256         원본 파일 재해시. 파일이 로컬에 없으면 inventory `checksum` 승계
  csv_sha256            업로드하는 CSV 재해시 (원본이 CSV 면 source_sha256 과 같은 값이 정상)
  npz_sha256            업로드하는 임베딩 npz 재해시. 없으면 null
  sentence_count        bank_sentences.jsonl 실측 행수 우선, 없으면 inventory `sentence_count`
                        (둘 다 있고 불일치하면 발행 거부 — 원장 모순을 정본에 굳히지 않는다)
  null_prompt_count     CSV 전체 행 − prompt 비어있지 않은 행. CSV 없으면 null
                        (ledger `read_csv_rows()` 가 필터링한 행수와 같은 정의)
  embedding_model_name  inventory `model_name` (없으면 env BANK_EMBED_MODEL)
  created_at            발행 시각 ISO8601 UTC. **유일한 휘발성 필드** — 재발행 대조에서 제외

경로 매핑: 원장은 analysis 컨테이너 안(`/data/...`)에서 만들어질 수 있어 origin_uri 가 호스트에
  없을 수 있다. `--map /data=<호스트경로>` 로 재매핑하며, 기본값은 이 repo 의 `docker/data` 다.

MinIO 접속: env `MINIO_ENDPOINT`/`MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY` 우선, 없으면 `docker/.env`
  파싱 fallback. 엔드포인트 하드코딩 없음 (구 10.0.0.x 주소는 전부 죽었다).
  버킷은 `vlm-dataset` 고정 — 5버킷 정책이라 버킷 생성 코드는 두지 않는다.

정본: docker/analysis/prompt_bank_publish.py (컨테이너 /workspace 는 수동 사본 — README 참조)
"""
from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone

# ─────────────────────────── 상수 (5버킷 정책 고정) ───────────────────────────
BUCKET = "vlm-dataset"
PREFIX = "_prompt_banks"
EVAL_PREFIX = f"{PREFIX}/_eval_ledger"
MANIFEST_NAME = "manifest.json"

# 정본 스키마 — 순서 = 파일에 쓰이는 순서. 키를 늘리려면 노션 편입계획을 먼저 고쳐라.
MANIFEST_FIELDS = (
    "source_file",
    "source_sha256",
    "csv_sha256",
    "npz_sha256",
    "sentence_count",
    "null_prompt_count",
    "embedding_model_name",
    "created_at",
)
# 재발행 대조에서 무시할 필드 (같은 뱅크를 두 번 발행하면 시각만 다르다)
VOLATILE_FIELDS = ("created_at",)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEFAULT_ENV_FILE = os.environ.get("BANK_ENV_FILE", os.path.join(REPO_ROOT, "docker", ".env"))
GEOMETRY_PY = os.environ.get("BANK_GEOMETRY_PY", os.path.join(REPO_ROOT, "docker", "analysis", "prompt_geometry.py"))
ANALYSIS_CONTAINER = os.environ.get("BANK_ANALYSIS_CONTAINER", "docker-analysis-1")
DEFAULT_MODEL_NAME = os.environ.get("BANK_EMBED_MODEL", "PE-Core-L14-336")

# 원격에 sha256 메타데이터도 md5 ETag 도 못 쓰는 객체를 만났을 때 받아서 대조할 최대 크기.
# 뱅크 npz 는 최대 300MB 라 기본값을 넘기면 "판정 불가"로 멈춘다 (조용한 동일 판정 금지).
COMPARE_DOWNLOAD_MAX_BYTES = 32 << 20

# sync-eval 이 걷어오는 평가 원장 — prompt_geometry.py 의 WORK/GEO 기준 상대 경로.
#   WORK = f"{ROOT}/work", GEO = f"{WORK}/geometry"  (set_profile() 과 동일 규칙)
EVAL_ARTIFACTS = (
    "work/ledger.jsonl",              # 프레임 단위 평가 원장 (_load_frames_ledger)
    "work/geometry/runs.jsonl",       # 런 원장 (_append_run)
    "work/geometry/gt_eval_keys.jsonl",  # GT 평가 키 (gtsync)
)
# prompt_geometry.py 를 못 읽을 때만 쓰는 폴백 (파싱 성공 시 이 표는 무시된다)
FALLBACK_PROFILES = {
    "sourceh": {"root": "/data/fiftyone/sourceh_v2", "dataset": "source-h",
             "prompt_dir": "/data/fiftyone/sourceh/prompts"},
    "frames": {"root": "/data/fiftyone/frames_bank", "dataset": "frames_captions",
               "prompt_dir": "/data/fiftyone/sourceh/prompts"},
    "sourcei": {"root": "/data/fiftyone/sourcei", "dataset": "sourcei",
                "prompt_dir": "/data/fiftyone/sourceh/prompts"},
}


def log(m: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


# ─────────────────────────── 해시 / 파일 ───────────────────────────
def sha256_file(path: str) -> str:
    """prompt_bank_ledger.sha256_file 과 동일 알고리즘.

    import 하지 않고 복제한 이유: 컨테이너 /workspace 는 파일 단위 수동 복사라 이 스크립트만
    올라가 있는 상황이 실제로 생긴다 (README 의 drift 경고).
    """
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def md5_file(path: str) -> str:
    """단일 PUT 객체의 ETag 대조용 (멀티파트 ETag 는 `-` 가 붙어 비교 불가)."""
    h = hashlib.md5()  # noqa: S324 — 무결성 아님, S3 ETag 규격을 맞추는 용도
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ─────────────────────────── env / MinIO ───────────────────────────
def load_env_file(path: str) -> dict[str, str]:
    """`KEY=VALUE` 한 줄씩. export/따옴표/주석만 처리하는 최소 파서 (.env 는 shell 이 아니다)."""
    out: dict[str, str] = {}
    if not path or not os.path.isfile(path):
        return out
    with open(path, encoding="utf-8", errors="replace") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            if line.startswith("export "):
                line = line[len("export "):].strip()
            k, _, v = line.partition("=")
            k, v = k.strip(), v.strip()
            if v[:1] == v[-1:] and v[:1] in ("'", '"') and len(v) >= 2:
                v = v[1:-1]
            if k:
                out[k] = v
    return out


def minio_config(env_file: str | None = None) -> dict[str, str]:
    """env 우선, 미설정 키만 `docker/.env` 로 보충. 하드코딩 기본값 없음."""
    keys = ("MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY")
    cfg = {k: os.environ.get(k, "") for k in keys}
    if not all(cfg.values()):
        fallback = load_env_file(env_file or DEFAULT_ENV_FILE)
        for k in keys:
            if not cfg[k]:
                cfg[k] = fallback.get(k, "")
    missing = [k for k in keys if not cfg[k]]
    if missing:
        raise RuntimeError(
            f"MinIO 설정 없음: {', '.join(missing)} — env 로 주거나 "
            f"--env-file 로 .env 경로를 지정하세요 (기본 {env_file or DEFAULT_ENV_FILE})"
        )
    ep = cfg["MINIO_ENDPOINT"]
    if not ep.startswith(("http://", "https://")):
        cfg["MINIO_ENDPOINT"] = "http://" + ep
    return cfg


def make_s3_client(cfg: dict[str, str], *, probe: bool = False):
    """boto3 는 함수 안에서 import — 호스트/컨테이너 어느 쪽 python 으로도 dry-run 은 돌아야 한다.

    `probe=True` 는 dry-run 조회용 — MinIO 가 안 보이는 곳(랩톱·CI)에서 기본 60s×3회 재시도로
    dry-run 이 몇 분씩 매달리지 않도록 짧게 끊는다. 업로드에는 기본 타임아웃을 쓴다(대용량 npz).
    """
    import boto3  # noqa: PLC0415 — 의도적 지연 import

    kwargs = {}
    if probe:
        from botocore.config import Config  # noqa: PLC0415

        kwargs["config"] = Config(connect_timeout=5, read_timeout=15, retries={"max_attempts": 1})
    return boto3.client(
        "s3",
        endpoint_url=cfg["MINIO_ENDPOINT"],
        aws_access_key_id=cfg["MINIO_ACCESS_KEY"],
        aws_secret_access_key=cfg["MINIO_SECRET_KEY"],
        **kwargs,
    )


def _error_code(exc) -> tuple[str, int | None]:
    resp = getattr(exc, "response", None) or {}
    code = str((resp.get("Error") or {}).get("Code") or "")
    status = (resp.get("ResponseMetadata") or {}).get("HTTPStatusCode")
    return code, status


def is_precondition_failed(exc) -> bool:
    """`IfNoneMatch='*'` 2차 PUT 의 거절 (MinIO 라이브 프로브로 확인된 응답)."""
    code, status = _error_code(exc)
    return code in ("PreconditionFailed", "412") or status == 412


def is_not_found(exc) -> bool:
    code, status = _error_code(exc)
    return code in ("404", "NoSuchKey", "NotFound") or status == 404


# ─────────────────────────── 원장 → manifest ───────────────────────────
def find_bank(inventory: list[dict], version: str) -> dict | None:
    """정확 일치 우선, 실패 시 대소문자 무시 (userwatch 표기가 `v`/`V` 비일관 — 실측)."""
    for b in inventory:
        if b.get("version_tag") == version:
            return b
    low = version.lower()
    for b in inventory:
        if str(b.get("version_tag", "")).lower() == low:
            return b
    return None


def default_path_maps() -> list[tuple[str, str]]:
    """컨테이너 경로 → 호스트 경로. `/data` 바인드는 compose 의 `./data` (DOCKER_DATA_HOST_PATH)."""
    host_data = os.environ.get("DOCKER_DATA_HOST_PATH") or os.path.join(REPO_ROOT, "docker", "data")
    return [("/data", host_data)] if os.path.isdir(host_data) else []


def parse_path_maps(items: list[str] | None) -> list[tuple[str, str]]:
    maps: list[tuple[str, str]] = []
    for it in items or []:
        if "=" not in it:
            raise ValueError(f"--map 형식은 CONTAINER=HOST 입니다: {it!r}")
        c, _, h = it.partition("=")
        maps.append((c.rstrip("/"), h.rstrip("/")))
    return maps + default_path_maps()


def resolve_local_path(path: str | None, maps: list[tuple[str, str]]) -> str | None:
    """원장에 적힌 경로를 이 머신에서 실제로 읽을 수 있는 경로로. 못 찾으면 None."""
    if not path:
        return None
    if os.path.isfile(path):
        return path
    for container, host in maps:
        if path == container or path.startswith(container + "/"):
            cand = host + path[len(container):]
            if os.path.isfile(cand):
                return cand
    return None


def count_csv_prompts(path: str) -> tuple[int, int]:
    """(prompt 있는 행, prompt 비어있는 행). ledger.read_csv_rows() 의 필터 정의와 동일."""
    filled = empty = 0
    with open(path, newline="", encoding="utf-8", errors="replace") as fh:
        for r in csv.DictReader(fh):
            if (r.get("prompt") or "").strip():
                filled += 1
            else:
                empty += 1
    return filled, empty


def count_ledger_sentences(ledger_dir: str, version: str) -> int | None:
    """bank_sentences.jsonl 에서 해당 버전 행수. 파일이 없으면 None (inventory 로 폴백).

    0행도 None 으로 돌려준다 — ledger 는 CSV 있는 버전만 쓰므로 0행은 "문장이 0개"가 아니라
    "이 원장이 다루지 않은 버전"이다. 0 으로 발행하면 벡터 전용 뱅크에 대해 거짓을 굳힌다.
    """
    p = os.path.join(ledger_dir, "bank_sentences.jsonl")
    if not os.path.isfile(p):
        return None
    n = 0
    with open(p, encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            if json.loads(line).get("version_tag") == version:
                n += 1
    return n or None


def build_manifest(
    bank: dict,
    *,
    csv_path: str | None,
    npz_path: str | None,
    sentence_count: int | None,
    null_prompt_count: int | None,
    created_at: str | None = None,
) -> dict:
    """정본 스키마 8필드만. 파일이 실재하면 inventory 값 대신 **재해시** 값을 쓴다."""
    source_file = bank.get("origin_uri")
    source_sha = bank.get("checksum")
    csv_sha = None
    if csv_path:
        csv_sha = sha256_file(csv_path)
        # 현행 실측에서 origin_uri 는 곧 그 CSV 다 → 두 값이 같은 게 정상.
        if source_file and os.path.basename(str(source_file)) == os.path.basename(csv_path):
            source_sha = csv_sha
    m = {
        "source_file": source_file,
        "source_sha256": source_sha,
        "csv_sha256": csv_sha,
        "npz_sha256": sha256_file(npz_path) if npz_path else None,
        "sentence_count": sentence_count,
        "null_prompt_count": null_prompt_count,
        "embedding_model_name": bank.get("model_name") or DEFAULT_MODEL_NAME,
        "created_at": created_at or datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    assert tuple(m) == MANIFEST_FIELDS, "manifest 키 집합이 정본 스키마에서 벗어났다"
    return m


def manifest_bytes(manifest: dict) -> bytes:
    return (json.dumps(manifest, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def manifest_stable(manifest: dict) -> dict:
    """발행 시각 등 휘발 필드를 뺀 비교용 사본."""
    return {k: v for k, v in manifest.items() if k not in VOLATILE_FIELDS}


def manifest_diff(local: dict, remote: dict) -> list[tuple[str, object, object]]:
    a, b = manifest_stable(local), manifest_stable(remote)
    return [(k, a.get(k), b.get(k)) for k in sorted(set(a) | set(b)) if a.get(k) != b.get(k)]


# ─────────────────────────── 업로드 (write-once) ───────────────────────────
def compare_remote_manifest(client, key: str, local: dict) -> tuple[str, str]:
    """이미 있는 manifest 와 대조. 반환 (status, detail). status ∈ already/conflict."""
    body = client.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    try:
        remote = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        return "conflict", f"원격 manifest 파싱 실패: {exc!r}"
    if not isinstance(remote, dict) or set(remote) != set(MANIFEST_FIELDS):
        # 키 집합부터 확인해야 한다: 원격에 `npz_sha256` 가 아예 없고 로컬이 None 이면
        # 값 비교만으로는 `None == None` 이라 "동일"로 새어나간다 (구버전 스키마 통과 구멍).
        got = sorted(remote) if isinstance(remote, dict) else type(remote).__name__
        missing = sorted(set(MANIFEST_FIELDS) - set(remote)) if isinstance(remote, dict) else list(MANIFEST_FIELDS)
        extra = sorted(set(remote) - set(MANIFEST_FIELDS)) if isinstance(remote, dict) else []
        return "conflict", f"원격 manifest 스키마 불일치 — 없는 키 {missing} / 잉여 키 {extra} (원격 키: {got})"
    diff = manifest_diff(local, remote)
    if not diff:
        return "already", f"동일 (created_at 만 다름: 원격 {remote.get('created_at')})"
    lines = "; ".join(f"{k}: 로컬 {a!r} ≠ 원격 {b!r}" for k, a, b in diff)
    return "conflict", lines


def compare_remote_object(client, key: str, local_path: str, local_sha: str) -> tuple[str, str]:
    """이미 있는 데이터 파일과 대조: ① 메타데이터 sha256 ② 단일PUT ETag(md5) ③ 소용량 다운로드."""
    head = client.head_object(Bucket=BUCKET, Key=key)
    meta_sha = (head.get("Metadata") or {}).get("sha256")
    if meta_sha:
        if meta_sha == local_sha:
            return "already", "동일 (원격 메타데이터 sha256 일치)"
        return "conflict", f"sha256 로컬 {local_sha} ≠ 원격 {meta_sha}"
    etag = str(head.get("ETag") or "").strip('"')
    if etag and "-" not in etag:
        local_md5 = md5_file(local_path)
        if etag == local_md5:
            return "already", f"동일 (ETag md5 {etag} 일치, 원격에 sha256 메타 없음)"
        return "conflict", f"md5 로컬 {local_md5} ≠ 원격 ETag {etag} (sha256 로컬 {local_sha})"
    size = int(head.get("ContentLength") or 0)
    if size <= COMPARE_DOWNLOAD_MAX_BYTES:
        body = client.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        remote_sha = hashlib.sha256(body).hexdigest()
        if remote_sha == local_sha:
            return "already", "동일 (원격 본문 재해시 일치)"
        return "conflict", f"sha256 로컬 {local_sha} ≠ 원격 {remote_sha}"
    return "conflict", (
        f"판정 불가 — 원격에 sha256 메타 없음 + 멀티파트 ETag({etag}) + {size:,}B 로 "
        f"대조 다운로드 한도({COMPARE_DOWNLOAD_MAX_BYTES:,}B) 초과. 사람이 확인해야 한다"
    )


def put_write_once(client, item: dict) -> tuple[str, str]:
    """`IfNoneMatch='*'` 조건부 PUT. 반환 (status, detail). status ∈ published/already/conflict.

    이미 존재하면 **절대 덮어쓰지 않고** 대조 결과만 돌려준다.
    """
    key, sha = item["key"], item["sha256"]
    extra = {"ContentType": item.get("content_type", "application/octet-stream"),
             "Metadata": {"sha256": sha}, "IfNoneMatch": "*"}
    try:
        if item.get("body") is not None:
            client.put_object(Bucket=BUCKET, Key=key, Body=item["body"], **extra)
        else:
            with open(item["path"], "rb") as fh:
                client.put_object(Bucket=BUCKET, Key=key, Body=fh, **extra)
        return "published", f"PUT {len(item['body']) if item.get('body') is not None else item['size']:,}B"
    except Exception as exc:  # noqa: BLE001 — botocore ClientError 를 지연 import 없이 판별
        if not is_precondition_failed(exc):
            raise
    if item.get("is_manifest"):
        return compare_remote_manifest(client, key, item["manifest"])
    return compare_remote_object(client, key, item["path"], sha)


def put_plain(client, item: dict) -> tuple[str, str]:
    """타임스탬프 키(sync-eval)용 무조건 PUT — 키가 매 실행 유일해 충돌 자체가 없다."""
    extra = {"ContentType": item.get("content_type", "application/octet-stream"),
             "Metadata": {"sha256": item["sha256"]}}
    if item.get("body") is not None:
        client.put_object(Bucket=BUCKET, Key=item["key"], Body=item["body"], **extra)
    else:
        with open(item["path"], "rb") as fh:
            client.put_object(Bucket=BUCKET, Key=item["key"], Body=fh, **extra)
    return "published", f"PUT {item['size']:,}B"


def print_plan(items: list[dict], *, apply: bool, note: str | None = None) -> None:
    mode = "발행" if apply else "dry-run — 업로드 없음"
    print(f"\n대상 버킷 s3://{BUCKET}/  ({mode})")
    if note:
        print(note)
    print(f"{'key':64s} {'size':>12s}  sha256[:12]   출처")
    print("-" * 118)
    for it in items:
        print(f"{it['key']:64s} {it['size']:>12,d}  {it['sha256'][:12]}  {it.get('src', '(생성)')}")
    print("-" * 118)
    print(f"객체 {len(items)}개 / 총 {sum(i['size'] for i in items):,}B")


def probe_remote(client, items: list[dict]) -> None:
    """dry-run 전용 best-effort — 이미 발행된 키를 표시. 접속 실패는 경고로만 흘린다."""
    for it in items:
        try:
            head = client.head_object(Bucket=BUCKET, Key=it["key"])
        except Exception as exc:  # noqa: BLE001
            if is_not_found(exc):
                print(f"  없음     {it['key']}")
                continue
            if "Connect" in type(exc).__name__ or "Endpoint" in type(exc).__name__:
                # 엔드포인트 자체가 안 보이는 상황 — 키마다 반복해봐야 같은 실패다
                print(f"  ⚠️ MinIO 접속 불가 ({type(exc).__name__}) — 원격 조회 생략, 로컬 계획만 표시")
                return
            print(f"  ⚠️ 조회 실패 {it['key']}: {exc!r}")
            continue
        meta = (head.get("Metadata") or {}).get("sha256")
        same = "" if meta is None else ("  (sha256 동일)" if meta == it["sha256"] else "  ❌ sha256 상이")
        print(f"  이미있음 {it['key']}  {head.get('ContentLength', 0):,}B{same}")


# ─────────────────────────── publish ───────────────────────────
def collect_publish_items(args) -> tuple[list[dict], int]:
    """원장 검증 → manifest 조립 → 업로드 계획. 반환 (items, rc). rc!=0 이면 items 는 비었다."""
    led = args.ledger_dir
    if not os.path.isdir(led):
        print(f"❌ --ledger-dir 이 디렉토리가 아닙니다: {led}\n"
              f"   먼저 원장을 만드세요:  python3 prompt_bank_ledger.py ledger --out {led} --checksum")
        return [], 1
    inv_path = os.path.join(led, "banks_inventory.json")
    if not os.path.isfile(inv_path):
        try:
            listing = ", ".join(sorted(os.listdir(led))[:10]) or "(비어 있음)"
        except OSError as exc:
            listing = f"(목록 실패: {exc})"
        print(f"❌ banks_inventory.json 이 없습니다: {inv_path}\n"
              f"   그 디렉토리 내용: {listing}\n"
              f"   `prompt_bank_ledger.py inventory --out {led} --checksum` 이 이 파일을 만듭니다.")
        return [], 1
    try:
        with open(inv_path, encoding="utf-8") as fh:
            inventory = json.load(fh)
    except (json.JSONDecodeError, OSError) as exc:
        print(f"❌ banks_inventory.json 읽기 실패: {exc}")
        return [], 1
    if not isinstance(inventory, list):
        print(f"❌ banks_inventory.json 형식이 리스트가 아닙니다 ({type(inventory).__name__}) — 원장 재생성 필요")
        return [], 1

    bank = find_bank(inventory, args.version)
    if bank is None:
        tags = [str(b.get("version_tag")) for b in inventory]
        print(f"❌ 원장에 버전 {args.version!r} 이 없습니다. 보유 {len(tags)}개:\n   "
              + "\n   ".join(", ".join(tags[i:i + 6]) for i in range(0, min(len(tags), 36), 6)))
        return [], 1
    version = str(bank["version_tag"])

    try:
        maps = parse_path_maps(args.map)
    except ValueError as exc:
        print(f"❌ {exc}")
        return [], 1

    csv_path = resolve_local_path(args.csv or (bank.get("origin_uri") or ""), maps)
    if csv_path and not csv_path.lower().endswith(".csv"):
        csv_path = None  # origin 이 JSON(벡터 전용)인 버전 — 텍스트 없음이 사실이다
    npz_path = resolve_local_path(args.npz, maps) if args.npz else find_npz(version, args.prompt_dir, maps)

    filled = empty = None
    if csv_path:
        filled, empty = count_csv_prompts(csv_path)
    inv_count = bank.get("sentence_count")
    led_count = count_ledger_sentences(led, version)
    # 삼각검증이 성립하는 근거(계약): ledger 의 bank_sentences.jsonl 은 **중복 문장도 그대로 센다**
    # — content_hash dedup 은 unique_sentences.jsonl 쪽에서만 일어나고, 같은 뱅크에 같은 문장이
    # 두 번 있으면 gidx 로 구분해 두 행을 남긴다 (ledger 주석 "gidx 로 구분되어 보존된다").
    # 그래서 led_count == CSV 의 prompt 비지 않은 행수 == inventory sentence_count 여야 한다.
    # 만약 ledger 가 언젠가 dedup 을 시작하면 여기서 조용히 "불일치 → 발행 거부"만 나서 원인을
    # 못 찾는다 → tests/unit/test_prompt_bank_publish.py 가 이 계약을 ledger 실물로 고정한다.
    counts = {n for n in (inv_count, led_count, filled) if n is not None}
    if len(counts) > 1:
        print(f"❌ 문장수 불일치 — inventory={inv_count} bank_sentences.jsonl={led_count} CSV={filled}\n"
              f"   원장 모순을 정본에 굳히지 않는다. `prompt_bank_ledger.py ledger` 를 다시 돌리세요.")
        return [], 1
    sentence_count = next(iter(counts)) if counts else None

    manifest = build_manifest(bank, csv_path=csv_path, npz_path=npz_path,
                              sentence_count=sentence_count, null_prompt_count=empty)
    # ⚠️ 업로드 순서 = 이 리스트 순서다. **데이터 먼저, manifest 마지막**.
    # manifest 를 먼저 올리면 발행자 A 가 manifest 만 올리고 죽었을 때, 내용이 다른 발행자 B 가
    # manifest 충돌을 보고도 CSV/npz 키는 비어 있어 그대로 올려 "manifest A + data B" 혼합
    # prefix 가 생긴다. manifest 를 마지막에 두면 그 존재 자체가 **완결 마커**가 된다.
    items: list[dict] = []
    for path, ctype in ((csv_path, "text/csv"), (npz_path, "application/octet-stream")):
        if not path:
            continue
        items.append({
            "key": f"{PREFIX}/{version}/{os.path.basename(path)}", "path": path,
            "size": os.path.getsize(path), "sha256": sha256_file(path),
            "content_type": ctype, "src": path,
        })
    body = manifest_bytes(manifest)
    items.append({
        "key": f"{PREFIX}/{version}/{MANIFEST_NAME}", "body": body, "size": len(body),
        "sha256": hashlib.sha256(body).hexdigest(), "content_type": "application/json",
        "is_manifest": True, "manifest": manifest, "src": "(생성)",
    })

    print(f"버전 {version} — 문장 {sentence_count if sentence_count is not None else '?'}"
          f" / null prompt {empty if empty is not None else '?'}"
          f" / 모델 {manifest['embedding_model_name']}")
    if not csv_path:
        print(f"⚠️ CSV 를 로컬에서 못 찾음 (origin_uri={bank.get('origin_uri')!r}, "
              f"_text_source={bank.get('_text_source')!r}) — manifest 의 csv_sha256/"
              f"null_prompt_count 는 null 로 발행됩니다. 경로 문제면 --csv 또는 --map 을 쓰세요.")
    if not npz_path:
        print("⚠️ 임베딩 npz 를 못 찾음 — npz_sha256 은 null 로 발행됩니다 (--npz 로 지정 가능).")
    return items, 0


def find_npz(version: str, prompt_dir: str | None, maps: list[tuple[str, str]]) -> str | None:
    """`<prompt_dir>/<version>.npz` 관례. prompt_dir 미지정 시 prompt_geometry 프로필에서 수집."""
    dirs: list[str] = []
    if prompt_dir:
        dirs.append(prompt_dir)
    else:
        seen = set()
        for p in load_profiles().values():
            d = p.get("prompt_dir")
            if d and d not in seen:
                seen.add(d)
                dirs.append(d)
    for d in dirs:
        for name in (f"{version}.npz", f"text_features_{version}.npz"):
            cand = os.path.join(d, name)
            hit = resolve_local_path(cand, maps)
            if hit:
                return hit
    return None


def cmd_publish(args) -> int:
    items, rc = collect_publish_items(args)
    if rc:
        return rc
    assert items[-1].get("is_manifest"), "manifest 는 완결 마커라 반드시 마지막에 올라가야 한다"
    print_plan(items, apply=not args.dry_run,
               note="업로드 순서: 데이터 → manifest(마지막 = 완결 마커). 충돌 시 즉시 중단.")

    if args.dry_run:
        print("\n원격 상태 조회(best-effort):")
        try:
            client = make_s3_client(minio_config(args.env_file), probe=True)
            probe_remote(client, items)
        except Exception as exc:  # noqa: BLE001 — dry-run 은 오프라인에서도 계획을 보여줘야 한다
            print(f"  ⚠️ 조회 생략 ({exc!r}) — 로컬 계획만 표시했습니다")
        print("\n(dry-run — 실제 업로드는 --apply)")
        return 0

    client = make_s3_client(minio_config(args.env_file))
    results = []
    for i, it in enumerate(items):
        status, detail = put_write_once(client, it)
        icon = {"published": "✅", "already": "↺", "conflict": "❌"}[status]
        log(f"{icon} {status:9s} {it['key']}  {detail}")
        results.append(status)
        if status == "conflict":
            # fail-fast: 남은 객체를 계속 올리면 이 prefix 안에서 "이 발행분"과 "저 발행분"이
            # 섞인다. already(대조 성공)는 멱등 재실행이므로 중단 사유가 아니다.
            remaining = [x["key"] for x in items[i + 1:]]
            print(f"\n발행 {results.count('published')} / 기존동일 {results.count('already')} / 충돌 1")
            print(f"❌ 충돌 — 즉시 중단했습니다. 미발행 {len(remaining)}개: {', '.join(remaining) or '(없음)'}")
            if not any(x.get("is_manifest") for x in items[:i + 1]):
                print("   manifest 는 아직 안 올라갔습니다 → 이 prefix 는 미완결로 남습니다(혼합 아님).")
            print("   내용이 바뀐 뱅크면 새 version_tag 로 발행하세요 (write-once 라 덮어쓰지 않습니다).")
            return 2
    print(f"\n발행 {results.count('published')} / 기존동일 {results.count('already')} / 충돌 0")
    return 0


# ─────────────────────────── sync-eval ───────────────────────────
def parse_profiles(source: str) -> dict[str, dict]:
    """prompt_geometry.py 의 PROFILES 를 **import 없이** ast 로 읽는다.

    import 하면 numpy/fiftyone 이 필요해 호스트에서 못 돈다. 문자열 리터럴 값(root/dataset/
    prompt_dir)만 뽑고 `os.environ.get(...)` 같은 비리터럴 값은 건너뛴다.
    """
    tree = ast.parse(source)
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(t, ast.Name) and t.id == "PROFILES" for t in node.targets):
            continue
        if not isinstance(node.value, ast.Dict):
            break
        out: dict[str, dict] = {}
        for k, v in zip(node.value.keys, node.value.values):
            if not isinstance(k, ast.Constant) or not isinstance(v, ast.Dict):
                continue
            inner = {
                ik.value: iv.value
                for ik, iv in zip(v.keys, v.values)
                if isinstance(ik, ast.Constant) and isinstance(iv, ast.Constant) and isinstance(iv.value, str)
            }
            if inner.get("root"):
                out[str(k.value)] = inner
        if out:
            return out
    raise ValueError("PROFILES 딕셔너리를 찾지 못했다")


def load_profiles(path: str | None = None) -> dict[str, dict]:
    p = path or GEOMETRY_PY
    try:
        with open(p, encoding="utf-8") as fh:
            return parse_profiles(fh.read())
    except (OSError, ValueError, SyntaxError):
        return dict(FALLBACK_PROFILES)


def profile_paths(profile: dict) -> dict[str, str]:
    """prompt_geometry.set_profile() 과 동일 규칙: WORK={root}/work, GEO={WORK}/geometry."""
    root = profile["root"].rstrip("/")
    work = f"{root}/work"
    return {"root": root, "work": work, "geo": f"{work}/geometry"}


def _run(cmd: list[str], timeout: int = 300) -> tuple[int, str, str]:
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False)
    except FileNotFoundError:
        return 127, "", f"명령을 찾을 수 없음: {cmd[0]}"
    except subprocess.TimeoutExpired:
        return 124, "", f"타임아웃 {timeout}s: {' '.join(cmd[:4])}…"
    return p.returncode, p.stdout, p.stderr


BANK_RUN_SENTINEL = "<<<BANK_RUN>>>"


def bank_run_snippet(dataset: str) -> str:
    return (
        "import json, fiftyone as fo\n"
        f"ds = fo.load_dataset({dataset!r})\n"
        "info = (ds.info or {}).get('bank_run')\n"
        f"print({BANK_RUN_SENTINEL!r} + json.dumps(info, ensure_ascii=False, default=str))\n"
    )


def fetch_bank_run(container: str, dataset: str, timeout: int = 300) -> tuple[bytes | None, str]:
    """컨테이너 안 fiftyone 의 `ds.info['bank_run']` 을 JSON 으로 뽑는다. 실패는 fail-forward."""
    rc, out, err = _run(["docker", "exec", container, "python3", "-c", bank_run_snippet(dataset)], timeout)
    if rc != 0:
        return None, f"docker exec 실패(rc={rc}): {(err or out).strip()[-300:]}"
    for line in out.splitlines():
        if line.startswith(BANK_RUN_SENTINEL):
            payload = line[len(BANK_RUN_SENTINEL):]
            try:
                parsed = json.loads(payload)
            except json.JSONDecodeError as exc:
                # 출력이 잘리거나(파이프 버퍼) 컨테이너 로그가 끼어들면 여기서 죽었었다 —
                # 평가 원장 백업 전체를 날리지 않도록 fail-forward 계약을 지킨다.
                return None, f"sentinel payload JSON 파싱 실패: {exc} (앞 120자: {payload[:120]!r})"
            return (json.dumps(parsed, ensure_ascii=False, indent=2) + "\n").encode("utf-8"), "ok"
    return None, "출력에서 sentinel 을 못 찾음 (fiftyone 로그만 있음)"


def cmd_sync_eval(args) -> int:
    profiles = load_profiles()
    prof = profiles.get(args.profile)
    if prof is None:
        print(f"❌ 알 수 없는 프로필 {args.profile!r} — 가능: {', '.join(sorted(profiles))}")
        return 1
    paths = profile_paths(prof)
    dataset = prof.get("dataset") or ""
    container = args.container
    # 초 해상도 타임스탬프만 쓰면 같은 초에 두 번 돌린 실행이 **한 prefix 에 섞인다**
    # (스냅샷이 시점 단위로 갈라져야 하는데 파일별로 다른 실행이 될 수 있다) → 짧은 uuid 접미.
    ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:6]
    base = f"{EVAL_PREFIX}/{args.profile}/{ts}"
    print(f"프로필 {args.profile} — 컨테이너 {container} / ROOT {paths['root']} / dataset {dataset!r}")
    print(f"대상 prefix s3://{BUCKET}/{base}/")

    remote_files: list[tuple[str, int]] = []
    for rel in EVAL_ARTIFACTS:
        p = f"{paths['root']}/{rel}"
        rc, out, err = _run(["docker", "exec", container, "stat", "-c", "%s", p], timeout=30)
        if rc != 0:
            print(f"  - 없음 {p}  ({(err or out).strip()[:120]})")
            continue
        remote_files.append((p, int(out.strip() or 0)))
        print(f"  + 있음 {p}  {int(out.strip() or 0):,}B")
    if not remote_files:
        print("⚠️ 걷어올 평가 원장 파일이 없습니다 (프로필/컨테이너 확인). bank_run 만 시도합니다.")

    if args.dry_run:
        print("\n올릴 키(예정):")
        for p, size in remote_files:
            print(f"  {base}/{os.path.basename(p)}  {size:,}B")
        print(f"  {base}/bank_run.json           (docker exec python -c … fiftyone)")
        print(f"  {base}/_sync_manifest.json     (수집 목록·해시)")
        print("\n(dry-run — docker cp / 업로드 없음. 실행은 --apply)")
        return 0

    outdir = args.out or tempfile.mkdtemp(prefix=f"bank-eval-{args.profile}-")
    os.makedirs(outdir, exist_ok=True)
    items: list[dict] = []
    collected: list[dict] = []
    for p, remote_size in remote_files:
        dest = os.path.join(outdir, os.path.basename(p))
        rc, out, err = _run(["docker", "cp", f"{container}:{p}", dest], timeout=args.timeout)
        if rc != 0:
            log(f"⚠️ docker cp 실패 {p}: {(err or out).strip()[:200]} — 건너뜀")
            continue
        local_size = os.path.getsize(dest)
        if local_size != remote_size:
            # stat 으로 잰 크기와 다르면 부분 복사이거나 복사 도중 append 된 것(원장은 계속 자란다).
            # 반쪽 스냅샷을 정본 백업에 올리면 나중에 "그때 그 평가"를 재현할 수 없다 → skip.
            log(f"⚠️ 크기 불일치 {p}: 원격 {remote_size:,}B ≠ 로컬 {local_size:,}B "
                f"(부분 복사/동시 append 의심) — 건너뜀. 원장 쓰기가 끝난 뒤 재실행하세요")
            continue
        sha = sha256_file(dest)
        items.append({"key": f"{base}/{os.path.basename(p)}", "path": dest,
                      "size": local_size, "sha256": sha,
                      "content_type": "application/x-ndjson", "src": p})
        collected.append({"source_path": p, "name": os.path.basename(p),
                          "size": local_size, "sha256": sha})

    body, detail = fetch_bank_run(container, dataset, timeout=args.timeout)
    if body is None:
        log(f"⚠️ bank_run 수집 실패 — {detail} (나머지는 계속 올립니다)")
    else:
        sha = hashlib.sha256(body).hexdigest()
        items.append({"key": f"{base}/bank_run.json", "body": body, "size": len(body),
                      "sha256": sha, "content_type": "application/json",
                      "src": f"{container}:fiftyone[{dataset}].info['bank_run']"})
        collected.append({"source_path": f"fiftyone[{dataset}].info['bank_run']",
                          "name": "bank_run.json", "size": len(body), "sha256": sha})

    sync_manifest = {
        "profile": args.profile, "dataset": dataset, "container": container,
        "root": paths["root"], "work": paths["work"], "geo": paths["geo"],
        "captured_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "files": collected,
    }
    sm = (json.dumps(sync_manifest, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    items.append({"key": f"{base}/_sync_manifest.json", "body": sm, "size": len(sm),
                  "sha256": hashlib.sha256(sm).hexdigest(), "content_type": "application/json",
                  "src": "(생성)"})

    print_plan(items, apply=True)
    client = make_s3_client(minio_config(args.env_file))
    for it in items:
        status, detail = put_plain(client, it)
        log(f"✅ {status} {it['key']}  {detail}")
    print(f"\n로컬 사본: {outdir}")
    return 0


# ─────────────────────────── selftest ───────────────────────────
def cmd_selftest(_args) -> int:
    # 1) manifest 는 정본 8필드 정확히 — 키가 늘거나 줄면 정본 스키마 계약 위반
    m = build_manifest({"origin_uri": "/x/a.csv", "checksum": "abc", "model_name": "PE-Core-L14-336"},
                       csv_path=None, npz_path=None, sentence_count=3, null_prompt_count=0)
    assert tuple(m) == MANIFEST_FIELDS, tuple(m)
    # 2) created_at 만 다른 manifest 는 "같은 뱅크"다 (재발행 멱등의 근거)
    m2 = dict(m, created_at="2000-01-01T00:00:00+00:00")
    assert manifest_diff(m, m2) == []
    assert manifest_diff(m, dict(m, sentence_count=4)) != []
    # 3) 버킷은 5버킷 정책 고정 + 엔드포인트 하드코딩 없음
    assert BUCKET == "vlm-dataset"
    with open(os.path.abspath(__file__), encoding="utf-8") as fh:
        src = fh.read()
    assert not re.search(r"172\.168\.\d+\.\d+", src), "죽은 구 IP 하드코딩"
    assert not re.search(r"create_bucket\s*\(", src), "5버킷 정책 — 버킷 생성 호출 금지"
    # 4) 프로필 경로 규칙이 prompt_geometry 와 같은가 (WORK={root}/work)
    profs = load_profiles()
    assert "sourceh" in profs, sorted(profs)
    assert profile_paths(profs["sourceh"])["geo"].endswith("/work/geometry")
    # 5) .env 폴백 파서
    import tempfile as _tf

    with _tf.TemporaryDirectory() as td:
        p = os.path.join(td, ".env")
        with open(p, "w", encoding="utf-8") as fh:
            fh.write("# c\nexport MINIO_ENDPOINT=\"http://h:9000\"\nMINIO_ACCESS_KEY=k\nMINIO_SECRET_KEY=s\n")
        env = load_env_file(p)
        assert env["MINIO_ENDPOINT"] == "http://h:9000" and env["MINIO_SECRET_KEY"] == "s", env
    print("✅ selftest 통과 (5종)")
    return 0


# ─────────────────────────── CLI ───────────────────────────
def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="command", required=True)

    def _common(p):
        p.add_argument("--env-file", default=None, help=f"MinIO 자격 폴백 .env (기본 {DEFAULT_ENV_FILE})")
        g = p.add_mutually_exclusive_group()
        g.add_argument("--dry-run", dest="dry_run", action="store_true", default=True)
        g.add_argument("--apply", dest="dry_run", action="store_false", help="실제 업로드")

    p_pub = sub.add_parser("publish", help="뱅크 1버전 → manifest + CSV/npz 발행 (write-once)")
    p_pub.add_argument("--ledger-dir", required=True, help="prompt_bank_ledger.py 산출 디렉토리")
    p_pub.add_argument("--version", required=True, help="version_tag (예: v1.0.8.4)")
    p_pub.add_argument("--csv", default=None, help="CSV 경로 직접 지정 (기본: inventory origin_uri)")
    p_pub.add_argument("--npz", default=None, help="임베딩 npz 경로 직접 지정")
    p_pub.add_argument("--prompt-dir", default=None, help="npz 탐색 디렉토리 (기본: prompt_geometry 프로필)")
    p_pub.add_argument("--map", action="append", default=None,
                       help="컨테이너=호스트 경로 재매핑 (반복 가능, 기본 /data=<repo>/docker/data)")
    _common(p_pub)

    p_sync = sub.add_parser("sync-eval", help="평가 원장 JSONL + bank_run 스냅샷 백업")
    p_sync.add_argument("--profile", required=True, choices=sorted(load_profiles()))
    p_sync.add_argument("--container", default=ANALYSIS_CONTAINER)
    p_sync.add_argument("--out", default=None, help="추출 사본을 둘 디렉토리 (기본 임시 디렉토리)")
    p_sync.add_argument("--timeout", type=int, default=600, help="docker cp/exec 타임아웃 초")
    _common(p_sync)

    sub.add_parser("selftest", help="네트워크 없이 도는 불변식 검사")
    return ap


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    fn = {"publish": cmd_publish, "sync-eval": cmd_sync_eval, "selftest": cmd_selftest}[args.command]
    try:
        return fn(args)
    except RuntimeError as exc:  # 설정 누락 등 — 스택트레이스 대신 한 줄로
        print(f"❌ {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
