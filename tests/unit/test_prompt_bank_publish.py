"""docker/analysis/prompt_bank_publish.py — manifest 조립 / write-once 분기 / dry-run 무해성.

순수 로직만 검사한다. boto3·docker 는 전부 mock — 이 테스트는 MinIO 나 컨테이너가 없어도 돈다
(CI 러너에는 둘 다 없다).

핵심 회귀 가드 3종:
  1. manifest 는 정본 8필드 **정확히** — 키가 늘면 이미 발행된 뱅크와 대조가 깨진다.
  2. write-once 재발행은 `created_at` 차이를 충돌로 오판하면 안 된다 (멱등이 죽는다).
  3. dry-run 은 `put_object` 를 한 번도 부르면 안 된다 — 발행은 사람이 --apply 로만 한다.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import pathlib
import re
from unittest import mock

import pytest

_PATH = pathlib.Path(__file__).resolve().parents[2] / "docker" / "analysis" / "prompt_bank_publish.py"
_SPEC = importlib.util.spec_from_file_location("prompt_bank_publish", str(_PATH))
pbp = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pbp)


class FakeClientError(Exception):
    """botocore.ClientError 의 최소 대역 — `.response` 만 읽는 우리 판별기에 충분하다."""

    def __init__(self, code: str, status: int | None = None):
        super().__init__(code)
        self.response = {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}


def _write_csv(path: pathlib.Path, rows: list[tuple[str, str]]) -> pathlib.Path:
    lines = ["ID,class,prompt"] + [f"{i},{c},{p}" for i, (c, p) in enumerate(rows)]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _make_ledger(tmp_path: pathlib.Path, *, version: str = "vTEST.1", sentence_count: int | None = 2):
    """banks_inventory.json + CSV 만 있는 최소 원장 (bank_sentences.jsonl 없음 = inventory 폴백)."""
    csv_path = _write_csv(
        tmp_path / f"text_features_{version}.csv", [("0", "a quiet yard"), ("2", "a fire"), ("0", "")]
    )
    led = tmp_path / "led"
    led.mkdir()
    (led / "banks_inventory.json").write_text(
        json.dumps(
            [
                {
                    "version_tag": version,
                    "source": "userwatch",
                    "origin_uri": str(csv_path),
                    "embedding_npz_key": None,
                    "model_name": "PE-Core-L14-336",
                    "sentence_count": sentence_count,
                    "class_counts": {"normal": 1, "fire": 1},
                    "checksum": None,
                    "sentence_storage": "db_backed",
                    "notes": None,
                    "_text_source": "csv:nas",
                }
            ]
        ),
        encoding="utf-8",
    )
    return led, csv_path


# ─────────────────────────── manifest ───────────────────────────
def test_manifest_has_exactly_the_canonical_fields(tmp_path):
    csv_path = _write_csv(tmp_path / "a.csv", [("0", "x")])
    npz_path = tmp_path / "v.npz"
    npz_path.write_bytes(b"\x93NUMPY-not-really")
    bank = {"origin_uri": str(csv_path), "checksum": "stale-value", "model_name": "PE-Core-L14-336"}

    m = pbp.build_manifest(bank, csv_path=str(csv_path), npz_path=str(npz_path), sentence_count=1, null_prompt_count=0)

    assert tuple(m) == pbp.MANIFEST_FIELDS
    assert set(m) == set(pbp.MANIFEST_FIELDS)
    assert m["source_file"] == str(csv_path)
    assert m["csv_sha256"] == hashlib.sha256(csv_path.read_bytes()).hexdigest()
    # origin 이 곧 그 CSV 면 inventory 의 낡은 checksum 대신 재해시 값이 이긴다
    assert m["source_sha256"] == m["csv_sha256"] != "stale-value"
    assert m["npz_sha256"] == hashlib.sha256(npz_path.read_bytes()).hexdigest()
    assert (m["sentence_count"], m["null_prompt_count"]) == (1, 0)
    assert m["embedding_model_name"] == "PE-Core-L14-336"
    assert m["created_at"].endswith("+00:00")


def test_manifest_nulls_when_files_absent(tmp_path):
    bank = {"origin_uri": "/data/x/text_features_v9.json", "checksum": "abc", "model_name": None}
    m = pbp.build_manifest(bank, csv_path=None, npz_path=None, sentence_count=None, null_prompt_count=None)
    assert m["csv_sha256"] is None and m["npz_sha256"] is None
    assert m["source_sha256"] == "abc"  # 파일이 없으면 inventory 값 승계
    assert m["sentence_count"] is None and m["null_prompt_count"] is None
    assert m["embedding_model_name"] == pbp.DEFAULT_MODEL_NAME
    assert json.loads(pbp.manifest_bytes(m).decode("utf-8")) == m


def test_null_prompt_count_matches_ledger_filter_definition(tmp_path):
    """ledger.read_csv_rows() 가 버리는 행수 = null_prompt_count (공백만 있는 행 포함)."""
    p = _write_csv(tmp_path / "b.csv", [("0", "a"), ("0", ""), ("2", "  "), ("3", "b")])
    assert pbp.count_csv_prompts(str(p)) == (2, 2)


def test_ledger_zero_rows_is_unknown_not_zero(tmp_path):
    """벡터 전용 뱅크는 원장에 행이 없다 — 0 으로 발행하면 '문장 0개'라는 거짓이 굳는다."""
    led = tmp_path / "led"
    led.mkdir()
    (led / "bank_sentences.jsonl").write_text(json.dumps({"version_tag": "vA", "text": "x"}) + "\n", encoding="utf-8")
    assert pbp.count_ledger_sentences(str(led), "vA") == 1
    assert pbp.count_ledger_sentences(str(led), "vB") is None
    assert pbp.count_ledger_sentences(str(tmp_path / "nope"), "vA") is None


def test_ledger_counts_duplicate_sentences_so_triangulation_holds(tmp_path):
    """publish 의 문장수 삼각검증이 성립하는 **계약**: ledger 는 뱅크 행을 dedup 하지 않는다.

    같은 문장이 한 뱅크에 두 번 있으면 gidx 로 구분해 두 행을 남긴다 (unique_sentences.jsonl
    쪽에서만 content_hash dedup). 언젠가 ledger 가 dedup 을 시작하면 publish 는 "문장수 불일치"
    로 조용히 거부만 하게 되므로, 원인을 여기서 시끄럽게 잡는다.
    """
    spec = importlib.util.spec_from_file_location("prompt_bank_ledger", str(_PATH.parent / "prompt_bank_ledger.py"))
    ledger = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ledger)

    root = tmp_path / "vDUP"
    root.mkdir()
    _write_csv(
        root / "text_features_vDUP.csv", [("0", "a quiet yard"), ("0", "a quiet yard"), ("2", "a fire"), ("0", "")]
    )
    bank_rows, uniq, coll = ledger.build_ledger(str(tmp_path), [])

    assert len(bank_rows) == 3, "중복 문장이 dedup 됐다 — publish 삼각검증 계약이 깨진다"
    assert [r["gidx"] for r in bank_rows] == [0, 1, 2]
    assert len(uniq) == 2 and coll == []
    # 같은 CSV 를 publish 쪽 카운터로 세면 같은 값이어야 한다 (양쪽 정의 일치)
    assert pbp.count_csv_prompts(str(root / "text_features_vDUP.csv")) == (3, 1)


def test_manifest_diff_ignores_created_at_only():
    base = {k: None for k in pbp.MANIFEST_FIELDS} | {"sentence_count": 10, "created_at": "2026-01-01T00:00:00+00:00"}
    later = dict(base, created_at="2026-08-18T00:00:00+00:00")
    assert pbp.manifest_diff(base, later) == []
    assert pbp.VOLATILE_FIELDS == ("created_at",)
    diff = pbp.manifest_diff(base, dict(later, sentence_count=11))
    assert [d[0] for d in diff] == ["sentence_count"] and diff[0][1:] == (10, 11)


# ─────────────────────────── write-once 분기 ───────────────────────────
def _item(sha: str = "a" * 64, *, path: str = "/tmp/x.csv", size: int = 7) -> dict:
    return {"key": "_prompt_banks/vT/x.csv", "path": path, "size": size, "sha256": sha, "content_type": "text/csv"}


def test_put_write_once_uses_conditional_put_when_absent(tmp_path):
    f = tmp_path / "x.csv"
    f.write_bytes(b"hello\n")
    client = mock.MagicMock()
    status, _ = pbp.put_write_once(client, _item(path=str(f)))
    assert status == "published"
    kwargs = client.put_object.call_args.kwargs
    assert kwargs["IfNoneMatch"] == "*", "write-once 조건부 PUT 이 빠지면 덮어쓰기가 가능해진다"
    assert kwargs["Bucket"] == pbp.BUCKET == "vlm-dataset"
    assert kwargs["Metadata"]["sha256"] == "a" * 64


def test_put_write_once_existing_same_hash_is_success(tmp_path):
    f = tmp_path / "x.csv"
    f.write_bytes(b"hello\n")
    sha = hashlib.sha256(b"hello\n").hexdigest()
    client = mock.MagicMock()
    client.put_object.side_effect = FakeClientError("PreconditionFailed", 412)
    client.head_object.return_value = {"Metadata": {"sha256": sha}, "ETag": '"ignored"'}

    status, detail = pbp.put_write_once(client, _item(sha, path=str(f)))
    assert status == "already" and "동일" in detail
    client.get_object.assert_not_called()  # 같은 바이트면 내려받지 않는다


def test_put_write_once_existing_different_hash_is_conflict(tmp_path, capsys):
    f = tmp_path / "x.csv"
    f.write_bytes(b"hello\n")
    local = hashlib.sha256(b"hello\n").hexdigest()
    client = mock.MagicMock()
    client.put_object.side_effect = FakeClientError("PreconditionFailed", 412)
    client.head_object.return_value = {"Metadata": {"sha256": "b" * 64}}

    status, detail = pbp.put_write_once(client, _item(local, path=str(f)))
    assert status == "conflict"
    assert local in detail and "b" * 64 in detail, "충돌 시 양쪽 해시를 다 보여줘야 사람이 판단한다"


def test_put_write_once_falls_back_to_etag_md5_when_no_metadata(tmp_path):
    f = tmp_path / "x.csv"
    f.write_bytes(b"hello\n")
    sha = hashlib.sha256(b"hello\n").hexdigest()
    md5 = hashlib.md5(b"hello\n").hexdigest()  # noqa: S324 — S3 ETag 규격
    client = mock.MagicMock()
    client.put_object.side_effect = FakeClientError("PreconditionFailed", 412)
    client.head_object.return_value = {"Metadata": {}, "ETag": f'"{md5}"'}
    assert pbp.put_write_once(client, _item(sha, path=str(f)))[0] == "already"

    client.head_object.return_value = {"Metadata": {}, "ETag": '"deadbeef"'}
    assert pbp.put_write_once(client, _item(sha, path=str(f)))[0] == "conflict"


def test_put_write_once_multipart_etag_too_large_is_not_silently_ok(tmp_path):
    """판정 불가를 '동일'로 뭉개면 write-once 가 거짓 안심을 준다 → 충돌로 올린다."""
    f = tmp_path / "x.npz"
    f.write_bytes(b"x")
    client = mock.MagicMock()
    client.put_object.side_effect = FakeClientError("PreconditionFailed", 412)
    client.head_object.return_value = {
        "Metadata": {},
        "ETag": '"abc-3"',
        "ContentLength": pbp.COMPARE_DOWNLOAD_MAX_BYTES + 1,
    }
    status, detail = pbp.put_write_once(client, _item(path=str(f)))
    assert status == "conflict" and "판정 불가" in detail


def test_put_write_once_reraises_non_precondition_errors(tmp_path):
    f = tmp_path / "x.csv"
    f.write_bytes(b"hello\n")
    client = mock.MagicMock()
    client.put_object.side_effect = FakeClientError("AccessDenied", 403)
    with pytest.raises(FakeClientError):
        pbp.put_write_once(client, _item(path=str(f)))


def test_put_plain_is_unconditional_for_timestamped_eval_keys(tmp_path):
    """sync-eval 키에는 UTC 타임스탬프가 들어가 충돌이 없다 → 조건부 PUT 을 걸지 않는다."""
    f = tmp_path / "ledger.jsonl"
    f.write_bytes(b'{"key": 1}\n')
    client = mock.MagicMock()
    item = {
        "key": f"{pbp.EVAL_PREFIX}/sourceh/20260818T000000Z/ledger.jsonl",
        "path": str(f),
        "size": f.stat().st_size,
        "sha256": pbp.sha256_file(str(f)),
        "content_type": "application/x-ndjson",
    }
    assert pbp.put_plain(client, item)[0] == "published"
    kwargs = client.put_object.call_args.kwargs
    assert "IfNoneMatch" not in kwargs
    assert kwargs["Metadata"]["sha256"] == item["sha256"] and kwargs["Bucket"] == "vlm-dataset"


def test_resolve_local_path_remaps_container_paths(tmp_path):
    """원장이 analysis 컨테이너 안에서 만들어지면 origin_uri 가 `/data/...` 로 적힌다."""
    real = tmp_path / "prompts" / "text_features_vT.csv"
    real.parent.mkdir()
    real.write_text("ID,class,prompt\n", encoding="utf-8")
    maps = [("/data", str(tmp_path))]
    assert pbp.resolve_local_path("/data/prompts/text_features_vT.csv", maps) == str(real)
    assert pbp.resolve_local_path(str(real), []) == str(real)  # 이미 로컬이면 그대로
    assert pbp.resolve_local_path("/data/prompts/absent.csv", maps) is None
    assert pbp.resolve_local_path(None, maps) is None


def test_manifest_conflict_compares_stable_fields_only():
    local = pbp.build_manifest(
        {"origin_uri": "/a.csv", "checksum": "h", "model_name": "M"},
        csv_path=None,
        npz_path=None,
        sentence_count=5,
        null_prompt_count=0,
    )
    remote_same = dict(local, created_at="1999-01-01T00:00:00+00:00")
    remote_diff = dict(remote_same, sentence_count=6)

    def _client(remote):
        c = mock.MagicMock()
        c.put_object.side_effect = FakeClientError("PreconditionFailed", 412)
        c.get_object.return_value = {"Body": mock.MagicMock(read=lambda: pbp.manifest_bytes(remote))}
        return c

    item = {
        "key": "_prompt_banks/vT/manifest.json",
        "body": pbp.manifest_bytes(local),
        "size": 1,
        "sha256": "z" * 64,
        "content_type": "application/json",
        "is_manifest": True,
        "manifest": local,
    }
    assert pbp.put_write_once(_client(remote_same), item)[0] == "already"
    status, detail = pbp.put_write_once(_client(remote_diff), item)
    assert status == "conflict" and "sentence_count" in detail and "5" in detail and "6" in detail

    # 키가 통째로 빠진 원격(구 스키마)은 값 비교만으로는 None==None 으로 새어나간다 → 스키마부터 본다
    remote_missing = {k: v for k, v in remote_same.items() if k != "npz_sha256"}
    status, detail = pbp.put_write_once(_client(remote_missing), item)
    assert status == "conflict" and "npz_sha256" in detail and "스키마" in detail
    remote_extra = dict(remote_same, extra_key=1)
    assert pbp.put_write_once(_client(remote_extra), item)[0] == "conflict"


# ─────────────────────────── dry-run 무해성 ───────────────────────────
def test_publish_dry_run_never_uploads(tmp_path, monkeypatch, capsys):
    led, _ = _make_ledger(tmp_path)
    client = mock.MagicMock()
    monkeypatch.setattr(pbp, "make_s3_client", lambda cfg, **kw: client)
    monkeypatch.setattr(
        pbp,
        "minio_config",
        lambda *a, **k: {"MINIO_ENDPOINT": "http://x:9000", "MINIO_ACCESS_KEY": "k", "MINIO_SECRET_KEY": "s"},
    )
    spy = mock.MagicMock(return_value=("published", ""))
    monkeypatch.setattr(pbp, "put_write_once", spy)
    client.head_object.side_effect = FakeClientError("404", 404)

    rc = pbp.main(["publish", "--ledger-dir", str(led), "--version", "vTEST.1", "--prompt-dir", str(tmp_path)])

    assert rc == 0
    spy.assert_not_called()
    client.put_object.assert_not_called()
    out = capsys.readouterr().out
    assert "dry-run" in out and "_prompt_banks/vTEST.1/manifest.json" in out


def test_publish_apply_uploads_data_first_manifest_last(tmp_path, monkeypatch):
    """manifest 는 **완결 마커**라 마지막이어야 한다 (혼합 prefix 방지)."""
    led, csv_path = _make_ledger(tmp_path)
    npz = tmp_path / "vTEST.1.npz"
    npz.write_bytes(b"npz-bytes")
    client = mock.MagicMock()
    client.put_object.return_value = {}
    monkeypatch.setattr(pbp, "make_s3_client", lambda cfg, **kw: client)
    monkeypatch.setattr(
        pbp,
        "minio_config",
        lambda *a, **k: {"MINIO_ENDPOINT": "http://x:9000", "MINIO_ACCESS_KEY": "k", "MINIO_SECRET_KEY": "s"},
    )

    rc = pbp.main(
        ["publish", "--ledger-dir", str(led), "--version", "vTEST.1", "--prompt-dir", str(tmp_path), "--apply"]
    )

    assert rc == 0
    keys = [c.kwargs["Key"] for c in client.put_object.call_args_list]
    assert keys == [
        f"{pbp.PREFIX}/vTEST.1/{csv_path.name}",
        f"{pbp.PREFIX}/vTEST.1/{npz.name}",
        f"{pbp.PREFIX}/vTEST.1/manifest.json",
    ], "데이터 → manifest 순서가 뒤집혔다"
    assert all(c.kwargs["IfNoneMatch"] == "*" for c in client.put_object.call_args_list)
    assert all(c.kwargs["Bucket"] == "vlm-dataset" for c in client.put_object.call_args_list)


def test_publish_apply_stops_at_first_conflict(tmp_path, monkeypatch, capsys):
    """fail-fast: 첫 충돌 뒤 남은 객체를 올리면 한 prefix 안에 두 발행분이 섞인다."""
    led, csv_path = _make_ledger(tmp_path)
    npz = tmp_path / "vTEST.1.npz"
    npz.write_bytes(b"npz-bytes")
    monkeypatch.setattr(pbp, "make_s3_client", lambda cfg, **kw: mock.MagicMock())
    monkeypatch.setattr(
        pbp,
        "minio_config",
        lambda *a, **k: {"MINIO_ENDPOINT": "http://x:9000", "MINIO_ACCESS_KEY": "k", "MINIO_SECRET_KEY": "s"},
    )
    seen: list[str] = []

    def fake_put(_client, item):
        seen.append(item["key"])
        return ("conflict", "sha 다름") if item["key"].endswith(csv_path.name) else ("published", "")

    monkeypatch.setattr(pbp, "put_write_once", fake_put)
    rc = pbp.main(
        ["publish", "--ledger-dir", str(led), "--version", "vTEST.1", "--prompt-dir", str(tmp_path), "--apply"]
    )

    assert rc == 2
    assert seen == [f"{pbp.PREFIX}/vTEST.1/{csv_path.name}"], "충돌 뒤에도 계속 올렸다"
    out = capsys.readouterr().out
    assert "manifest 는 아직 안 올라갔습니다" in out and "미발행 2개" in out


def test_publish_resumes_after_crash_that_left_only_data(tmp_path, monkeypatch, capsys):
    """A 가 CSV 만 올리고 죽은 prefix 에 같은 내용으로 재실행 → already + manifest 로 완결."""
    led, csv_path = _make_ledger(tmp_path)
    monkeypatch.setattr(pbp, "make_s3_client", lambda cfg, **kw: mock.MagicMock())
    monkeypatch.setattr(
        pbp,
        "minio_config",
        lambda *a, **k: {"MINIO_ENDPOINT": "http://x:9000", "MINIO_ACCESS_KEY": "k", "MINIO_SECRET_KEY": "s"},
    )
    seen: list[tuple[str, str]] = []

    def fake_put(_client, item):
        st = "already" if item["key"].endswith(csv_path.name) else "published"
        seen.append((item["key"], st))
        return st, "동일"

    monkeypatch.setattr(pbp, "put_write_once", fake_put)
    rc = pbp.main(
        ["publish", "--ledger-dir", str(led), "--version", "vTEST.1", "--prompt-dir", str(tmp_path), "--apply"]
    )

    assert rc == 0, "already 는 멱등 재실행이지 중단 사유가 아니다"
    assert [s for _, s in seen] == ["already", "published"]
    assert seen[-1][0].endswith("manifest.json")


def test_publish_stops_on_manifest_conflict_and_mixing_stays_detectable(tmp_path, monkeypatch, capsys):
    """manifest 만 있는 prefix(새 순서에서는 A 가 만들 수 없는 상태)에 다른 내용의 B 가 와도
    manifest 충돌에서 멈춘다. 설령 B 의 데이터가 한 개 올라갔어도 manifest 가 csv_sha256 을
    들고 있어 혼합은 **탐지 가능**하다 — 조용한 오염이 되지 않는 게 이 순서의 목적이다."""
    led, csv_path = _make_ledger(tmp_path)
    monkeypatch.setattr(pbp, "make_s3_client", lambda cfg, **kw: mock.MagicMock())
    monkeypatch.setattr(
        pbp,
        "minio_config",
        lambda *a, **k: {"MINIO_ENDPOINT": "http://x:9000", "MINIO_ACCESS_KEY": "k", "MINIO_SECRET_KEY": "s"},
    )
    uploaded: list[str] = []

    def fake_put(_client, item):
        # A 의 잔존 manifest 와 내용이 다르다 → 충돌. 데이터 키는 비어 있어 PUT 자체는 성공한다.
        if item.get("is_manifest"):
            return "conflict", "sentence_count: 로컬 2 ≠ 원격 99"
        uploaded.append(item["key"])
        return "published", ""

    monkeypatch.setattr(pbp, "put_write_once", fake_put)
    rc = pbp.main(
        ["publish", "--ledger-dir", str(led), "--version", "vTEST.1", "--prompt-dir", str(tmp_path), "--apply"]
    )

    assert rc == 2
    # 데이터가 먼저 올라간 건 "같은 내용이면 무해"하지만, 충돌 시점에서 멈춰 이후 객체는 없다.
    assert uploaded == [f"{pbp.PREFIX}/vTEST.1/{csv_path.name}"]
    assert "미발행 0개" in capsys.readouterr().out


def test_publish_refuses_when_ledger_counts_disagree(tmp_path, monkeypatch, capsys):
    """inventory 5 vs CSV 2 → 원장 모순을 정본에 굳히지 않고 거부."""
    led, _ = _make_ledger(tmp_path, sentence_count=5)
    monkeypatch.setattr(pbp, "make_s3_client", lambda *a, **k: pytest.fail("연결하면 안 된다"))
    rc = pbp.main(["publish", "--ledger-dir", str(led), "--version", "vTEST.1", "--prompt-dir", str(tmp_path)])
    assert rc == 1 and "문장수 불일치" in capsys.readouterr().out


def test_publish_missing_ledger_dir_is_friendly_not_traceback(tmp_path, capsys):
    rc = pbp.main(["publish", "--ledger-dir", str(tmp_path / "nope"), "--version", "vX"])
    out = capsys.readouterr().out
    assert rc == 1
    assert "nope" in out and "prompt_bank_ledger.py" in out


def test_publish_unknown_version_lists_available(tmp_path, capsys):
    led, _ = _make_ledger(tmp_path)
    rc = pbp.main(["publish", "--ledger-dir", str(led), "--version", "v9.9.9"])
    out = capsys.readouterr().out
    assert rc == 1 and "vTEST.1" in out


def test_publish_version_lookup_is_case_insensitive_fallback(tmp_path):
    """userwatch 표기가 v/V 비일관 — 대소문자만 다른 요청은 살려준다 (실측)."""
    inv = [{"version_tag": "V1.0.9.0", "origin_uri": None}]
    assert pbp.find_bank(inv, "v1.0.9.0")["version_tag"] == "V1.0.9.0"
    assert pbp.find_bank(inv, "v1.0.9.1") is None


# ─────────────────────────── sync-eval / 프로필 경로 ───────────────────────────
def test_parse_profiles_reads_prompt_geometry_without_importing_it():
    """import 하면 numpy/fiftyone 이 필요하다 → ast 파싱. 실제 파일로 drift 를 잡는다."""
    profs = pbp.load_profiles()
    assert {"sourceh", "frames", "sourcei"} <= set(profs)
    assert profs["sourceh"]["root"].endswith("/sourceh_v2") and profs["sourceh"]["dataset"] == "source-h"
    paths = pbp.profile_paths(profs["frames"])
    assert paths["work"] == profs["frames"]["root"] + "/work"
    assert paths["geo"] == paths["work"] + "/geometry"


def test_parse_profiles_skips_non_literal_values():
    src = (
        "PROFILES = {\n"
        '    "sourceh": {"root": "/r", "dataset": "d", "map_yaml": os.environ.get("X"),\n'
        '             "class_names": {0: "normal"}},\n'
        '    "bad": {"dataset": "no-root"},\n'
        "}\n"
    )
    profs = pbp.parse_profiles(src)
    assert profs == {"sourceh": {"root": "/r", "dataset": "d"}}, profs


def test_sync_eval_dry_run_does_not_cp_or_upload(monkeypatch, capsys):
    calls: list[list[str]] = []

    def fake_run(cmd, timeout=300):
        calls.append(cmd)
        return (0, "123\n", "") if cmd[3] == "stat" else (1, "", "should not happen")

    monkeypatch.setattr(pbp, "_run", fake_run)
    monkeypatch.setattr(pbp, "make_s3_client", lambda *a, **k: pytest.fail("dry-run 이 업로드했다"))
    monkeypatch.setattr(pbp, "fetch_bank_run", lambda *a, **k: pytest.fail("dry-run 이 docker exec 했다"))

    rc = pbp.main(["sync-eval", "--profile", "sourceh"])
    out = capsys.readouterr().out
    assert rc == 0
    assert all(c[3] == "stat" for c in calls), calls  # 존재 확인만 (읽기 전용)
    assert not any("cp" in c for c in calls)
    assert f"{pbp.EVAL_PREFIX}/sourceh/" in out and "bank_run.json" in out


def test_sync_eval_keys_are_unique_within_the_same_second(monkeypatch, capsys):
    """초 해상도만으로는 같은 초의 두 실행이 한 prefix 에 섞인다 → uuid 접미로 분리."""
    monkeypatch.setattr(pbp.time, "strftime", lambda *a, **k: "20260818T000000Z")
    monkeypatch.setattr(pbp, "_run", lambda cmd, timeout=300: (1, "", "absent"))
    monkeypatch.setattr(pbp, "make_s3_client", lambda *a, **k: pytest.fail("dry-run 이 업로드했다"))

    prefixes = []
    for _ in range(2):
        pbp.main(["sync-eval", "--profile", "sourceh"])
        line = next(x for x in capsys.readouterr().out.splitlines() if x.startswith("대상 prefix"))
        prefixes.append(line)
    assert prefixes[0] != prefixes[1], prefixes
    assert "20260818T000000Z-" in prefixes[0]


def test_sync_eval_skips_partially_copied_file(tmp_path, monkeypatch, capsys):
    """stat 크기 ≠ 복사본 크기 = 부분 복사/동시 append. 반쪽 스냅샷은 백업하지 않는다."""

    def fake_run(cmd, timeout=300):
        if cmd[3] == "stat":  # docker exec … stat -c %s <path>
            return (0, "100\n", "") if cmd[-1].endswith("ledger.jsonl") else (1, "", "absent")
        if cmd[1] == "cp":  # docker cp <c>:<src> <dest>
            pathlib.Path(cmd[3]).write_bytes(b"short")  # 5B ≠ 100B
            return 0, "", ""
        return 1, "", "unexpected"

    client = mock.MagicMock()
    monkeypatch.setattr(pbp, "_run", fake_run)
    monkeypatch.setattr(pbp, "fetch_bank_run", lambda *a, **k: (None, "테스트에서 생략"))
    monkeypatch.setattr(pbp, "make_s3_client", lambda *a, **k: client)
    monkeypatch.setattr(
        pbp,
        "minio_config",
        lambda *a, **k: {"MINIO_ENDPOINT": "http://x:9000", "MINIO_ACCESS_KEY": "k", "MINIO_SECRET_KEY": "s"},
    )

    rc = pbp.main(["sync-eval", "--profile", "sourceh", "--apply", "--out", str(tmp_path / "o")])

    assert rc == 0
    keys = [c.kwargs["Key"] for c in client.put_object.call_args_list]
    assert not any(k.endswith("ledger.jsonl") for k in keys), "반쪽 파일이 올라갔다"
    assert len(keys) == 1 and keys[0].endswith("_sync_manifest.json")
    body = json.loads(client.put_object.call_args.kwargs["Body"].decode("utf-8"))
    assert body["files"] == [], "건너뛴 파일이 수집 목록에 남으면 안 된다"
    assert "크기 불일치" in capsys.readouterr().out


def test_fetch_bank_run_survives_truncated_json(monkeypatch):
    """sentinel 뒤 payload 가 잘리면 예전엔 JSONDecodeError 로 sync-eval 전체가 죽었다."""
    monkeypatch.setattr(pbp, "_run", lambda cmd, timeout=300: (0, pbp.BANK_RUN_SENTINEL + '{"run_id": "abc"', ""))
    body, detail = pbp.fetch_bank_run("c", "source-h")
    assert body is None and "JSON 파싱 실패" in detail

    monkeypatch.setattr(pbp, "_run", lambda cmd, timeout=300: (0, "fiftyone migration log\n", ""))
    assert pbp.fetch_bank_run("c", "source-h")[0] is None

    monkeypatch.setattr(
        pbp, "_run", lambda cmd, timeout=300: (0, "noise\n" + pbp.BANK_RUN_SENTINEL + '{"n_gt": 40}\n', "")
    )
    body, detail = pbp.fetch_bank_run("c", "source-h")
    assert detail == "ok" and json.loads(body.decode("utf-8")) == {"n_gt": 40}


def test_sync_eval_artifact_paths_follow_geometry_work_layout():
    profs = pbp.load_profiles()
    paths = pbp.profile_paths(profs["frames"])
    got = [f"{paths['root']}/{rel}" for rel in pbp.EVAL_ARTIFACTS]
    assert f"{paths['work']}/ledger.jsonl" in got
    assert f"{paths['geo']}/runs.jsonl" in got
    assert f"{paths['geo']}/gt_eval_keys.jsonl" in got


def test_bank_run_snippet_emits_parsable_sentinel_line():
    code = pbp.bank_run_snippet("source-h")
    assert "fo.load_dataset('source-h')" in code and pbp.BANK_RUN_SENTINEL in code
    assert "ds.save()" not in code and "delete" not in code, "평가 원장 백업은 읽기 전용이어야 한다"


# ─────────────────────────── 설정 / 정책 ───────────────────────────
def test_minio_config_prefers_env_then_env_file(tmp_path, monkeypatch):
    envf = tmp_path / ".env"
    envf.write_text(
        '# c\nexport MINIO_ENDPOINT="http://file:9000"\n' "MINIO_ACCESS_KEY=fk\nMINIO_SECRET_KEY=fs\n", encoding="utf-8"
    )
    monkeypatch.delenv("MINIO_ENDPOINT", raising=False)
    monkeypatch.delenv("MINIO_ACCESS_KEY", raising=False)
    monkeypatch.setenv("MINIO_SECRET_KEY", "from-env")

    cfg = pbp.minio_config(str(envf))
    assert cfg["MINIO_ENDPOINT"] == "http://file:9000"  # 파일 폴백
    assert cfg["MINIO_SECRET_KEY"] == "from-env"  # env 우선
    assert cfg["MINIO_ACCESS_KEY"] == "fk"


def test_minio_config_missing_is_actionable_error(tmp_path, monkeypatch):
    for k in ("MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY"):
        monkeypatch.delenv(k, raising=False)
    with pytest.raises(RuntimeError, match="MINIO_ENDPOINT"):
        pbp.minio_config(str(tmp_path / "absent.env"))


def test_source_has_no_dead_ip_and_no_bucket_creation():
    """구 10.0.0.x 는 전부 죽었고, 버킷은 5개 고정 정책이다."""
    src = _PATH.read_text(encoding="utf-8")
    assert not re.search(r"172\.168\.\d+\.\d+", src)
    assert not re.search(r"create_bucket\s*\(", src)
    assert re.search(r'^BUCKET = "vlm-dataset"$', src, re.M)


def test_selftest_passes_without_network():
    assert pbp.main(["selftest"]) == 0
