from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("dagster", exc_type=ImportError)

from vlm_pipeline.defs.ingest.sensor_helpers import (  # noqa: E402
    collect_in_flight_manifest_paths,
    load_pending_manifest_entries,
)


class _FakeLog:
    def __init__(self) -> None:
        self.warnings: list[str] = []

    def warning(self, message: str) -> None:
        self.warnings.append(message)

    def info(self, _message: str) -> None:
        pass


class _FakeContext:
    def __init__(self) -> None:
        self.log = _FakeLog()


def test_load_pending_manifest_entries_quarantines_invalid_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MANIFEST_QUARANTINE_MIN_AGE_SEC", "0")
    pending_manifest = tmp_path / "pending" / "broken_manifest.json"
    pending_manifest.parent.mkdir(parents=True, exist_ok=True)
    pending_manifest.write_text("", encoding="utf-8")

    processed_dir = tmp_path / "processed"
    context = _FakeContext()

    entries = load_pending_manifest_entries(
        [pending_manifest],
        context,
        processed_dir=processed_dir,
    )

    assert entries == []
    assert not pending_manifest.exists()
    moved_files = list(processed_dir.glob("broken_manifest*.invalid*.json"))
    assert len(moved_files) == 1
    assert any("manifest JSON 파싱 실패" in message for message in context.log.warnings)
    assert any("invalid manifest 격리" in message for message in context.log.warnings)


def test_transient_read_error_is_not_quarantined(tmp_path: Path) -> None:
    """OSError 계열(CIFS 지연·권한)은 내용 손상이 아니다 — 옮기면 NAS 장애 때 정상 manifest 를 잃는다."""
    # 디렉토리를 manifest 경로로 두면 read_text 가 IsADirectoryError(OSError) 를 낸다.
    unreadable = tmp_path / "pending" / "transient.json"
    unreadable.mkdir(parents=True, exist_ok=True)

    processed_dir = tmp_path / "processed"
    context = _FakeContext()

    entries = load_pending_manifest_entries([unreadable], context, processed_dir=processed_dir)

    assert entries == []
    assert unreadable.exists(), "일시적 오류인데 원본이 사라졌다"
    assert list(processed_dir.glob("*.invalid*.json")) == []
    assert any("일시적" in m for m in context.log.warnings)
    assert not any("격리" in m for m in context.log.warnings)


def test_non_dict_payload_is_quarantined(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MANIFEST_QUARANTINE_MIN_AGE_SEC", "0")
    """파싱은 되지만 객체가 아닌 manifest 도 손상이다."""
    bad = tmp_path / "pending" / "listy.json"
    bad.parent.mkdir(parents=True, exist_ok=True)
    bad.write_text("[1, 2, 3]", encoding="utf-8")

    processed_dir = tmp_path / "processed"
    context = _FakeContext()

    entries = load_pending_manifest_entries([bad], context, processed_dir=processed_dir)

    assert entries == []
    assert not bad.exists()
    assert len(list(processed_dir.glob("listy*.invalid*.json"))) == 1


def test_in_flight_manifest_is_not_quarantined(tmp_path: Path) -> None:
    """진행 중 run 이 manifest 를 재작성하는 창에서 읽으면 정상 파일도 파싱 실패로 보인다.

    _persist_manifest 원자화로 창을 없앴지만, 외부 드롭 프로세스까지 보장하지는 못하므로
    in-flight 가드를 유지한다. 이 테스트가 그 방어선의 유일한 회귀 감지기다.
    """
    live = tmp_path / "pending" / "live.json"
    live.parent.mkdir(parents=True, exist_ok=True)
    live.write_text("{partial", encoding="utf-8")

    processed_dir = tmp_path / "processed"
    context = _FakeContext()

    entries = load_pending_manifest_entries(
        [live],
        context,
        processed_dir=processed_dir,
        in_flight_manifest_paths={str(live)},
    )

    assert entries == []
    assert live.exists(), "run 진행 중인 manifest 를 격리해버렸다"
    assert list(processed_dir.glob("*.invalid*.json")) == []
    assert any("격리 보류" in m for m in context.log.warnings)


def test_valid_manifest_still_loads(tmp_path: Path) -> None:
    ok = tmp_path / "pending" / "good.json"
    ok.parent.mkdir(parents=True, exist_ok=True)
    ok.write_text('{"source_unit_path": "/nas/data/incoming/unit_a", "manifest_id": "m-1"}', encoding="utf-8")

    processed_dir = tmp_path / "processed"
    context = _FakeContext()

    entries = load_pending_manifest_entries([ok], context, processed_dir=processed_dir)

    assert len(entries) == 1
    assert entries[0]["source_unit_path"] == "/nas/data/incoming/unit_a"
    assert entries[0]["manifest_id"] == "m-1"
    assert ok.exists()


class _FakeRun:
    def __init__(self, tags: dict) -> None:
        self.tags = tags


def test_collect_in_flight_manifest_paths_reads_the_tag_sensor_writes() -> None:
    """태그 키가 sensor_incoming 이 싣는 이름과 어긋나면 오격리 방어가 조용히 꺼진다."""
    context = _FakeContext()
    runs = [
        _FakeRun({"manifest_path": "/nas/data/incoming/.manifests/pending/a.json"}),
        _FakeRun({"manifest_path": "  /nas/data/incoming/.manifests/pending/b.json  "}),
        _FakeRun({"manifest_path": ""}),
        _FakeRun({"source_unit_path": "/nas/data/incoming/unit_x"}),
        _FakeRun({}),
    ]

    paths = collect_in_flight_manifest_paths(context, runs)

    assert paths == {
        "/nas/data/incoming/.manifests/pending/a.json",
        "/nas/data/incoming/.manifests/pending/b.json",
    }


def test_collect_in_flight_manifest_paths_empty_runs() -> None:
    assert collect_in_flight_manifest_paths(_FakeContext(), []) == set()


def test_recently_written_corrupt_manifest_is_not_quarantined(tmp_path: Path) -> None:
    """주 방어선 회귀 감지기.

    pending/ 에 셸 리다이렉트로 스트리밍하는 writer(scripts/bootstrap_manifest.sh)가 있어서,
    쓰는 도중의 부분 JSON 은 진짜 손상과 바이트 단위로 구분되지 않는다. 게다가 쓰는 중인
    파일을 rename 해도 열린 fd 는 inode 를 따라가므로, 유효한 manifest 가 .invalid.json 으로
    격리되고 writer 는 성공했다고 보고한다. 방금 수정된 파일은 격리하면 안 된다.
    """
    fresh = tmp_path / "pending" / "being_written.json"
    fresh.parent.mkdir(parents=True, exist_ok=True)
    fresh.write_text('{"files": [', encoding="utf-8")  # 쓰다 만 상태

    processed_dir = tmp_path / "processed"
    context = _FakeContext()

    # 기본값(600초)에서는 방금 쓴 파일이므로 보류돼야 한다.
    entries = load_pending_manifest_entries([fresh], context, processed_dir=processed_dir)

    assert entries == []
    assert fresh.exists(), "쓰기 중일 수 있는 manifest 를 격리해버렸다"
    assert list(processed_dir.glob("*.invalid*.json")) == []
    assert any("격리 보류" in m for m in context.log.warnings)


def test_unexpected_error_on_one_manifest_does_not_stop_the_rest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """per-file fail-forward — 파일 하나가 센서 tick 전체를 멈추면 안 된다.

    read_manifest_payload 가 좁힌 예외(RecursionError·MemoryError 등)를 흘려도
    나머지 manifest 는 계속 처리돼야 한다.
    """
    good = tmp_path / "pending" / "good.json"
    good.parent.mkdir(parents=True, exist_ok=True)
    good.write_text('{"source_unit_path": "/nas/data/incoming/unit_ok"}', encoding="utf-8")
    boom = tmp_path / "pending" / "boom.json"
    # 빈 payload 분기에서 먼저 빠지지 않도록 내용을 채운다 — stat 호출까지 도달해야 한다.
    boom.write_text('{"source_unit_path": "/nas/data/incoming/unit_boom"}', encoding="utf-8")

    real_stat = Path.stat

    def exploding_stat(self, *a, **kw):
        if self.name == "boom.json":
            raise RecursionError("simulated")
        return real_stat(self, *a, **kw)

    monkeypatch.setattr(Path, "stat", exploding_stat)

    context = _FakeContext()
    entries = load_pending_manifest_entries([boom, good], context, processed_dir=tmp_path / "processed")

    assert [e["source_unit_path"] for e in entries] == ["/nas/data/incoming/unit_ok"]
    assert any("처리 실패(건너뜀)" in m for m in context.log.warnings)
