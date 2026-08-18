"""GCS download asset — subprocess 스트리밍 로직 단위 테스트.

Dagster import를 피하기 위해 _stream_to_logger / _terminate_process를
모듈 전체 import 없이 개별 참조한다.
"""

from __future__ import annotations

import importlib
import subprocess
import sys
import threading
import types
from io import StringIO
from unittest.mock import MagicMock

import pytest


def _load_helpers():
    """Dagster 의존 없이 _stream_to_logger, _terminate_process만 추출."""
    src = importlib.util.find_spec("vlm_pipeline.defs.gcp.assets")
    if src is None:
        pytest.skip("vlm_pipeline.defs.gcp.assets not importable")

    import ast
    from pathlib import Path

    source_path = Path(src.origin)
    tree = ast.parse(source_path.read_text())

    module_code = compile(tree, str(source_path), "exec")

    fake_dagster = types.ModuleType("dagster")
    fake_dagster.AssetKey = lambda *a, **k: None
    fake_dagster.Field = lambda *a, **k: None
    fake_dagster.asset = lambda *a, **k: (lambda fn: fn)
    sys.modules.setdefault("dagster", fake_dagster)

    fake_env_utils = types.ModuleType("vlm_pipeline.lib.env_utils")
    fake_env_utils.as_int = lambda v, default=0: int(v) if v is not None else default
    sys.modules.setdefault("vlm_pipeline.lib.env_utils", fake_env_utils)

    ns: dict = {}
    exec(module_code, ns)  # noqa: S102
    return ns["_stream_to_logger"], ns["_terminate_process"]


_stream_to_logger, _terminate_process = _load_helpers()


class TestStreamToLogger:
    def test_lines_forwarded_to_logger(self) -> None:
        stream = StringIO("line1\nline2\nline3\n")
        logged: list[str] = []
        sink: list[str] = []
        lock = threading.Lock()

        _stream_to_logger(stream, logged.append, sink, lock)

        assert logged == ["line1", "line2", "line3"]
        assert len(sink) == 3

    def test_empty_lines_not_logged(self) -> None:
        stream = StringIO("hello\n\nworld\n")
        logged: list[str] = []
        sink: list[str] = []
        lock = threading.Lock()

        _stream_to_logger(stream, logged.append, sink, lock)

        assert logged == ["hello", "world"]
        assert len(sink) == 3

    def test_empty_stream(self) -> None:
        stream = StringIO("")
        logged: list[str] = []
        sink: list[str] = []
        lock = threading.Lock()

        _stream_to_logger(stream, logged.append, sink, lock)

        assert logged == []
        assert sink == []


class TestTerminateProcess:
    def test_already_exited(self) -> None:
        proc = MagicMock(spec=subprocess.Popen)
        proc.poll.return_value = 0

        _terminate_process(proc)

        proc.terminate.assert_not_called()
        proc.kill.assert_not_called()

    def test_graceful_terminate(self) -> None:
        proc = MagicMock(spec=subprocess.Popen)
        proc.poll.return_value = None
        proc.wait.return_value = None

        _terminate_process(proc)

        proc.terminate.assert_called_once()
        proc.kill.assert_not_called()

    def test_force_kill_on_timeout(self) -> None:
        proc = MagicMock(spec=subprocess.Popen)
        proc.poll.return_value = None
        proc.wait.side_effect = [subprocess.TimeoutExpired("cmd", 5), None]

        _terminate_process(proc)

        proc.terminate.assert_called_once()
        proc.kill.assert_called_once()


class TestGcsDownloadStreaming:
    """Popen 기반 스트리밍이 실제 프로세스에서 동작하는지 통합 수준 검증."""

    def test_real_subprocess_streams_to_logger(self) -> None:
        proc = subprocess.Popen(
            ["python3", "-c", "import sys; print('stdout_msg'); print('err_msg', file=sys.stderr)"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        stdout_logged: list[str] = []
        stderr_logged: list[str] = []
        stdout_sink: list[str] = []
        stderr_sink: list[str] = []
        lock = threading.Lock()

        t_out = threading.Thread(
            target=_stream_to_logger,
            args=(proc.stdout, stdout_logged.append, stdout_sink, lock),
            daemon=True,
        )
        t_err = threading.Thread(
            target=_stream_to_logger,
            args=(proc.stderr, stderr_logged.append, stderr_sink, lock),
            daemon=True,
        )
        t_out.start()
        t_err.start()

        returncode = proc.wait(timeout=10)
        t_out.join(timeout=5)
        t_err.join(timeout=5)

        assert returncode == 0
        assert "stdout_msg" in stdout_logged
        assert "err_msg" in stderr_logged

    def test_timeout_terminates_process(self) -> None:
        proc = subprocess.Popen(
            ["python3", "-c", "import time; time.sleep(60)"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        with pytest.raises(subprocess.TimeoutExpired):
            proc.wait(timeout=0.5)

        _terminate_process(proc)
        assert proc.poll() is not None
