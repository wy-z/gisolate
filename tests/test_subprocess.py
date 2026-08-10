"""Tests for gisolate.subprocess module."""

import multiprocessing.process
import os
import subprocess
import sys
import textwrap

import pytest

from gisolate.subprocess import run_in_subprocess

from .helpers import add, get_pid, greet, noop, raise_value_error, slow_func, suicide


class TestRunInSubprocess:
    def test_basic_return(self):
        assert run_in_subprocess(add, args=(3, 4)) == 7

    def test_runs_in_different_process(self):
        child_pid = run_in_subprocess(get_pid)
        assert child_pid != os.getpid()

    def test_propagates_exception(self):
        with pytest.raises(ValueError, match="subprocess boom"):
            run_in_subprocess(raise_value_error)

    def test_timeout(self):
        with pytest.raises(TimeoutError):
            run_in_subprocess(slow_func, timeout=0.5)

    def test_kwargs(self):
        result = run_in_subprocess(greet, args=("world",), kwargs={"greeting": "hi"})
        assert result == "hi world"

    def test_returns_none(self):
        assert run_in_subprocess(noop) is None

    def test_crashed_child_reported_when_is_alive_lies(self, monkeypatch):
        """A gevent parent's libev loop can steal the reap; multiprocessing's
        waitpid then gets ECHILD and is_alive() calls a dead child running
        forever. Pinned True here: a crashed target used to burn the whole
        timeout (1h default) and surface as TimeoutError."""
        monkeypatch.setattr(
            multiprocessing.process.BaseProcess, "is_alive", lambda self: True
        )
        with pytest.raises(RuntimeError, match="exited with code"):
            run_in_subprocess(suicide, timeout=30, poll_interval=0.05)

    def test_make_pipe_forces_blocking_with_gevent_patch(self):
        script = textwrap.dedent(
            """
            import multiprocessing
            import os

            from gevent import monkey

            monkey.patch_all()

            from gisolate.subprocess import _make_pipe

            parent_conn, child_conn = _make_pipe(multiprocessing.get_context("spawn"))
            for conn in (parent_conn, child_conn):
                print(os.get_blocking(conn.fileno()))
                conn.close()
            """
        )

        proc = subprocess.run(
            [sys.executable, "-c", script],
            check=True,
            capture_output=True,
            text=True,
        )

        assert proc.stdout.strip().splitlines() == ["True", "True"]
