"""Tests for gisolate.subprocess module."""

import os

import pytest

from gisolate.subprocess import run_in_subprocess

from .helpers import add, get_pid, greet, noop, raise_value_error, slow_func


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

