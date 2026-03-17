"""Tests for gisolate.hub module."""

import gevent
import pytest
from gisolate.hub import (AsyncResult, ensure_hub_started, run_on_main_hub,
                          spawn_on_main_hub)


class TestAsyncResult:
    def test_set_and_get(self):
        ar = AsyncResult()
        ar.set(42)
        assert ar.get(timeout=1) == 42

    def test_set_exception_and_get(self):
        ar = AsyncResult()
        ar.set_exception(ValueError("boom"))
        with pytest.raises(ValueError, match="boom"):
            ar.get(timeout=1)

    def test_get_timeout(self):
        ar = AsyncResult()
        with pytest.raises(TimeoutError):
            ar.get(timeout=0.01)

    def test_set_none(self):
        ar = AsyncResult()
        ar.set(None)
        assert ar.get(timeout=1) is None


class TestMainHub:
    def test_ensure_hub_started_idempotent(self):
        ensure_hub_started()
        ensure_hub_started()

    def test_run_on_main_hub_executes_func(self):
        """run_on_main_hub runs the function and waits (returns None on success)."""
        side_effect = []
        run_on_main_hub(lambda: side_effect.append(42))
        assert side_effect == [42]

    def test_run_on_main_hub_propagates_exception(self):
        def fail():
            raise RuntimeError("hub error")

        with pytest.raises(RuntimeError, match="hub error"):
            run_on_main_hub(fail)

    def test_spawn_on_main_hub(self):
        results = []

        def append_value(v):
            results.append(v)

        spawn_on_main_hub(append_value, "hello")
        gevent.sleep(0.1)
        assert "hello" in results
