"""Tests for gisolate._internal module."""

import pickle

import dill
import pytest

from gisolate._internal import (
    Event,
    Local,
    ProcessError,
    Queue,
    QueueEmpty,
    RLock,
    RemoteError,
    SmartPickle,
    current_thread,
    wrap_exception,
)


# ---------------------------------------------------------------------------
# Unpatched primitives
# ---------------------------------------------------------------------------


class TestUnpatchedPrimitives:
    def test_event_set_and_wait(self):
        e = Event()
        assert not e.is_set()
        e.set()
        assert e.wait(timeout=0.1)

    def test_rlock_acquire_release(self):
        lock = RLock()
        assert lock.acquire(timeout=0.1)
        lock.release()

    def test_rlock_reentrant(self):
        lock = RLock()
        lock.acquire()
        assert lock.acquire(timeout=0.1)
        lock.release()
        lock.release()

    def test_local_isolation(self):
        local = Local()
        local.x = 42
        assert local.x == 42

    def test_queue_put_get(self):
        q = Queue()
        q.put("hello")
        assert q.get_nowait() == "hello"

    def test_queue_empty_raises(self):
        q = Queue()
        with pytest.raises(QueueEmpty):
            q.get_nowait()

    def test_current_thread_returns_something(self):
        t = current_thread()
        assert t is not None


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class TestExceptions:
    def test_process_error_is_runtime_error(self):
        assert issubclass(ProcessError, RuntimeError)

    def test_remote_error_message_and_type(self):
        err = RemoteError("something broke", "ValueError")
        assert str(err) == "something broke"
        assert err.exc_type == "ValueError"

    def test_remote_error_default_exc_type(self):
        err = RemoteError("msg")
        assert err.exc_type == ""


# ---------------------------------------------------------------------------
# SmartPickle
# ---------------------------------------------------------------------------


class TestSmartPickle:
    def test_roundtrip_simple_types(self):
        for obj in [42, "hello", [1, 2, 3], {"a": 1}, (1,), {1, 2}]:
            assert SmartPickle.loads(SmartPickle.dumps(obj)) == obj

    def test_roundtrip_none(self):
        assert SmartPickle.loads(SmartPickle.dumps(None)) is None

    def test_uses_pickle_prefix_for_simple(self):
        data = SmartPickle.dumps(42)
        assert data[:1] == b"P"

    def test_roundtrip_lambda_uses_dill(self):
        fn = lambda x: x + 1  # noqa: E731
        data = SmartPickle.dumps(fn)
        assert data[:1] == b"D"
        restored = SmartPickle.loads(data)
        assert restored(10) == 11

    def test_remembers_dill_types(self):
        """After first dill fallback, same type goes directly to dill."""
        # Lambda type is not picklable by stdlib, but dill handles it.
        # After first attempt, SmartPickle should remember to use dill.
        fn = lambda: 99  # noqa: E731
        SmartPickle._dill_types.discard(type(fn))
        SmartPickle.dumps(fn)
        assert type(fn) in SmartPickle._dill_types

    def test_builtin_containers_not_cached(self):
        """Builtin containers should not be added to _dill_types even on fallback."""
        before = SmartPickle._dill_types.copy()
        # A list containing a lambda forces dill, but list type shouldn't be cached
        SmartPickle.dumps([lambda: None])
        assert list not in SmartPickle._dill_types
        SmartPickle._dill_types = before


# ---------------------------------------------------------------------------
# wrap_exception
# ---------------------------------------------------------------------------


class TestWrapException:
    def test_picklable_exception_passes_through(self):
        original = ValueError("test error")
        wrapped = wrap_exception(original)
        assert wrapped is original

    def test_unpicklable_becomes_remote_error(self):
        class WeirdError(Exception):
            def __reduce__(self):
                raise TypeError("can't pickle")

        original = WeirdError("oops")
        wrapped = wrap_exception(original)
        assert isinstance(wrapped, RemoteError)
        assert "WeirdError" in str(wrapped)
        assert wrapped.exc_type == "WeirdError"

    def test_traceback_attached(self):
        err = ValueError("boom")
        wrapped = wrap_exception(err, tb_str="Traceback line 1\nline 2")
        assert getattr(wrapped, "__remote_traceback__") == "Traceback line 1\nline 2"

    def test_no_traceback_when_none(self):
        err = ValueError("boom")
        wrapped = wrap_exception(err)
        assert not hasattr(wrapped, "__remote_traceback__")

    def test_dill_fallback_for_pickle_failure(self):
        """If pickle fails but dill succeeds, exception still passes through."""
        # Standard exceptions with non-standard args fail pickle but dill handles them
        original = ValueError(lambda: 42)
        wrapped = wrap_exception(original)
        assert wrapped is original
