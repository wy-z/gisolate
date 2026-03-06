"""Internal primitives: unpatched stdlib, exceptions, serialization."""

import contextlib
import io
import pickle
from typing import Any

import dill
import gevent.monkey

# ---------------------------------------------------------------------------
# Unpatched stdlib primitives
# ---------------------------------------------------------------------------

current_thread = gevent.monkey.get_original("threading", "current_thread")
RLock = gevent.monkey.get_original("threading", "RLock")
Event = gevent.monkey.get_original("threading", "Event")
Local = gevent.monkey.get_original("threading", "local")
Queue = gevent.monkey.get_original("queue", "Queue")
QueueEmpty = gevent.monkey.get_original("queue", "Empty")

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class ProcessError(RuntimeError):
    """Child process died or communication failed."""


class RemoteError(RuntimeError):
    """Wrapper for exceptions from child process that can't be pickled."""

    def __init__(self, message: str, exc_type: str = ""):
        self.exc_type = exc_type
        super().__init__(message)


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


class SmartPickle:
    """Serializer preferring pickle, falling back to dill. Learns from failures."""

    _PICKLE = b"P"
    _DILL = b"D"
    _dill_types: set[type] = set()
    _BUILTIN_CONTAINERS = frozenset({list, tuple, dict, set, frozenset})

    @classmethod
    def dumps(cls, obj: Any) -> bytes:
        if type(obj) in cls._dill_types:
            return cls._DILL + dill.dumps(obj)
        buf = io.BytesIO()
        buf.write(cls._PICKLE)
        try:
            pickle.dump(obj, buf, protocol=5)
        except (pickle.PicklingError, TypeError, AttributeError):
            t = type(obj)
            if t not in cls._BUILTIN_CONTAINERS:
                cls._dill_types.add(t)
            return cls._DILL + dill.dumps(obj)
        return buf.getvalue()

    @classmethod
    def loads(cls, data: bytes) -> Any:
        mv = memoryview(data)
        tag = mv[:1]
        if tag == cls._PICKLE:
            return pickle.loads(mv[1:])
        return dill.loads(bytes(mv[1:]))


def wrap_exception(e: Exception, tb_str: str | None = None) -> Exception:
    """Ensure exception survives serialization, attach remote traceback."""
    for serializer in (pickle, dill):
        with contextlib.suppress(Exception):
            serializer.dumps(e)
            exc = e
            break
    else:
        exc = RemoteError(f"{type(e).__name__}: {e}", type(e).__name__)
    if tb_str:
        with contextlib.suppress(AttributeError):
            exc.__remote_traceback__ = tb_str  # type: ignore[attr-defined]
    return exc
