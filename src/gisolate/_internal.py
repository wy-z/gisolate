"""Internal primitives: unpatched stdlib, exceptions, serialization."""

import contextlib
import io
import logging
import pickle
from typing import Any

import dill
import gevent.monkey

log = logging.getLogger(__name__)

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


@contextlib.contextmanager
def suppress_main_reimport():
    """Prevent child process from reimporting the caller's __main__ module.

    multiprocessing's spawn/forkserver contexts reimport __main__ in child
    processes via ``get_preparation_data``. Since gisolate workers live in
    their own modules, this reimport is unnecessary and causes errors
    (duplicate patches, side effects from re-executing main-module code).
    """
    import multiprocessing.spawn as mp_spawn

    orig = getattr(mp_spawn, "get_preparation_data", None)
    if orig is None:
        log.warning(
            "multiprocessing.spawn.get_preparation_data not found, "
            "cannot suppress __main__ reimport"
        )
        yield
        return

    def _stripped(name):
        d = orig(name)
        d.pop("init_main_from_path", None)
        d.pop("init_main_from_name", None)
        return d

    mp_spawn.get_preparation_data = _stripped  # type: ignore[assignment]
    try:
        yield
    finally:
        mp_spawn.get_preparation_data = orig  # type: ignore[assignment]


def wrap_exception(e: Exception, tb_str: str | None = None) -> Exception:
    """Ensure exception survives serialization round-trip, attach remote traceback."""
    try:
        SmartPickle.loads(SmartPickle.dumps(e))
        exc = e
    except Exception:
        exc = RemoteError(f"{type(e).__name__}: {e}", type(e).__name__)
    if tb_str:
        with contextlib.suppress(AttributeError):
            exc.__remote_traceback__ = tb_str  # type: ignore[attr-defined]
    return exc
