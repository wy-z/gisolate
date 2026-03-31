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


# ---------------------------------------------------------------------------
# ZMQ helpers
# ---------------------------------------------------------------------------


def patch_zmq_green_poller():
    """Fix zmq.green._Poller.poll() busy-spin bug.  Idempotent.

    The bug: _Poller.poll(timeout) loops calling super().poll(0) then
    gevent.select.select() on the ZMQ FD.  When the FD is stuck readable
    (e.g. dead peer cleanup) the hub short-circuits select in userspace
    (no syscall) while poll(0) finds no user events, producing a
    ~300µs/iteration pure-CPU spin visible as endless
    ``poll(fd, POLLIN, 0) = 0`` in strace with zero select/epoll calls.

    The fix: call getsockopt(EVENTS) on each registered zmq socket before
    select.select().  This clears FD signaling at the kernel level so
    select truly blocks.
    """
    import gevent
    from gevent import select

    import zmq
    from zmq import Poller as _original_Poller
    from zmq.green.poll import _Poller

    if getattr(_Poller.poll, "_patched", False):
        return

    def poll(self, timeout=-1):
        if timeout is None:
            timeout = -1
        if timeout < 0:
            timeout = -1

        if timeout > 0:
            tout = gevent.Timeout.start_new(timeout / 1000.0)
        else:
            tout = None

        try:
            rlist, wlist, xlist = self._get_descriptors()
            while True:
                events = _original_Poller.poll(self, 0)
                if events or timeout == 0:
                    return events

                # Clear FD signaling so select.select() truly blocks.
                for s, _ in self.sockets:
                    if isinstance(s, zmq.Socket):
                        s.getsockopt(zmq.EVENTS)

                # Re-check: a real event may have arrived between
                # the first poll(0) and the EVENTS drain above.
                events = _original_Poller.poll(self, 0)
                if events:
                    return events

                _bug_timeout = gevent.Timeout.start_new(
                    self._gevent_bug_timeout
                )
                try:
                    select.select(rlist, wlist, xlist)
                except gevent.Timeout as t:
                    if t is not _bug_timeout:
                        raise
                finally:
                    _bug_timeout.cancel()

        except gevent.Timeout as t:
            if t is not tout:
                raise
            return []
        finally:
            if timeout > 0:
                tout.cancel()  # type: ignore[union-attr]

    poll._patched = True  # type: ignore[attr-defined]
    _Poller.poll = poll  # type: ignore[method-assign]


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
