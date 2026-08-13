"""ThreadLocalProxy: transparent proxy with true thread-local isolation."""

from typing import Any, Callable, TypeVar

from . import _internal

T = TypeVar("T")


class ThreadLocalProxy:
    """Proxy that delegates to thread-local instance.

    Uses original (unpatched) threading.local to ensure true thread isolation,
    even in patched environments where gevent.threadpool uses real threads.

    Type-transparent: ``ThreadLocalProxy(factory)`` is typed as the return type
    of *factory*, so IDE autocompletion and type checking work as expected.
    """

    __slots__ = ("_factory", "_local")

    def __new__(cls, factory: Callable[[], T]) -> T:  # type: ignore[misc]
        return object.__new__(cls)  # type: ignore[return-value]

    def __init__(self, factory: Callable[[], Any]):
        object.__setattr__(self, "_factory", factory)
        object.__setattr__(self, "_local", _internal.Local())

    def _get_instance(self):
        local = self._local
        if not hasattr(local, "instance"):
            # Serialised, not merely re-checked: the factory opens things, which
            # under gevent is a switch point, so two greenlets on this thread
            # would otherwise BOTH build. Publishing one of them is not enough —
            # this class has no close protocol, so the loser's descriptors would
            # stay open for the life of the process. A gevent lock, because a
            # native one is reentrant per thread and these are greenlets on one.
            # The lock itself is created without yielding, so the greenlet that
            # gets here first is the only one that can create it.
            import gevent.lock

            lock = getattr(local, "lock", None)
            if lock is None:
                lock = local.lock = gevent.lock.RLock()
            with lock:
                if not hasattr(local, "instance"):
                    local.instance = self._factory()
        return local.instance

    def __getattr__(self, name):
        return getattr(self._get_instance(), name)

    def __setattr__(self, name, value):
        setattr(self._get_instance(), name, value)

    def __delattr__(self, name):
        delattr(self._get_instance(), name)
