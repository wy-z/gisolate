"""ThreadLocalProxy: transparent proxy with true thread-local isolation."""

from typing import Any, Callable

from . import _internal


class ThreadLocalProxy:
    """Proxy that delegates to thread-local instance.

    Uses original (unpatched) threading.local to ensure true thread isolation,
    even in patched environments where gevent.threadpool uses real threads.
    """

    __slots__ = ("_factory", "_local")

    def __init__(self, factory: Callable[[], Any]):
        object.__setattr__(self, "_factory", factory)
        object.__setattr__(self, "_local", _internal.Local())

    def _get_instance(self):
        local = self._local
        if not hasattr(local, "instance"):
            local.instance = self._factory()
        return local.instance

    def __getattr__(self, name):
        return getattr(self._get_instance(), name)

    def __setattr__(self, name, value):
        setattr(self._get_instance(), name, value)

    def __delattr__(self, name):
        delattr(self._get_instance(), name)
