"""gisolate: Process isolation for gevent applications.

Run any object in a clean subprocess, call methods transparently via ZMQ IPC.
Isolates libraries incompatible with gevent monkey-patching.
"""

from ._internal import ProcessError, RemoteError
from .bridge import ProcessBridge
from .hub import ensure_hub_started, shutdown as shutdown_hub, spawn_on_main_hub
from .local import ThreadLocalProxy
from .proxy import ProcessProxy, get_default_mp_context, set_default_mp_context
from .subprocess import run_in_subprocess

# Pre-initialize threadpoolctl on main thread to cache library info.
# Avoids subprocess calls when sklearn runs in threadpool workers.
try:
    import threadpoolctl  # type: ignore[import-untyped]

    threadpoolctl.ThreadpoolController()
except Exception:
    pass

__all__ = [
    "ProcessBridge",
    "ProcessError",
    "ProcessProxy",
    "RemoteError",
    "ThreadLocalProxy",
    "ensure_hub_started",
    "get_default_mp_context",
    "run_in_subprocess",
    "set_default_mp_context",
    "shutdown_hub",
    "spawn_on_main_hub",
]
