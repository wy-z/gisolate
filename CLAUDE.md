# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is gisolate?

Process isolation library for gevent applications. Runs objects in clean subprocesses and proxies method calls transparently via ZMQ IPC. Primary use case: isolating libraries incompatible with gevent monkey-patching.

## Commands

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest

# Run a single test
uv run pytest tests/test_foo.py::test_name -x

# Lint
uv run ruff check src/
uv run isort --check src/

# Type check
uv run pyright

# Dead code detection
uv run vulture src/
```

## Architecture

All source lives in `src/gisolate/`. Key modules:

- **`proxy.py`** — `ProcessProxy`: the main abstraction. Subclass it, implement `client_factory()`, and method calls are transparently forwarded to an isolated child process via ZMQ DEALER/ROUTER sockets. Supports both gevent and asyncio child workers. Use `ProcessProxy.create()` for quick one-off proxies without subclassing.
- **`_workers.py`** — Child process entry points: `gevent_worker` (with monkey-patching) and `asyncio_worker`. Selected based on whether `patch_kwargs` is set on the proxy class.
- **`bridge.py`** — `ProcessBridge`: lower-level ZMQ RPC bridge for cross-process function calls (server=gevent ROUTER, client=asyncio DEALER). Unlike ProcessProxy, this sends arbitrary callables rather than method names.
- **`subprocess.py`** — `run_in_subprocess()`: simple one-shot function execution in a subprocess with gevent-safe polling via `multiprocessing.Pipe`.
- **`local.py`** — `ThreadLocalProxy`: proxy with true thread-local isolation using unpatched `threading.local`.
- **`hub.py`** — Marshals tasks to gevent's main event loop. Enables thread-safe operations from non-main threads via `run_on_main_hub()` / `spawn_on_main_hub()`.
- **`_internal.py`** — Unpatched stdlib primitives (`threading.Event`, `queue.Queue`, etc. via `gevent.monkey.get_original`), custom exceptions (`ProcessError`, `RemoteError`), and `SmartPickle` (pickle-first, dill-fallback serializer).

## Key Design Patterns

- **Unpatched primitives**: All cross-thread synchronization uses `gevent.monkey.get_original()` to get real threading primitives, not gevent-patched ones. This is critical for correctness.
- **Main hub marshaling**: Operations that must run on the main gevent loop (start/stop/restart) are automatically marshaled via `_hub.run_on_main_hub()` when called from non-main threads.
- **ZMQ protocol**: Request/response uses multipart messages with `_OK`/`_ERR`/`_SHUTDOWN` byte markers. Serialization via `SmartPickle` (tries pickle first, falls back to dill, remembers which types need dill).
- **Child worker selection**: `patch_kwargs=None` → asyncio worker; `patch_kwargs=dict(...)` → gevent worker with those monkey-patch settings.
