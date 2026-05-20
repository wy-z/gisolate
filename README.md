# gisolate

> *Gevent has tormented me a thousand times, yet I keep coming back for more. This library is proof of that love.*

Process isolation for gevent applications. Run any object in a clean subprocess, call its methods transparently via ZMQ IPC.

## Why

gevent's `monkey.patch_all()` replaces stdlib modules globally. Some libraries (database drivers, native async frameworks, etc.) break under monkey-patching. **gisolate** spawns a clean child process — no monkey-patching — and proxies method calls over ZMQ, so incompatible code runs in isolation while your gevent app stays cooperative.

## Install

```bash
pip install gisolate
```

Requires Python 3.12+.

## Quick Start

### ProcessProxy — persistent child process

Proxy method calls to an object living in an isolated subprocess:

```python
import gevent.monkey
gevent.monkey.patch_all()

from gisolate import ProcessProxy

# Define a factory (must be importable / picklable)
def create_client():
    from some_native_lib import Client
    return Client(host="localhost")

# Option 1: inline
proxy = ProcessProxy.create(create_client, timeout=30)
result = proxy.query("SELECT 1")  # runs in child process
proxy.shutdown()

# Option 2: subclass
class ClientProxy(ProcessProxy):
    client_factory = staticmethod(create_client)
    timeout = 30

with ClientProxy() as proxy:
    result = proxy.query("SELECT 1")
```

### run_in_subprocess — one-shot call

Run a single function in a subprocess and get the result:

```python
from gisolate import run_in_subprocess

def heavy_compute(n):
    return sum(range(n))

result = run_in_subprocess(heavy_compute, args=(10_000_000,), timeout=60)
```

### ProcessBridge — cross-process RPC

ZMQ-based RPC bridge for server/client architectures. Server side uses gevent, client side uses asyncio:

```python
from gisolate import ProcessBridge

# Server (gevent side)
server = ProcessBridge("ipc:///tmp/rpc.sock", mode=ProcessBridge.Mode.SERVER)
server.start()

# Client (asyncio side)
import asyncio

async def main():
    client = ProcessBridge("ipc:///tmp/rpc.sock", mode=ProcessBridge.Mode.CLIENT)
    result = await client.call(lambda x, y: x + y, 3, 4, timeout=5)
    print(result)  # 7
    client.close()

asyncio.run(main())
server.close()
```

### ProcessPublisher / ProcessSubscriber — one-way fan-out

ZMQ PUB/SUB for one-way data streaming (snapshots, signals, heartbeats). Use this when message loss is acceptable; use `ProcessBridge` when you need request/response with delivery guarantees.

Both ends take a `runtime=` kwarg (a `PubSubRuntime` enum, also accepts the strings `"gevent"` / `"asyncio"`) selecting the concurrency backend:

| Class | Default runtime | `publish` / `close` |
|-------|-----------------|---------------------|
| `ProcessPublisher` | `PubSubRuntime.GEVENT` | sync in GEVENT, awaitable in ASYNC |
| `ProcessSubscriber` | `PubSubRuntime.ASYNC` | `close` sync in GEVENT, awaitable in ASYNC; handlers must be sync in GEVENT and `async def` in ASYNC |

The wire format is identical across runtimes, so a gevent publisher pairs with an asyncio subscriber (and vice versa) without any adapter.

```python
# Producer (gevent side — default runtime)
from gisolate import ProcessPublisher

pub = ProcessPublisher("ipc:///tmp/stream.sock").start()
pub.publish("v1.snapshot.AAPL", {"price": 150.0})
pub.publish("v1.heartbeat.gevent", {"ts_ns": 1234567890})
pub.close()

# Consumer (asyncio side — default runtime)
import asyncio
from gisolate import ProcessSubscriber

async def main():
    sub = ProcessSubscriber("ipc:///tmp/stream.sock")

    async def on_snapshot(topic, payload):
        print(topic, payload)

    async def on_heartbeat(topic, payload):
        print("heartbeat", payload)

    sub.subscribe("v1.snapshot.", on_snapshot)
    sub.subscribe("v1.heartbeat.", on_heartbeat)
    sub.start()
    await asyncio.sleep(10)
    await sub.close()

asyncio.run(main())
```

Asyncio publisher / gevent subscriber — same wire format, just flip the `runtime=`:

```python
# Producer (asyncio side)
from gisolate import ProcessPublisher, PubSubRuntime

async def producer():
    async with ProcessPublisher(addr, runtime=PubSubRuntime.ASYNC) as pub:
        await pub.publish("v1.tick.AAPL", {"price": 150.0})

# Consumer (gevent side) — handlers are sync
from gisolate import ProcessSubscriber, PubSubRuntime

def on_tick(topic, payload):  # sync, not async def
    print(topic, payload)

with ProcessSubscriber(addr, runtime=PubSubRuntime.GEVENT) as sub:
    sub.subscribe("v1.tick.", on_tick)
    gevent.sleep(10)
```

Notes:
- **Runtime must match the host loop** — `start()` requires a running asyncio loop in ASYNC mode and a greenlet context in GEVENT mode. Subsequent `subscribe` / `unsubscribe` / `publish` / `close` calls must stay on that same loop/hub; ZMQ sockets are not thread-safe.
- **Handler signature follows the subscriber's runtime, not the publisher's** — a gevent subscriber consuming from an asyncio publisher still uses sync handlers.
- **Context managers** — `with` for GEVENT, `async with` for ASYNC; using the wrong form raises `RuntimeError`. `start()` and `close()` are idempotent.
- **Topic prefix matching** — `sub.subscribe("v1.snapshot.", h)` receives every topic starting with that prefix. Multiple handlers may share a prefix; in ASYNC mode they run via `asyncio.gather`, in GEVENT mode each is spawned in its own greenlet. An exception in one handler is logged and does not kill the reader.
- **`close()` from inside a handler is safe** — the reader is not joined in that case (would self-deadlock); sibling handlers in the current dispatch are allowed to finish.
- **Lossy by design** — `publish` is non-blocking; messages are dropped when the send queue is full (slow subscriber). Tune via `sndhwm=` on the publisher.
- **Late joiners miss history** — PUB/SUB has no replay; a subscriber that connects after a message was published will not see it. Treat published state as a stream, not a store.
- **IPC cleanup** — `close()` unlinks the socket file for `ipc://` addresses on the publisher side. Relying on `__del__` is best-effort only; call `close()` (or use a context manager) for deterministic teardown.
- **Pluggable serializer** — defaults to `SmartPickle` (pickle, falling back to dill). Pass any object implementing the `Serializer` protocol (`dumps`/`loads`) to use msgpack, JSON, etc. Publisher and subscriber must agree.

### ThreadLocalProxy — per-thread instances

Thread-local proxy using unpatched `threading.local` for true isolation in `gevent.threadpool`:

```python
from gisolate import ThreadLocalProxy

proxy = ThreadLocalProxy(create_client)
proxy.query("SELECT 1")  # each real OS thread gets its own instance
```

## Child Process Modes

| `patch_kwargs`  | Child process runtime |
|-----------------|----------------------|
| `None` (default) | asyncio event loop   |
| `dict`          | gevent with `patch_all(**patch_kwargs)` |

```python
# Child uses asyncio (default)
proxy = ProcessProxy.create(factory)

# Child uses gevent with selective patching
proxy = ProcessProxy.create(factory, patch_kwargs={"thread": False, "os": False})
```

## API Reference

### `ProcessProxy`

- **`ProcessProxy.create(factory, *, timeout=24, mp_context=None, patch_kwargs=None)`** — create a proxy without subclassing
- **`proxy.<method>(*args, **kwargs)`** — transparently call any method on the remote object
- **`proxy.restart_process()`** — kill and restart child process
- **`proxy.shutdown()`** — gracefully stop child process
- Supports context manager (`with` statement)
- Thread-safe: usable from greenlets and native threads

### `run_in_subprocess(target, args=(), kwargs=None, *, timeout=3600, mp_context=None)`

Run a function in an isolated subprocess. Blocks with gevent-safe polling.

### `ProcessBridge(address, mode)`

- **`bridge.start()`** — start the bridge (idempotent, returns self)
- **`bridge.address`** — IPC address
- **`await bridge.call(func, *args, timeout=60, **kwargs)`** — async RPC call (client mode)
- **`bridge.close()`** — cleanup resources

### `ProcessPublisher(address, *, runtime=PubSubRuntime.GEVENT, serializer=SmartPickle, sndhwm=1000)`

- **`pub.start()`** — bind the PUB socket (idempotent, returns self). In ASYNC mode requires a running asyncio loop.
- **`pub.publish(topic, payload)`** — non-blocking publish; drops on slow consumers. Returns `None` in GEVENT mode, a coroutine in ASYNC mode (must `await`).
- **`pub.close()`** — cleanup (idempotent). Returns `None` in GEVENT mode, a coroutine in ASYNC mode.
- **`pub.address`** / **`pub.runtime`** — read-only properties.
- Context manager: `with` for GEVENT, `async with` for ASYNC. Using the wrong form raises `RuntimeError`.

### `ProcessSubscriber(address, *, runtime=PubSubRuntime.ASYNC, serializer=SmartPickle)`

- **`sub.subscribe(topic_prefix, handler)`** — register a handler for a topic prefix. Handler must be sync (`def`) in GEVENT mode and `async def` (or returning an awaitable) in ASYNC mode. Safe to call before or after `start()`.
- **`sub.unsubscribe(topic_prefix, handler=None)`** — remove a specific handler or all handlers for a prefix. When the last handler is removed, the ZMQ-level subscription is dropped.
- **`sub.start()`** — connect and spawn the reader (idempotent, returns self). In ASYNC mode requires a running asyncio loop; in GEVENT mode must be called from a greenlet context.
- **`sub.close()`** — tear down the socket and join the reader (idempotent). Returns `None` in GEVENT mode, a coroutine in ASYNC mode. Safe to call from inside a handler — the reader is not joined in that case to avoid self-deadlock.
- **`sub.address`** / **`sub.runtime`** — read-only properties.
- Context manager: `with` for GEVENT, `async with` for ASYNC.

### `PubSubRuntime` (StrEnum)

- **`PubSubRuntime.GEVENT`** (`"gevent"`) — bind to the gevent hub; sync APIs and sync handlers.
- **`PubSubRuntime.ASYNC`** (`"asyncio"`) — bind to the running asyncio loop; awaitable APIs and async handlers.

### `Serializer` (Protocol)

Anything with `dumps(obj) -> bytes` and `loads(bytes) -> obj` static methods can be used as a serializer for `ProcessPublisher` / `ProcessSubscriber`. Default is `SmartPickle` (pickle, falling back to dill). Publisher and subscriber must agree on the serializer.

### `ThreadLocalProxy(factory)`

Transparent proxy delegating attribute access to a per-thread instance.

### `ensure_hub_started()`

Pre-start the internal gevent hub loop on demand. Idempotent and thread-safe. Called automatically by `ProcessProxy`, but can be invoked explicitly to control initialization timing.

### `spawn_on_main_hub(func, *args, **kwargs)`

Schedule a function on the main gevent hub without waiting. Thread-safe, fire-and-forget.

### `ProcessError`

Raised when a child process dies or communication fails.

### `RemoteError`

Wrapper for exceptions from the child process that can't be pickled. Preserves the original exception type name and message.

### `shutdown_hub()`

Explicitly stop the internal gevent hub loop. Registered via `atexit` automatically.

### `set_default_mp_context(ctx)` / `get_default_mp_context()`

Configure the default `multiprocessing` context for all proxies (default: `"spawn"`).

## Note on `multiprocessing` and `__main__`

`multiprocessing` spawn/forkserver children re-import the caller's `__main__` module. If your `main.py` has top-level side effects (e.g. `gevent.monkey.patch_all()`), these will re-execute in the child — causing double-patching warnings or import errors.

**Best practice**: guard monkey-patching behind `__name__` and defer heavy imports:

```python
# main.py
if __name__ == "__main__":
    import gevent.monkey
    gevent.monkey.patch_all()

    import my_app
    my_app.run()
```

Spawn children re-import `main.py` but skip the `__name__` block, avoiding side effects.

## License

MIT
