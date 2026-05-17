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

```python
# Producer (gevent side)
from gisolate import ProcessPublisher

pub = ProcessPublisher("ipc:///tmp/stream.sock").start()
pub.publish("v1.snapshot.AAPL", {"price": 150.0})
pub.publish("v1.heartbeat.gevent", {"ts_ns": 1234567890})
pub.close()

# Consumer (asyncio side)
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

Notes:
- **Topic prefix matching** — `sub.subscribe("v1.snapshot.", h)` receives every topic starting with that prefix.
- **Multiple handlers per prefix** — invoked concurrently with `asyncio.gather`. Exceptions in one handler do not kill the reader task.
- **Lossy by design** — `publish` is non-blocking; messages are dropped when the send queue is full (slow subscriber). Set `sndhwm=` to tune.
- **Pluggable serializer** — defaults to `SmartPickle`. Pass any object implementing the `Serializer` protocol (`dumps`/`loads`) to use msgpack, JSON, etc.

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

### `ProcessPublisher(address, *, serializer=SmartPickle, sndhwm=1000)`

- **`pub.start()`** — bind the PUB socket (idempotent, returns self)
- **`pub.publish(topic, payload)`** — non-blocking publish; drops on slow consumers
- **`pub.close()`** — cleanup (idempotent)
- Supports context manager (`with` statement)

### `ProcessSubscriber(address, *, serializer=SmartPickle)`

- **`sub.subscribe(topic_prefix, handler)`** — register an async handler for a topic prefix
- **`sub.unsubscribe(topic_prefix, handler=None)`** — remove a handler or all handlers for a prefix
- **`sub.start()`** — connect and spawn the reader task (idempotent, returns self)
- **`await sub.close()`** — cancel reader and cleanup (idempotent)
- Supports async context manager (`async with` statement)

### `Serializer` (Protocol)

Anything with `dumps(obj) -> bytes` and `loads(bytes) -> obj` static methods can be used as a serializer for `ProcessPublisher` / `ProcessSubscriber`. Default is `SmartPickle` (pickle, falling back to dill).

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
