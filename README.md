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

### serve / attach — one worker, many client processes

A `ProcessProxy` spawns a worker at a random address it never advertises, so N
client processes pay for N copies of the isolated library. Where that matters (several gunicorn workers in
one container, say), one process hosts the worker and the rest attach to it:

```python
# Host process — becomes the worker; blocks.
import os
from gisolate import serve

os.makedirs("/run/myapp", exist_ok=True)
os.chmod("/run/myapp", 0o700)   # mode= on makedirs only applies when it creates
serve(create_client, "ipc:///run/myapp/client.sock")
```

```python
# Every client process — no spawn, no factory.
from gisolate import ProcessProxy

proxy = ProcessProxy.attach("ipc:///run/myapp/client.sock", timeout=10)
result = proxy.query("SELECT 1")   # runs in the host
proxy.shutdown()                   # closes this client only; the host keeps serving
```

**Put the socket in a directory you control.** The wire is unauthenticated
pickle and an empty frame stops the worker, so anyone who can connect can run
code in the host or shut it down for every client. The socket file's own mode
follows the host's umask — a world-writable one in a shared directory like
`/tmp` is reachable by any local user — whereas a `0700` directory is not,
whatever the umask. `ipc://` only, and same host only: a call carries the
caller's monotonic deadline, which means nothing on another machine's clock.

Attaching is asynchronous: it connects to an address nobody has bound yet just as
happily, a restarted host is picked up by the DEALER's own reconnect, and an
absent one surfaces as an RPC timeout. The proxy is process-local like any ZMQ
socket — a forking server attaches after the fork, once per worker. `serve()`'s
`max_concurrency` bounds the worker, so it is shared by every client.

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

### AsyncioThread — asyncio on a native thread

One asyncio loop on one real OS thread inside the gevent process — for a library that only speaks asyncio (an ASGI app, an async SDK's protocol engine) when a child process is too much. The loop is built on the unpatched `poll()` and `socketpair()`, so nothing in that thread ever waits on a gevent hub; application and network I/O stay on the gevent side. That is a boundary, not a convenience: an asyncio socket, or the `getaddrinfo` behind one, needs the loop's executor, which is refused (below) — hand the I/O to `to_gevent`.

```python
from gisolate import AsyncioThread

aio = AsyncioThread().start()          # once per process, after any fork

async def handle(payload):
    # ... asyncio-only code ...
    rows = await aio.to_gevent(query_db, payload)   # hop back: runs in a greenlet on the gevent thread
    return rows

result = aio.call(handle(payload), timeout=30)      # from any greenlet: blocks only that greenlet
aio.stop()
```

Both crossings are bounded and cancellable from either side: a timeout, a `gevent.Timeout`, or a killed greenlet cancels the coroutine — and `call()` returns only once the loop has unwound it (a 6s grace bounds a coroutine that ignores cancellation), so the kill it queued for any greenlet it was awaiting has already landed on the caller's hub; a cancelled coroutine kills the greenlet it is awaiting. A loop that dies fails every in-flight `call()` with `LoopStopped`. `run_in_executor` / `asyncio.to_thread` are refused inside the loop while `threading` is monkey-patched — their worker would be a greenlet on the loop's own thread, which cannot run while the loop blocks in `poll()`; use `to_gevent` instead.

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
- **`ProcessProxy.attach(address, *, timeout=24)`** — proxy a worker hosted by `serve()`; never spawns, and `shutdown()` leaves the worker running
- **`proxy.restart_process()`** — kill and restart the child process; on an attached proxy there is no process to kill, so it rebuilds the local transport instead
- **`proxy.shutdown()`** — gracefully stop child process
- Supports context manager (`with` statement)
- Thread-safe: usable from greenlets and native threads

### `serve(factory, address, *, patch_kwargs=None, max_concurrency=None)`

Turn this process into the worker, bound to *address*, for clients that
`ProcessProxy.attach()`. Blocks until interrupted or terminated.

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

### `AsyncioThread(*, start_timeout=10.0)`

POSIX only (a native `socketpair` for the loop's wake-up pipe).

- **`.start()`** / **`.stop(timeout=10.0)`** — start the loop thread (also a context manager). `stop()`'s contract, and all of it: every greenlet a `to_gevent` awaits is killed, including ones created during the teardown, and every other task is cancelled; a call whose coroutine honours the cancellation raises `LoopStopped`, one already handling its own cancellation keeps its cleanup and its own answer (unless that cleanup awaits a `to_gevent`, whose kill ends the call with `LoopStopped`), one that finishes regardless gets its answer; the whole teardown is bounded by a grace (6s), after which a task, async generator or executor thread that has not yielded is abandoned and the loop closed under it. Nothing beyond that is promised for a coroutine that resists its cancellation
- **`.call(coro, timeout=None)`** — run a coroutine on the loop from a greenlet; raises `WaitTimeout` on timeout and cancels the coroutine whenever the caller leaves early
- **`await .to_gevent(fn, *args, **kwargs)`** — from the loop: run `fn` in a fresh greenlet on the thread that called in (the starting thread for tasks nobody called in); a killed greenlet raises `GreenletExit` here, a cancelled await kills the greenlet

### `LoopStopped` / `WaitTimeout`

`LoopStopped`: raised by `call()` when the loop is not running, or stopped before the call completed. `WaitTimeout`: `call()`'s timeout, exported here as well as from `gisolate.hub`.

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

## Known limits

These are decisions, not oversights — each one is documented at the site that
lives with it, and each was reached by measuring the alternative.

**A synchronous client method cannot be stopped (asyncio worker).** Sync methods,
and a sync `factory()` or `connect()`, run on the default executor. Expiring a
call's deadline cancels only the awaiting task: the thread runs to completion, so
its side effects can land after the caller was told it timed out. Bounding it
would need an executor of our own to join, which every correct client would pay
for. `max_concurrency` bounds awaiting handlers; a client whose sync methods must
not overlap needs its own lock. The gevent worker has no such gap — `kill()`
lands inside client code at its next switch.

**A handler's own cleanup can run after the client is closed.** The shutdown
cancels outstanding asyncio handlers *before* waiting out a pending client build:
a handler parked on that build has its wake registered ahead of the teardown's,
so left uncancelled it resumes first when the build completes and `off_loop`
submits the very call the shutdown exists to prevent (measured, in an unpatched
child). A cancelled handler's `finally` may still touch an object that is already
closed — giving it a step first is the worse trade. The gevent worker's mirror is
a handler whose lock wake outruns the kill batch's `GreenletExit` (both are hub
callbacks, FIFO): a `stopping` fence set between the drain giving up and the kill
is re-checked at admission's last yield-free instant, so that wake refuses
instead of starting a call after the grace.

**`serve()` has no hard shutdown bound (asyncio worker).** After the six-second
drain, `asyncio.run` cancels what is left and waits for it with no timeout of its
own, so a coroutine that swallows its cancellation holds the host open. A spawned
`ProcessProxy` worker has the bound its parent's terminate/kill gives it; an
in-process host does not. The teardown's own join of a pending client build is
bounded at six seconds, after which that client is lost rather than closed.

**Two microsecond windows on an `ipc://` socket file.** A replacement that binds
between our bind and the claim's `stat` is recorded as ours; one that binds
between release's `stat` and its `unlink` is removed by us. libzmq exposes no
descriptor for the listening socket, so there is nothing to `fstat` instead of
the path, and closing either window needs a lock protocol an address cannot
carry. What the claim does buy is the difference between "wrong whenever anyone
rebinds" and "wrong only inside that window".

**A revival is not bounded by the call that triggered it.** When `execute()`
finds the worker dead it rebuilds it, and the spawn takes what it takes — the
call's deadline bounds the marshal, which is a queue we do not control, but not
the shared work of starting a child. `restart_cooldown` bounds how often an owner
pays for it.

**`ProcessBridge` belongs to one thread.** `start()` and `close()` carry no
synchronization, and the ZMQ socket, the serving greenlet and the reader task are
all single-thread constructs. Concurrent `call()` from many coroutines on the
owning thread is supported and is what the send lock is for; use `ProcessProxy`
when calls must come from several threads.

**A custom multiprocessing launcher can strand a child if `Process.start()`
raises after creating it.** `start()` is not atomic: CPython's POSIX spawn
creates the OS child, records its pid and sentinel, and only then writes the
bootstrap payload. gisolate recovers and reaps that case for the standard POSIX
`spawn` and `fork` launchers, which record the pid before anything fallible.
`forkserver` is not covered: it reads the pid last, after the request that
already created the child, so a failure before that leaves a child gisolate can
neither name nor signal. A custom `mp_context` whose `Process`/`Popen` creates a child and
then raises without exposing its handle may leave that child running, or a
zombie, until the parent exits. gisolate does not call `waitpid(-1)`, because
that would consume the exit status of children owned by the host application.

**Build a `ProcessProxy` on the thread whose hub you want it on — in practice the
main one.** The owner is the thread that constructed it, and its socket, reader
and lifecycle calls belong to that thread's hub; but the marshal other threads
use to reach it targets the MAIN hub. Construct one on a native thread and those
two disagree, which for a ZMQ socket is undefined behaviour rather than a slow
path. Calling and restarting FROM other threads is supported and is what the
marshal is for.

**A `ProcessProxy` that is dropped rather than shut down is never collected.**
Its reader greenlet holds the proxy, and the hub holds the reader, so nothing
finalizes: the child, the socket and its file all stay until the process exits.
`shutdown()` — or the context manager — is the lifecycle; `__del__` is a
best-effort backstop for the cases that do reach it, such as a start that failed
after publishing. Breaking the cycle would mean a weak reference re-resolved on
every iteration of the reader, paid by every correct user.

**A subscriber handler that never returns outlives its `close()`.** The reader
stops with its generation, but a handler already running is not killed — it holds
the callback it was given, and a close/start cycle can leave one abandoned batch
per generation. The same cooperative limit as the executor thread above: nothing
can stop code that will not stop itself.

**A subscriber handler that never returns also stops the stream.** Each batch of
handlers is waited for before the next receive, on both runtimes, because the
alternative was measured and worse: draining ahead of the handlers defeats SUB's
receive queue and PUB's high-water mark — the two things that answer a handler
slower than the stream — and turns a slow handler into unbounded live greenlets,
each holding its payload. A handler that never returns therefore wedges its
subscriber; a handler that is merely slow gets backpressure, which is the design.

**An async `factory()` or `connect()` that never finishes poisons its worker.**
The one client build is never cancelled and never retried while in flight: a
caller's expiring deadline abandons only its own wait. Retrying instead was the
original bug — two clients built for one worker, and a close racing a connect
that cancellation cannot reach (the executor thread above) — and each retry of a
hanging build stacks another unstoppable thread. Every call times out against
the same build; restarting the worker is the recovery, as it is for a hung sync
build.

**Reaping a stranded child is not bounded.** In the one launch path with a pid
but no sentinel, gisolate kills the child and waits for it, and that wait has no
timeout — a child SIGKILLed while in uninterruptible kernel I/O holds up the
start that found it. There is no partial answer here: leaving early makes it a
zombie nothing owns.

**`ipc://@name` leaks its socket file off Linux.** On Linux that is an abstract
endpoint with no filesystem entry; elsewhere `@` is an ordinary path character,
and rather than risk deleting a stranger's file we leak ours.

**`serve()` trusts whoever can connect.** The wire is unauthenticated pickle and
an empty frame stops the worker, so the socket's directory is the access control.
A spawned `ProcessProxy` gets this for free — its addresses live in a per-uid
directory created `0700`.

## License

MIT
