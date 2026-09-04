"""Shared test helpers — importable by child processes (must be top-level picklable)."""

import contextlib
import os
import threading


class ConcurrencyTracker:
    """Tracks peak concurrent executions."""

    def __init__(self):
        self._lock = threading.Lock()
        self._current = 0
        self.peak = 0

    def run(self, seconds=0.2):
        import time

        with self._lock:
            self._current += 1
            if self._current > self.peak:
                self.peak = self._current
        time.sleep(seconds)
        with self._lock:
            self._current -= 1
        return self.peak

    def get_peak(self):
        return self.peak


def tracker_factory():
    return ConcurrencyTracker()


class Adder:
    def add(self, a, b):
        return a + b

    def echo(self, x):
        return x

    def echo_timeout(self, timeout=10):
        return timeout

    def fail(self):
        raise ValueError("intentional error")

    def raise_timeout(self):
        raise TimeoutError("quota exceeded")

    def slow(self, seconds=5):
        import time

        time.sleep(seconds)
        return "done"

    def pid(self):
        return os.getpid()


def adder_factory():
    return Adder()


class TimeoutSwallower:
    """Client whose retry loop swallows any injected exception and keeps
    blocking — models retry-on-Exception code that eats a raised deadline."""

    active = 0  # class-level so overlap is observable across calls in-child
    peak = 0

    def swallow_and_hang(self):
        import time

        while True:
            try:
                time.sleep(60)
            except Exception:  # noqa: BLE001
                continue

    def hang_with_slow_cleanup(self):
        import time

        cls = type(self)
        cls.active += 1
        cls.peak = max(cls.peak, cls.active)
        try:
            time.sleep(60)
        finally:
            time.sleep(1.0)  # yielding cleanup — a killed call lingers here
            cls.active -= 1

    def self_kill(self):
        import gevent

        raise gevent.GreenletExit("client killed itself")

    def escaping_base_exception(self):
        import gevent

        # A client's own expiring timeout guard — gevent.Timeout is a
        # BaseException, so it slips past `except Exception`.
        with gevent.Timeout(0.05):
            gevent.sleep(60)

    def get_peak(self):
        return type(self).peak

    def add(self, a, b):
        return a + b


def swallower_factory():
    return TimeoutSwallower()


class Unserializable:
    """Neither pickle nor dill can ship this: ``__reduce__`` raises something
    SmartPickle does not read as "try dill instead"."""

    def __reduce__(self):
        raise RuntimeError("cannot serialize me")


class Stateful:
    """Client with in-memory state, so a needless restart is observable."""

    def __init__(self):
        self.memory = None

    def remember(self, value):
        self.memory = value
        return True

    def recall(self):
        return self.memory

    def pid(self):
        return os.getpid()


def stateful_factory():
    return Stateful()


class CancelSwallower:
    """Async client whose retry loop swallows the cancellation its deadline
    raises — TimeoutSwallower's asyncio shape, so its slot never comes back."""

    async def swallow_and_hang(self):
        import asyncio

        while True:
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                continue

    async def add(self, a, b):
        return a + b


def cancel_swallower_factory():
    return CancelSwallower()


class UnprintableError(Exception):
    """Picklable, but formatting it raises — models exceptions whose __str__
    touches a lazy/detached attribute."""

    def __str__(self):
        raise RuntimeError("format failed")


class Unprintable:
    """Client raising an exception that cannot be stringified."""

    def boom(self):
        raise UnprintableError("payload")

    def add(self, a, b):
        return a + b


def unprintable_factory():
    return Unprintable()


class CancelLeaker:
    """Async client that leaks a CancelledError from an inner task — the
    common asyncio shape where a cancelled background await escapes the
    method the caller invoked."""

    async def leak_cancelled(self):
        import asyncio

        inner = asyncio.create_task(asyncio.sleep(60))
        inner.cancel()
        await inner  # CancelledError is a BaseException, not an Exception

    async def add(self, a, b):
        return a + b


def cancel_leaker_factory():
    return CancelLeaker()


class SlowConnectClient:
    """Async client whose connect() is slow enough to outlive a short deadline."""

    closes = 0  # class-level so the counts are observable in-child
    connects = 0

    def __init__(self):
        self.ready = False

    async def connect(self):
        import asyncio

        type(self).connects += 1
        await asyncio.sleep(0.8)
        self.ready = True

    async def close(self):
        type(self).closes += 1

    def is_ready(self):
        return self.ready

    def close_count(self):
        return type(self).closes

    def connect_count(self):
        return type(self).connects


def slow_connect_factory():
    return SlowConnectClient()


def add(a, b):
    return a + b


def get_pid():
    return os.getpid()


def raise_value_error():
    raise ValueError("subprocess boom")


def slow_func(seconds=30):
    import time

    time.sleep(seconds)


def greet(name, greeting="hello"):
    return f"{greeting} {name}"


def noop():
    pass


def suicide():
    import signal

    os.kill(os.getpid(), signal.SIGKILL)


def serve_adder(address, patch_kwargs=None):
    """Host entry point: this process BECOMES the worker (see gisolate.serve)."""
    import gisolate

    gisolate.serve(adder_factory, address, patch_kwargs=patch_kwargs)


class Marker:
    """Records every call it runs, so a test can prove one did NOT run."""

    def __init__(self, path):
        self.path = path

    def mark(self):
        with open(self.path, "a") as f:
            f.write("ran\n")

    def ping(self):
        return "pong"


def serve_marker(address, path):
    """Host entry point serving a Marker (see gisolate.serve).

    One slot on purpose: a later call cannot answer while an earlier one is
    still running, which lets a test use a reply as a barrier.
    """
    import functools

    import gisolate

    gisolate.serve(functools.partial(Marker, path), address, max_concurrency=1)


@contextlib.contextmanager
def host_process(spawn_ctx, target, address, *args):
    """Run a serve() host for the duration of a test.

    The host binds a fixed path and nothing in gisolate unlinks it — an
    attached client must not, and a terminated host cannot — so the test that
    chose the path is what clears it.
    """
    from gisolate.proxy import _proc_exited

    proc = spawn_ctx.Process(target=target, args=(address, *args), daemon=True)
    proc.start()
    try:
        yield proc
    finally:
        # Sentinel-gated like ProcessProxy._cleanup_process, and for the same
        # reason: under a gevent parent the child's reap can be stolen, leaving
        # is_alive() true over a pid that now belongs to someone else.
        if not _proc_exited(proc):
            proc.terminate()
            proc.join(timeout=5)
        if not _proc_exited(proc):
            proc.kill()
            proc.join(timeout=5)
        # Never unlink out from under a live host: the next test picks a fresh
        # path, so a survivor would linger unnamed and unnoticed.
        if _proc_exited(proc):
            with contextlib.suppress(OSError):
                os.unlink(address.removeprefix("ipc://"))


def wait_bound(proxy, timeout=10):
    """Block until the worker has bound its socket.

    Call deadlines are stamped by the caller, so on a cold proxy the child's
    startup spends the budget of the very first call. A test that needs a short
    deadline to land somewhere specific (inside a slow connect(), say) has to
    wait out the boot first, or it measures process startup instead.
    """
    import time

    import gevent

    path = proxy._transport.address.removeprefix("ipc://")
    deadline = time.monotonic() + timeout
    while not os.path.exists(path):
        assert time.monotonic() < deadline, "worker never bound its socket"
        gevent.sleep(0.02)


def ignore_sigterm_and_sleep(seconds=60):
    """Target that outlives terminate(), so only the kill step can end it."""
    import signal
    import time

    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    time.sleep(seconds)


class Unclosable:
    """Client whose close() expires — an ordinary shape: a shutdown hook with
    its own deadline, and gevent.Timeout is a BaseException."""

    def ping(self):
        return "pong"

    def close(self):
        import gevent

        raise gevent.Timeout(0.01)


def unclosable_factory():
    return Unclosable()


_worker_ticks: list[int] = []


class Ticker:
    """Client whose call never returns and whose side effects are visible from
    the process running the worker."""

    def tick_forever(self):
        import gevent

        while True:
            _worker_ticks.append(1)
            gevent.sleep(0.02)


def ticker_factory():
    return Ticker()


class BlockingConnect:
    """Async client whose connect() is an ordinary synchronous handshake — it
    knows nothing about asyncio and blocks the thread it runs on."""

    def connect(self):
        import time

        time.sleep(3.0)

    async def ping(self):
        return "pong"


def blocking_connect_factory():
    return BlockingConnect()


class SyncWrappedAsync:
    """What a decorator written for sync code leaves behind: a plain method
    that returns a coroutine instead of a result."""

    async def _add(self, a, b):
        return a + b

    def add(self, a, b):
        return self._add(a, b)


def sync_wrapped_async_factory():
    return SyncWrappedAsync()


def big_bytes(size):
    """A result big enough that moving it through the pipe takes real time."""
    return b"x" * size


class SlowBuild:
    """Client whose construction is slow AND synchronous — the executor running
    it cannot be stopped, so a deadline cancels only the await."""

    closes = 0  # class-level so the counts are observable in-child
    builds = 0

    def close(self):
        type(self).closes += 1

    def close_count(self):
        return type(self).closes

    def build_count(self):
        return type(self).builds

    def ping(self):
        return "pong"


def slow_build_factory():
    import time

    SlowBuild.builds += 1
    time.sleep(1.0)
    return SlowBuild()


def raise_gevent_timeout():
    """Target with its own expiring deadline — gevent.Timeout is a
    BaseException, so it slips past `except Exception`."""
    import gevent

    raise gevent.Timeout(0.01)


class MarkingBuild:
    """Client whose close() is slow, async, and observable from another
    process — the shape that loses when a teardown stops waiting too early."""

    def __init__(self, path):
        self.path = path

    async def close(self):
        import asyncio

        await asyncio.sleep(0.5)
        with open(self.path, "a") as f:
            f.write("closed\n")

    def ping(self):
        return "pong"


def slow_marking_build(path, seconds=1.0):
    """Slow AND synchronous, so a deadline can expire while it runs."""
    import time

    time.sleep(seconds)
    return MarkingBuild(path)


class NeverConnects:
    """Async client whose connect() waits on something that never arrives —
    ordinary, cancellable, and never cancelled, because the build is shielded."""

    async def connect(self):
        import asyncio

        await asyncio.Event().wait()

    def ping(self):
        return "pong"


def never_connects_factory():
    return NeverConnects()


class LateConnect:
    """Client whose synchronous connect() acquires its resource long after a
    teardown gives up on it — the executor thread cancellation cannot reach."""

    def __init__(self, path, seconds):
        self.path = path
        self.seconds = seconds

    def _mark(self, what):
        with open(self.path, "a") as f:
            f.write(what + "\n")

    def connect(self):
        import time

        time.sleep(self.seconds)
        self._mark("connected")  # the acquisition close() would be releasing

    def close(self):
        self._mark("closed")

    def ping(self):
        return "pong"


def late_connect_factory(path, seconds):
    return LateConnect(path, seconds)


class NestedProcessError:
    """Client that legitimately propagates a ProcessError of its own — from an
    inner proxy, say. It says nothing about THIS worker's transport."""

    def relay(self):
        from gisolate import ProcessError

        raise ProcessError("inner proxy is down")

    def pid(self):
        return os.getpid()


def nested_process_error_factory():
    return NestedProcessError()


def slow_returning(seconds=0.2):
    """Returns after a delay — long enough to miss the first poll."""
    import time

    time.sleep(seconds)
    return "done"


_exited_once: list[int] = []


def exit_once_factory():
    """Exits the process the first time it is called — what a client library
    does about configuration it does not like. SystemExit inside a task is
    re-raised into the event loop, which is how it used to end the worker."""
    import sys

    if not _exited_once:
        _exited_once.append(1)
        sys.exit(2)
    return Adder()


class ExitsOnClose:
    """Client whose cleanup calls sys.exit — its decision, not the host's."""

    def ping(self):
        return "pong"

    def close(self):
        import sys

        sys.exit(2)


def exits_on_close_factory():
    return ExitsOnClose()


class SlowBuildMarker:
    """Client whose build outlasts a shutdown drain, and whose every call and
    close is visible from another process."""

    def __init__(self, path):
        self.path = path

    def _mark(self, what):
        with open(self.path, "a") as f:
            f.write(what + "\n")

    def ping(self):
        self._mark("called")
        return "pong"

    def close(self):
        self._mark("closed")


def slow_build_marker(path, seconds=7.0):
    import time

    time.sleep(seconds)
    return SlowBuildMarker(path)


def _exit_while_unpickling():
    raise SystemExit("a reply reconstructed itself into an exit")


class ExitsWhenUnpickled:
    """Serializes cleanly; reconstructing it in the RECEIVER raises SystemExit.

    Both directions of a hop run client code, and only the sending one had a
    total error path.
    """

    def __reduce__(self):
        return (_exit_while_unpickling, ())


def returns_a_value_that_exits_on_arrival():
    return ExitsWhenUnpickled()


class ExitsOnSecondPickle(Exception):
    """Serializes once, then exits.

    wrap_exception's probe proves an exception pickles ONCE. The error reply is
    then serialized a second time, outside every guard — where a reducer that
    answers differently, or a MemoryError on a large error object, escapes.
    """

    calls = 0

    def __reduce__(self):
        type(self).calls += 1
        if type(self).calls > 1:
            raise SystemExit("the reply's own serialization exited")
        return (ExitsOnSecondPickle, ())


class FailsToSerializeHostilely:
    """A reply payload whose failure is that exception."""

    def __reduce__(self):
        raise ExitsOnSecondPickle("payload will not serialize")


class HostileTraceback(Exception):
    """Pickles fine; reading its remote traceback exits the reader.

    Deserialization succeeding is not the end of the client's code — every
    receiver then reaches for ``__remote_traceback__`` to log it.
    """

    def __getattribute__(self, name):
        if name == "__remote_traceback__":
            raise SystemExit("even naming the traceback is the client's code")
        return object.__getattribute__(self, name)


def raises_a_hostile_traceback():
    raise HostileTraceback("the failure the caller is owed")


def returns_a_value_that_will_not_serialize():
    return FailsToSerializeHostilely()


_unwind_order: list[str] = []


class UnwindOrder:
    """Records whether the client's close() ran before a killed handler had
    finished unwinding through its own finally."""

    def __init__(self):
        self.closed = False

    def wait_forever(self):
        import gevent

        try:
            gevent.sleep(120)
        finally:
            _unwind_order.append("closed-first" if self.closed else "handler")

    def close(self):
        self.closed = True
        _unwind_order.append("close")


def unwind_order_factory():
    _unwind_order.clear()
    return UnwindOrder()


class AsyncUnwindMarker:
    """The same order, in the runtime whose kill is task.cancel()."""

    def __init__(self, path):
        self.path = path

    def _mark(self, what):
        with open(self.path, "a") as f:
            f.write(what + "\n")

    async def wait_forever(self):
        import asyncio

        try:
            await asyncio.sleep(120)
        finally:
            self._mark("handler")

    def close(self):
        self._mark("close")


def async_unwind_marker(path):
    return AsyncUnwindMarker(path)
