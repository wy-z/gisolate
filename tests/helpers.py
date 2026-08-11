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

    closes = 0  # class-level so cancelled instances are observable in-child

    def __init__(self):
        self.ready = False

    async def connect(self):
        import asyncio

        await asyncio.sleep(0.8)
        self.ready = True

    async def close(self):
        type(self).closes += 1

    def is_ready(self):
        return self.ready

    def close_count(self):
        return type(self).closes


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


def make_list():
    return list(range(10000))


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

    path = proxy._addr.removeprefix("ipc://")
    deadline = time.monotonic() + timeout
    while not os.path.exists(path):
        assert time.monotonic() < deadline, "worker never bound its socket"
        gevent.sleep(0.02)
