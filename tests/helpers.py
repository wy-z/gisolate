"""Shared test helpers — importable by child processes (must be top-level picklable)."""

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

    def slow(self, seconds=5):
        import time

        time.sleep(seconds)
        return "done"

    def pid(self):
        return os.getpid()


def adder_factory():
    return Adder()


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
