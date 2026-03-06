"""Shared test helpers — importable by child processes (must be top-level picklable)."""

import os


class Adder:
    def add(self, a, b):
        return a + b

    def echo(self, x):
        return x

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
