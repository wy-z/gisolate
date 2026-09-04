"""Tests for gisolate.subprocess module."""

import multiprocessing.process
import os
import subprocess
import sys
import textwrap
import time

import gevent
import pytest

from gisolate._internal import ProcessError
from gisolate.proxy import _proc_exited
from gisolate.subprocess import run_in_subprocess

from .helpers import (
    add,
    big_bytes,
    get_pid,
    greet,
    ignore_sigterm_and_sleep,
    noop,
    raise_gevent_timeout,
    raise_value_error,
    raises_a_hostile_traceback,
    returns_a_value_that_exits_on_arrival,
    returns_a_value_that_will_not_serialize,
    slow_func,
    slow_returning,
    suicide,
)


class TestRunInSubprocess:
    def test_basic_return(self):
        assert run_in_subprocess(add, args=(3, 4)) == 7

    def test_runs_in_different_process(self):
        child_pid = run_in_subprocess(get_pid)
        assert child_pid != os.getpid()

    def test_propagates_exception(self):
        with pytest.raises(ValueError, match="subprocess boom"):
            run_in_subprocess(raise_value_error)

    def test_timeout(self):
        with pytest.raises(TimeoutError):
            run_in_subprocess(slow_func, timeout=0.5)

    def test_kwargs(self):
        result = run_in_subprocess(greet, args=("world",), kwargs={"greeting": "hi"})
        assert result == "hi world"

    def test_returns_none(self):
        assert run_in_subprocess(noop) is None

    def test_crashed_child_reported_when_is_alive_lies(self, monkeypatch):
        """A gevent parent's libev loop can steal the reap; multiprocessing's
        waitpid then gets ECHILD and is_alive() calls a dead child running
        forever. Pinned True here: a crashed target used to burn the whole
        timeout (1h default) and surface as TimeoutError."""
        monkeypatch.setattr(
            multiprocessing.process.BaseProcess, "is_alive", lambda self: True
        )
        with pytest.raises(RuntimeError, match="exited with code"):
            run_in_subprocess(suicide, timeout=30, poll_interval=0.05)

    # gevent patches subprocess to fork+exec, and receiving a subprocess result
    # now puts a worker on the hub's threadpool — which 3.12+ warns about at any
    # fork. Not about the code under test: gisolate itself never forks (its
    # default context is spawn), and gevent's own threadpool resolver puts
    # threads in the same process anyway.
    @pytest.mark.filterwarnings("ignore:This process .* is multi-threaded")
    def test_make_pipe_forces_blocking_with_gevent_patch(self):
        script = textwrap.dedent(
            """
            import multiprocessing
            import os

            from gevent import monkey

            monkey.patch_all()

            from gisolate.subprocess import _make_pipe

            parent_conn, child_conn = _make_pipe(multiprocessing.get_context("spawn"))
            for conn in (parent_conn, child_conn):
                print(os.get_blocking(conn.fileno()))
                conn.close()
            """
        )

        proc = subprocess.run(
            [sys.executable, "-c", script],
            check=True,
            capture_output=True,
            text=True,
        )

        assert proc.stdout.strip().splitlines() == ["True", "True"]


class TestStartFailureCleanup:
    def test_a_failed_start_closes_both_pipe_ends(self, spawn_ctx):
        """proc.start() raises on its own — an unpicklable target under spawn
        is the usual way — and the cleanup below it never ran, so both ends
        stayed open for as long as the caller kept the traceback."""
        with pytest.raises(Exception) as excinfo:
            run_in_subprocess(lambda: 1, mp_context=spawn_ctx, timeout=5)
        # Walk the traceback the way a caller holding the exception would.
        frames = []
        tb = excinfo.tb
        while tb is not None:
            frames.append(tb.tb_frame)
            tb = tb.tb_next
        conns = [
            v
            for f in frames
            for v in f.f_locals.values()
            if type(v).__name__ == "Connection"
        ]
        assert conns, "the repro must actually reach the pipe frames"
        assert all(c.closed for c in conns)


class TestInterruptedCleanup:
    def test_a_second_timeout_does_not_strand_the_child(self):
        """cleanup() runs proc.join(), a gevent switch point, so a caller's
        enclosing timeout can land between terminate() and kill() — leaving a
        child that ignores SIGTERM running, reachable only through the
        multiprocessing state this frame is about to drop."""

        class Recording:
            def __init__(self, ctx):
                self._ctx = ctx
                self.procs = []

            def Pipe(self, *args, **kwargs):
                return self._ctx.Pipe(*args, **kwargs)

            def Process(self, *args, **kwargs):
                proc = self._ctx.Process(*args, **kwargs)
                self.procs.append(proc)
                return proc

        ctx = Recording(multiprocessing.get_context("spawn"))
        timer = gevent.Timeout.start_new(3.0)  # fires inside cleanup's join
        try:
            with pytest.raises(gevent.Timeout) as excinfo:
                run_in_subprocess(
                    ignore_sigterm_and_sleep, timeout=2.0, mp_context=ctx
                )
            assert excinfo.value is timer
        finally:
            timer.close()

        proc = ctx.procs[0]
        for _ in range(100):
            if _proc_exited(proc):
                break
            gevent.sleep(0.1)
        try:
            assert _proc_exited(proc), "child left running"
        finally:
            if not _proc_exited(proc):
                proc.kill()
                proc.join(timeout=2)


class TestLargeResult:
    @pytest.mark.skipif(
        os.environ.get("CI") == "true",
        reason="the 90ms bound needs a quiet machine: a shared runner stalled 109ms on the loads alone",
    )
    def test_receiving_one_does_not_stop_the_hub(self):
        """poll(0) promises a byte, not a whole frame: recv_bytes then blocks
        until the last one arrives, on the fds _make_pipe deliberately leaves
        blocking. At roughly 1.5ms per MB that stopped every greenlet in this
        process for the length of the transfer — and took the timeout with it,
        in a module named for gevent-safe polling."""
        size = 128 << 20
        ticks = []

        def ticker():
            while True:
                ticks.append(time.monotonic())
                gevent.sleep(0.005)

        watcher = gevent.spawn(ticker)
        try:
            gevent.sleep(0.05)  # let it settle into its cadence
            result = run_in_subprocess(big_bytes, args=(size,), timeout=60)
        finally:
            watcher.kill()

        assert len(result) == size
        worst = max(b - a for a, b in zip(ticks, ticks[1:]))
        assert worst < 0.09, f"hub stalled {worst * 1000:.0f}ms during the receive"


class TestSaturatedThreadpool:
    def test_it_does_not_defeat_the_timeout(self):
        """The receive runs on the hub's threadpool, and its spawn waits on an
        untimed semaphore for a slot — measured at 2.5s with a backlog — so a
        pool busy with somebody else's blocking work would hold this function's
        promise open for as long as that lasts."""
        import gevent.monkey

        sleep = gevent.monkey.get_original("time", "sleep")
        pool = gevent.get_hub().threadpool
        released = False

        def hold():
            # Polled, not an Event: get_original's Event still builds on the
            # patched Condition, and a set() from this greenlet lost its
            # cross-thread wake — the pool sat out the whole wait, and the
            # next test's apply() paid for it (measured at 28s).
            while not released:
                sleep(0.05)

        def load():
            # From its own greenlet: spawn itself blocks once the pool is full,
            # and a backlog is what makes the next apply wait.
            for _ in range(pool.maxsize * 3):
                pool.spawn(hold)

        loader = gevent.spawn(load)
        try:
            gevent.sleep(0.5)  # let the pool fill and the queue build
            start = time.monotonic()
            with pytest.raises(TimeoutError):
                run_in_subprocess(add, args=(1, 2), timeout=1.0)
            assert time.monotonic() - start < 3.0
        finally:
            released = True
            loader.kill()


class TestBaseExceptionFromTarget:
    def test_it_is_forwarded_rather_than_killing_the_child(self):
        """A target's own expiring gevent.Timeout is a BaseException. Uncaught,
        the child died with nothing sent and the caller learned only that a
        process had exited."""
        with pytest.raises(Exception) as excinfo:
            run_in_subprocess(raise_gevent_timeout, timeout=30)
        assert "exited with code" not in str(excinfo.value)
        assert "Timeout" in str(excinfo.value)

    def test_a_result_that_exits_while_arriving_is_the_call_s_failure(self):
        """The child serializes the result; the PARENT reconstructs it, running
        the target's code here. Unguarded, a __reduce__ callable raising
        SystemExit came out of run_in_subprocess raw — past every ordinary
        `except Exception` in the caller, and out of a host that never chose to
        exit."""
        with pytest.raises(ProcessError, match="Bad result"):
            run_in_subprocess(returns_a_value_that_exits_on_arrival, timeout=30)

    def test_a_failure_that_cannot_be_serialized_twice_still_reaches_the_caller(self):
        """The child's error reply is serialized after wrap_exception has proved
        the error pickles ONCE. A second call that raises took the child down
        with nothing sent, and the caller was told only that a process had
        exited — the failure it was owed replaced by a generic one."""
        with pytest.raises(Exception) as excinfo:
            run_in_subprocess(returns_a_value_that_will_not_serialize, timeout=30)
        assert "exited with code" not in str(excinfo.value)

    def test_a_failure_whose_traceback_exits_is_still_the_call_s_failure(self):
        """Deserializing is not the last of the child's code the parent runs:
        every receiver then reaches for __remote_traceback__ to log it, and a
        hostile __getattribute__ turned that read into the caller's own exit."""
        with pytest.raises(Exception) as excinfo:
            run_in_subprocess(raises_a_hostile_traceback, timeout=30)
        assert not isinstance(excinfo.value, SystemExit)


class TestPollIntervalRespectsDeadline:
    def test_a_long_interval_does_not_outlast_the_timeout(self):
        """The sleep between polls was uncapped, so an interval larger than the
        timeout slept past the deadline and then reported a timeout for a
        result that had already arrived."""
        start = time.monotonic()
        with pytest.raises(TimeoutError):
            run_in_subprocess(slow_func, args=(5,), timeout=1.0, poll_interval=30.0)
        assert time.monotonic() - start < 5.0


class TestFinalReceiveCheck:
    def test_a_result_arriving_in_the_last_sleep_is_not_a_timeout(self):
        """The sleep is capped by the deadline, and the loop then exits without
        looking again — so a result delivered during that sleep was discarded
        and reported as a timeout."""
        assert (
            run_in_subprocess(
                slow_returning, args=(0.2,), timeout=2.0, poll_interval=30.0
            )
            == "done"
        )


class TestPipeRollback:
    def test_a_failed_set_blocking_closes_both_ends(self, monkeypatch):
        """set_blocking can refuse after Pipe() succeeded: both descriptors
        escaped through the traceback, with the cleanup never entered."""
        from gisolate import subprocess as gsub

        made = []
        real_ctx = gsub.proxy.get_default_mp_context()

        class Recording:
            def Pipe(self):
                pair = real_ctx.Pipe()
                made.extend(pair)
                return pair

            def __getattr__(self, name):
                return getattr(real_ctx, name)

        calls = []

        def refuse(_fd, _blocking):
            # The SECOND call, so the first endpoint is already configured: a
            # rollback that closed only what the loop had not reached yet
            # would leak it.
            calls.append(1)
            if len(calls) == 2:
                raise OSError("no fcntl for you")

        monkeypatch.setattr(gsub.os, "set_blocking", refuse)
        with pytest.raises(OSError, match="no fcntl"):
            run_in_subprocess(noop, mp_context=Recording())
        monkeypatch.undo()
        assert len(made) == 2 and all(c.closed for c in made)


class TestCleanupTotality:
    def test_a_raising_pipe_close_still_ends_the_child(self, monkeypatch):
        """cleanup ran its steps in sequence, so the parent pipe close raising
        skipped the terminate and kill after it: a timed-out child ran its
        full 30 seconds against a caller that had already been answered."""
        from gisolate import subprocess as gsub

        real_ctx = gsub.proxy.get_default_mp_context()
        made = []
        procs = []

        class Recording:
            def Pipe(self):
                pair = real_ctx.Pipe()
                made.extend(pair)
                return pair

            def Process(self, *a, **k):
                proc = real_ctx.Process(*a, **k)
                procs.append(proc)
                return proc

            def __getattr__(self, name):
                return getattr(real_ctx, name)

        armed = [True]

        def failing_close(conn_self):
            if armed[0] and made and conn_self is made[0]:
                armed[0] = False
                raise OSError("close refused")
            return real_close(conn_self)

        import multiprocessing.connection as mpc

        real_close = mpc.Connection.close
        monkeypatch.setattr(mpc.Connection, "close", failing_close)
        with pytest.raises(TimeoutError):
            run_in_subprocess(slow_func, (30,), timeout=1, mp_context=Recording())
        monkeypatch.undo()

        assert procs, "no child was ever started"
        deadline = time.monotonic() + 5
        while not _proc_exited(procs[0]) and time.monotonic() < deadline:
            gevent.sleep(0.1)
        assert _proc_exited(procs[0]), (
            "the raising pipe close skipped the terminate after it"
        )

    def test_a_custom_context_raising_terminate_still_ends_the_child(
        self, monkeypatch
    ):
        """The cleanup guards caught OSError only, and a custom mp_context —
        which is supported — can raise anything from terminate(): the kill
        escalation after it was skipped and the timed-out child ran on."""
        from multiprocessing.context import SpawnProcess

        from gisolate import subprocess as gsub

        real_ctx = gsub.proxy.get_default_mp_context()
        procs = []

        class Recording:
            def Process(self, *a, **k):
                proc = real_ctx.Process(*a, **k)
                procs.append(proc)
                return proc

            def __getattr__(self, name):
                return getattr(real_ctx, name)

        armed = [True]
        real_terminate = SpawnProcess.terminate

        def raising_terminate(proc_self):
            if armed[0] and procs and proc_self is procs[0]:
                armed[0] = False
                raise RuntimeError("terminate refused by a custom context")
            return real_terminate(proc_self)

        monkeypatch.setattr(SpawnProcess, "terminate", raising_terminate)
        with pytest.raises(TimeoutError):
            run_in_subprocess(slow_func, (30,), timeout=1, mp_context=Recording())
        monkeypatch.undo()

        assert procs, "no child was ever started"
        deadline = time.monotonic() + 6
        while not _proc_exited(procs[0]) and time.monotonic() < deadline:
            gevent.sleep(0.1)
        assert _proc_exited(procs[0]), (
            "the raising terminate skipped the kill after it"
        )
