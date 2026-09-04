"""Tests for gisolate._internal module."""

import os
import tempfile
import uuid

import gevent
import pytest
import zmq
import zmq.green

from gisolate import _internal
from gisolate._internal import (Event, Local, ProcessError, RemoteError,
                                RLock, SmartPickle, ZmqTransport,
                                suppress_main_reimport,
                                wrap_exception)

# ---------------------------------------------------------------------------
# Unpatched primitives
# ---------------------------------------------------------------------------


class TestUnpatchedPrimitives:
    def test_event_set_and_wait(self):
        e = Event()
        assert not e.is_set()
        e.set()
        assert e.wait(timeout=0.1)

    def test_rlock_acquire_release(self):
        lock = RLock()
        assert lock.acquire(timeout=0.1)
        lock.release()

    def test_rlock_reentrant(self):
        lock = RLock()
        lock.acquire()
        assert lock.acquire(timeout=0.1)
        lock.release()
        lock.release()

    def test_local_isolation(self):
        local = Local()
        local.x = 42
        assert local.x == 42


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class TestExceptions:
    def test_process_error_is_runtime_error(self):
        assert issubclass(ProcessError, RuntimeError)

    def test_remote_error_message_and_type(self):
        err = RemoteError("something broke", "ValueError")
        assert str(err) == "something broke"
        assert err.exc_type == "ValueError"

    def test_remote_error_default_exc_type(self):
        err = RemoteError("msg")
        assert err.exc_type == ""


# ---------------------------------------------------------------------------
# SmartPickle
# ---------------------------------------------------------------------------


class TestSmartPickle:
    def test_roundtrip_simple_types(self):
        for obj in [42, "hello", [1, 2, 3], {"a": 1}, (1,), {1, 2}]:
            assert SmartPickle.loads(SmartPickle.dumps(obj)) == obj
        assert SmartPickle.loads(SmartPickle.dumps(None)) is None

    def test_uses_pickle_prefix_for_simple(self):
        data = SmartPickle.dumps(42)
        assert data[:1] == b"P"

    def test_roundtrip_lambda_uses_dill(self):
        fn = lambda x: x + 1  # noqa: E731
        data = SmartPickle.dumps(fn)
        assert data[:1] == b"D"
        restored = SmartPickle.loads(data)
        assert restored(10) == 11


# ---------------------------------------------------------------------------
# wrap_exception
# ---------------------------------------------------------------------------


class TestWrapException:
    def test_picklable_exception_passes_through(self):
        original = ValueError("test error")
        wrapped = wrap_exception(original)
        assert wrapped is original

    def test_unpicklable_becomes_remote_error(self):
        class WeirdError(Exception):
            def __reduce__(self):
                raise TypeError("can't pickle")

        original = WeirdError("oops")
        wrapped = wrap_exception(original)
        assert isinstance(wrapped, RemoteError)
        assert "WeirdError" in str(wrapped)
        assert wrapped.exc_type == "WeirdError"

    def test_traceback_attached(self):
        err = ValueError("boom")
        wrapped = wrap_exception(err, tb_str="Traceback line 1\nline 2")
        assert getattr(wrapped, "__remote_traceback__") == "Traceback line 1\nline 2"

    def test_no_traceback_when_none(self):
        err = ValueError("boom")
        wrapped = wrap_exception(err)
        assert not hasattr(wrapped, "__remote_traceback__")

    def test_dill_fallback_for_pickle_failure(self):
        """If pickle fails but dill succeeds, exception still passes through."""
        # Standard exceptions with non-standard args fail pickle but dill handles them
        original = ValueError(lambda: 42)
        wrapped = wrap_exception(original)
        assert wrapped is original

    def test_dumps_ok_but_loads_fails_becomes_remote_error(self):
        """Exceptions that pickle-dump but fail to load become RemoteError."""

        class BadLoadError(Exception):
            def __init__(self, msg: str, *, required_kwarg: object):
                self.required_kwarg = required_kwarg
                super().__init__(msg)

        original = BadLoadError("boom", required_kwarg="val")
        wrapped = wrap_exception(original)
        assert isinstance(wrapped, RemoteError)
        assert "BadLoadError" in str(wrapped)
        assert wrapped.exc_type == "BadLoadError"


# ---------------------------------------------------------------------------
# suppress_main_reimport
# ---------------------------------------------------------------------------


class TestSuppressMainReimport:
    def test_strips_init_keys_from_preparation_data(self):
        import multiprocessing.spawn as mp_spawn

        orig_fn = mp_spawn.get_preparation_data

        with suppress_main_reimport():
            patched_data = mp_spawn.get_preparation_data("__main__")
            assert "init_main_from_name" not in patched_data
            assert "init_main_from_path" not in patched_data

        # Original function restored by identity
        assert mp_spawn.get_preparation_data is orig_fn

    def test_strips_init_main_from_path_for_script_mode(self, monkeypatch):
        """Simulate `python app.py` where __main__ has __file__ but no __spec__."""
        import multiprocessing.spawn as mp_spawn
        import sys

        main = sys.modules["__main__"]
        monkeypatch.setattr(main, "__file__", "/tmp/fake_app.py")
        monkeypatch.setattr(main, "__spec__", None)

        with suppress_main_reimport():
            data = mp_spawn.get_preparation_data("__main__")
            assert "init_main_from_path" not in data


class TestSuppressMainReimportConcurrency:
    def test_overlapping_starters_restore_the_real_function(self):
        """Two processes being started at once overlap inside this context
        manager. Stacking a second wrapper made the one that left FIRST restore
        the real function and the other reinstate a wrapper for good — after
        which every unrelated multiprocessing.Process silently lost its
        __main__ preparation and could fail to resolve a target defined there.
        """
        import multiprocessing.spawn as mp_spawn

        import gevent.monkey

        Thread = gevent.monkey.get_original("threading", "Thread")
        Event_ = gevent.monkey.get_original("threading", "Event")

        pristine = mp_spawn.get_preparation_data
        first_in, second_in, first_out = Event_(), Event_(), Event_()

        def first():
            with suppress_main_reimport():
                first_in.set()
                second_in.wait(5)  # the other starter overlaps us…
            first_out.set()  # …and we are the one that leaves first

        observed = []

        def second():
            first_in.wait(5)
            with suppress_main_reimport():
                second_in.set()
                first_out.wait(5)
                # The other starter has left; ours has not. The patch must
                # still be on, or this start's child re-executes __main__.
                observed.append(mp_spawn.get_preparation_data)

        threads = [Thread(target=first), Thread(target=second)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert observed and observed[0] is not pristine
        assert mp_spawn.get_preparation_data is pristine


class TestWrapExceptionBaseException:
    def test_a_reducer_raising_a_base_exception_still_wraps(self):
        """wrap_exception runs inside somebody's `except` block, so anything
        escaping it is caught by nothing above either: no reply for the caller,
        and in the asyncio worker a SystemExit escaping the task kills the
        worker for every client."""

        class HostileReducer(Exception):
            def __reduce__(self):
                raise KeyboardInterrupt("raised while probing serializability")

        wrapped = wrap_exception(HostileReducer("payload"))
        assert isinstance(wrapped, RemoteError)
        assert wrapped.exc_type == "HostileReducer"

    def test_a_str_raising_a_base_exception_still_wraps(self):
        class HostileStr(Exception):
            def __reduce__(self):
                raise TypeError("not picklable")  # forces the RemoteError path

            def __str__(self):
                raise SystemExit("raised while formatting the message")

        wrapped = wrap_exception(HostileStr())
        assert isinstance(wrapped, RemoteError)
        assert "<unprintable>" in str(wrapped)

    def test_a_hostile_type_name_still_wraps(self):
        """type(e).__name__ goes through client-controlled code too — a
        metaclass property can raise — and it sat outside both guards."""

        class Meta(type):
            @property
            def __name__(cls):  # type: ignore[override]  # noqa: N805
                raise SystemExit("raised while reading the type name")

        class Hostile(Exception, metaclass=Meta):
            def __reduce__(self):
                raise TypeError("not picklable")  # forces the RemoteError path

        wrapped = wrap_exception(Hostile())
        assert isinstance(wrapped, RemoteError)

    def test_a_hostile_setattr_still_wraps(self):
        """Attaching the remote traceback is a convenience; losing the reply
        over it is the worst trade in this function. __slots__ raises
        AttributeError, but a custom __setattr__ can raise anything."""

        class Hostile(Exception):
            def __setattr__(self, name, value):
                if name == "__remote_traceback__":
                    raise KeyboardInterrupt("raised while attaching a traceback")
                super().__setattr__(name, value)

        wrapped = wrap_exception(Hostile("payload"), "a traceback")
        assert isinstance(wrapped, Exception)

    def test_a_hostile_name_format_still_wraps(self):
        """A metaclass can return a non-string from __name__, and the message
        interpolation then runs THAT object's __format__ — outside the guard
        that made reading the name safe."""

        class Unformattable:
            def __format__(self, _spec):
                raise SystemExit("raised while formatting the type name")

            def __str__(self):
                raise SystemExit("raised while stringifying the type name")

        class Meta(type):
            @property
            def __name__(cls):  # type: ignore[override]  # noqa: N805
                return Unformattable()

        class Hostile(Exception, metaclass=Meta):
            def __reduce__(self):
                raise TypeError("not picklable")  # forces the RemoteError path

        wrapped = wrap_exception(Hostile())
        assert isinstance(wrapped, RemoteError)


class _Recorder:
    """A context/socket pair that records what teardown reached it."""

    def __init__(self, fail_at=None):
        self.fail_at = fail_at
        self.closed: list[int | None] = []
        self.termed = 0
        self.made = 0

    def __call__(self):  # context factory
        if self.fail_at == "context":
            raise zmq.ZMQError(zmq.EINVAL)
        return self

    def socket(self, _type):
        if self.fail_at == "socket":
            raise zmq.ZMQError(zmq.EINVAL)
        self.made += 1
        return self

    def setsockopt(self, option, _value):
        if self.fail_at == "setsockopt" and option != zmq.LINGER:
            raise zmq.ZMQError(zmq.EINVAL)

    def bind(self, _addr):
        if self.fail_at == "bind":
            raise zmq.ZMQError(zmq.EINVAL)

    def connect(self, _addr):
        if self.fail_at == "connect":
            raise zmq.ZMQError(zmq.EINVAL)

    def close(self, linger=None):
        self.closed.append(linger)

    def term(self):
        self.termed += 1


def _short_addr():
    """tmp_path is too long for sockaddr_un (103 chars)."""
    return f"ipc://{tempfile.gettempdir()}/gi-t-{uuid.uuid4().hex[:8]}.sock"


class TestZmqTransportOpen:
    """Every stage can fail, and a context left open outlives the failure —
    under serve(), in a process that survives it, where it wedges the next
    term(). Only the bind stage was covered before."""

    @pytest.mark.parametrize("stage", ["socket", "setsockopt", "bind"])
    def test_a_failure_takes_the_transport_with_it(self, stage):
        rec = _Recorder(fail_at=stage)
        with pytest.raises(zmq.ZMQError):
            ZmqTransport.open(
                rec, zmq.ROUTER, "ipc:///whatever", bind=True,
                options=[(zmq.SNDHWM, 1)],
            )
        assert rec.termed == 1
        assert rec.closed == ([0] if rec.made else [])

    def test_a_failing_context_factory_leaves_nothing_to_close(self):
        rec = _Recorder(fail_at="context")
        with pytest.raises(zmq.ZMQError):
            ZmqTransport.open(rec, zmq.ROUTER, "ipc:///whatever", bind=True)
        assert rec.termed == 0 and rec.closed == []

    def test_a_failing_connect_takes_the_transport_with_it(self):
        rec = _Recorder(fail_at="connect")
        with pytest.raises(zmq.ZMQError):
            ZmqTransport.open(rec, zmq.DEALER, "ipc:///whatever", bind=False)
        assert rec.termed == 1 and rec.closed == [0]

    def test_a_failing_claim_takes_it_too(self, monkeypatch):
        """The claim is the LAST step, and it was outside the guard: it stats
        the file, so an interrupt can land in it, and what it leaves behind is
        a bound socket with its context — reachable only through the traceback,
        with no lease to unlink the file it bound either."""

        def interrupted(_address):
            raise KeyboardInterrupt("landed in the stat")

        monkeypatch.setattr(_internal.IpcLease, "claim", interrupted)
        rec = _Recorder()
        with pytest.raises(KeyboardInterrupt):
            ZmqTransport.open(rec, zmq.ROUTER, "ipc:///whatever", bind=True)
        assert rec.termed == 1 and rec.closed == [0]


class TestZmqTransportClose:
    def test_close_releases_and_is_idempotent(self):
        addr = _short_addr()
        path = addr.removeprefix("ipc://")
        transport = ZmqTransport.open(zmq.green.Context, zmq.ROUTER, addr, bind=True)
        assert os.path.exists(path)
        transport.close()
        assert not os.path.exists(path)
        transport.close()  # must not raise, must not touch a new file

    def test_a_term_that_hangs_still_releases_the_lease(self, tmp_path):
        """term() switches under zmq.green, so a caller's enclosing timeout can
        land inside it. Sequential suppression would skip the unlink."""
        path = tmp_path / "t.sock"
        path.write_bytes(b"")

        class HangingCtx:
            def term(self):
                raise gevent.Timeout(0.01)

        transport = _internal.ZmqTransport(
            None, HangingCtx(), f"ipc://{path}", _internal.IpcLease.private(f"ipc://{path}")
        )
        with pytest.raises(gevent.Timeout):
            transport.close()
        assert not path.exists()


class TestIpcLease:
    def test_a_claim_spares_the_file_a_replacement_bound(self):
        """libzmq unlinks an ipc path before binding it, so a replacement takes
        the address over silently. The departing owner must leave it alone."""
        addr = _short_addr()
        path = addr.removeprefix("ipc://")
        old = ZmqTransport.open(zmq.green.Context, zmq.ROUTER, addr, bind=True)
        new = ZmqTransport.open(zmq.green.Context, zmq.ROUTER, addr, bind=True)
        try:
            old.close()
            assert os.path.exists(path)
        finally:
            new.close()
        assert not os.path.exists(path)

    def test_a_connector_never_removes_the_file(self):
        addr = _short_addr()
        path = addr.removeprefix("ipc://")
        server = ZmqTransport.open(zmq.green.Context, zmq.ROUTER, addr, bind=True)
        client = ZmqTransport.open(zmq.green.Context, zmq.DEALER, addr, bind=False)
        try:
            client.close()
            assert os.path.exists(path)
        finally:
            server.close()

    def test_a_private_lease_removes_what_it_finds(self, tmp_path):
        """The proxy's model: it connects, its CHILD binds, and the address is
        one nobody else can name — so there is no inode of ours to match."""
        path = tmp_path / "child.sock"
        path.write_bytes(b"")
        _internal.IpcLease.private(f"ipc://{path}").release()
        assert not path.exists()

    def test_an_abstract_endpoint_is_left_alone(self, tmp_path, monkeypatch):
        """``ipc://@name`` is a Linux abstract endpoint with no file at all, and
        address[6:] would be a RELATIVE path naming somebody else's."""
        monkeypatch.chdir(tmp_path)
        bystander = tmp_path / "name"
        bystander.write_bytes(b"")
        _internal.IpcLease.private("ipc://@name").release()
        _internal.IpcLease.claim("ipc://@name").release()
        assert bystander.exists()




class TestClaimWithoutAStat:
    def test_a_failed_stat_fails_the_open_and_removes_the_file(self, monkeypatch):
        """A stat failure after a successful bind must not degrade the claim:
        answering none() silently discarded ownership (the file outlived every
        close), and a private fallback was worse — a blind unlink lasting the
        transport's LIFETIME, so a server legitimately rebinding the address
        later would have its endpoint removed by ours closing. claim() raises
        instead, into open()'s rollback, whose blind unlink spans microseconds."""
        import pathlib

        sock_file = pathlib.Path(
            f"{tempfile.gettempdir()}/gi-claim-{uuid.uuid4().hex[:8]}.sock"
        )
        addr = f"ipc://{sock_file}"
        real_stat = os.stat

        def failing_stat(path, *a, **k):
            if str(path) == str(sock_file):
                raise OSError("transient stat failure")
            return real_stat(path, *a, **k)

        monkeypatch.setattr(_internal.os, "stat", failing_stat)
        with pytest.raises(OSError, match="transient stat failure"):
            ZmqTransport.open(zmq.green.Context, zmq.ROUTER, addr, bind=True)
        monkeypatch.undo()

        assert not sock_file.exists(), "the rollback never removed the bound file"


class TestReleaseRetries:
    def test_a_transient_stat_failure_keeps_the_claim(self, monkeypatch):
        """release() used to clear its path before the fallible stat/unlink, so
        one transient failure orphaned the file for good — the retry a later
        release could have been found nothing left to do."""
        import pathlib

        sock_file = pathlib.Path(
            f"{tempfile.gettempdir()}/gi-retry-{uuid.uuid4().hex[:8]}.sock"
        )
        sock_file.write_bytes(b"")
        addr = f"ipc://{sock_file}"
        lease = _internal.IpcLease.claim(addr)
        real_stat = os.stat

        def failing_stat(path, *a, **k):
            if str(path) == str(sock_file):
                raise PermissionError("transient stat failure")
            return real_stat(path, *a, **k)

        monkeypatch.setattr(_internal.os, "stat", failing_stat)
        lease.release()  # must not raise, and must keep the claim
        monkeypatch.undo()

        assert sock_file.exists(), "a failed release removed the file anyway"
        lease.release()
        assert not sock_file.exists(), "the retry found nothing left to do"

    def test_a_memory_error_neither_raises_nor_loses_the_claim(self, monkeypatch):
        """The stat's result is an allocation, so release can fail with more
        than OSError — and it sits inside callers' finally blocks, where the
        never-raises contract is what keeps the rest of a teardown running."""
        import pathlib

        sock_file = pathlib.Path(
            f"{tempfile.gettempdir()}/gi-mem-{uuid.uuid4().hex[:8]}.sock"
        )
        sock_file.write_bytes(b"")
        addr = f"ipc://{sock_file}"
        lease = _internal.IpcLease.claim(addr)
        real_stat = os.stat

        def failing_stat(path, *a, **k):
            if str(path) == str(sock_file):
                raise MemoryError("no stat result for you")
            return real_stat(path, *a, **k)

        monkeypatch.setattr(_internal.os, "stat", failing_stat)
        lease.release()  # must not raise
        monkeypatch.undo()

        assert sock_file.exists()
        lease.release()
        assert not sock_file.exists()

    def test_a_second_close_retries_a_failed_release(self, monkeypatch):
        """close() commits _closed before the release, so a transient failure
        there had no retry path: the owners had already detached the
        transport, and every later close() returned at the flag."""
        import pathlib

        sock_file = pathlib.Path(
            f"{tempfile.gettempdir()}/gi-reclose-{uuid.uuid4().hex[:8]}.sock"
        )
        addr = f"ipc://{sock_file}"
        transport = ZmqTransport.open(zmq.green.Context, zmq.ROUTER, addr, bind=True)
        real_stat = os.stat

        def failing_stat(path, *a, **k):
            if str(path) == str(sock_file):
                raise PermissionError("transient stat failure")
            return real_stat(path, *a, **k)

        monkeypatch.setattr(_internal.os, "stat", failing_stat)
        transport.close()
        monkeypatch.undo()

        assert sock_file.exists(), "a failed release removed the file anyway"
        transport.close()  # the retry
        assert not sock_file.exists(), "the second close never retried"

    def test_a_dropped_transport_still_releases_a_retained_claim(self, monkeypatch):
        """An owner that closed its transport and dropped the reference has no
        retry path of its own: the claim a failed release retained lived
        inside an object nothing pointed at, and the file was permanent.
        __del__ is the last hand that can still unlink it."""
        import gc
        import pathlib

        sock_file = pathlib.Path(
            f"{tempfile.gettempdir()}/gi-drop-{uuid.uuid4().hex[:8]}.sock"
        )
        addr = f"ipc://{sock_file}"
        transport = ZmqTransport.open(zmq.green.Context, zmq.ROUTER, addr, bind=True)
        real_stat = os.stat

        def failing_stat(path, *a, **k):
            if str(path) == str(sock_file):
                raise PermissionError("transient stat failure")
            return real_stat(path, *a, **k)

        monkeypatch.setattr(_internal.os, "stat", failing_stat)
        transport.close()  # the release fails; the claim is retained
        monkeypatch.undo()
        assert sock_file.exists()

        del transport  # the owner walks away
        gc.collect()
        assert not sock_file.exists(), "the dropped claim was never retried"


class TestCloseOrder:
    def test_the_lease_is_released_before_term(self):
        """term() is a switch point: a blind rollback lease held across it
        widened its unlink window from microseconds to however long the
        greenlet stays switched out — long enough for a replacement bind to
        land under it."""
        order = []

        class FakeSock:
            def close(self, linger=None):
                order.append("sock")

        class FakeCtx:
            def term(self):
                order.append("term")

        class FakeLease(_internal.IpcLease):
            def __init__(self):
                super().__init__(None, None)

            def release(self):
                order.append("release")

        # Held through the assert: dropping it would run __del__'s lease
        # retry and append a fourth event before we look.
        transport = ZmqTransport(FakeSock(), FakeCtx(), "ipc://order-test", FakeLease())
        transport.close()
        assert order == ["sock", "release", "term"]
