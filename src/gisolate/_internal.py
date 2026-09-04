"""Internal primitives: unpatched stdlib, exceptions, serialization."""

import contextlib
import logging
import os
import pickle
from typing import Any, Callable, Iterable, Protocol

import dill
import gevent.monkey

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Unpatched stdlib primitives
# ---------------------------------------------------------------------------

current_thread = gevent.monkey.get_original("threading", "current_thread")
RLock = gevent.monkey.get_original("threading", "RLock")
# NOTE the limit of get_original: it returns the original CLASS, whose
# instances still build their internals from the patched threading module at
# construction time — an Event's Condition gets a gevent-backed Lock. See
# hub.AsyncResult for where that matters.
Event = gevent.monkey.get_original("threading", "Event")
Local = gevent.monkey.get_original("threading", "local")

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class ProcessError(RuntimeError):
    """Child process died or communication failed."""


class RemoteError(RuntimeError):
    """Wrapper for exceptions from child process that can't be pickled."""

    def __init__(self, message: str, exc_type: str = ""):
        self.exc_type = exc_type
        super().__init__(message)


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


class Serializer(Protocol):
    """Pluggable serializer protocol: ``dumps(obj) -> bytes`` / ``loads(bytes) -> obj``."""

    @staticmethod
    def dumps(obj: Any) -> bytes: ...

    @staticmethod
    def loads(data: bytes) -> Any: ...


class SmartPickle:
    """Serializer preferring pickle, falling back to dill.

    Implements the :class:`Serializer` protocol.
    """

    _PICKLE = b"P"
    _DILL = b"D"

    @classmethod
    def dumps(cls, obj: Any) -> bytes:
        try:
            return cls._PICKLE + pickle.dumps(obj, protocol=5)
        except (pickle.PicklingError, TypeError, AttributeError):
            return cls._DILL + dill.dumps(obj)

    @classmethod
    def loads(cls, data: bytes) -> Any:
        mv = memoryview(data)
        tag = mv[:1]
        if tag == cls._PICKLE:
            return pickle.loads(mv[1:])
        return dill.loads(bytes(mv[1:]))


def _ipc_path(address: str) -> str | None:
    """Filesystem path behind an ``ipc://`` address, or None when there is none
    to protect: a different transport, or a Linux abstract endpoint (``@name``),
    whose ``address[6:]`` would be a RELATIVE path naming an unrelated file in
    the working directory. We would rather leak a socket file on a platform
    where ``@`` is an ordinary path character than delete a stranger's file on
    the one this is deployed to.
    """
    if not address.startswith("ipc://") or address.startswith("ipc://@"):
        return None
    return address[6:]


class IpcLease:
    """A claim on the socket file behind an address.

    Three models, one constructor each, because picking the wrong one is a bug
    that has been written in this package more than once:

    - :meth:`claim` — we bound it ourselves. libzmq unlinks an ipc path before
      binding it, so a replacement silently takes the address over instead of
      failing; releasing blind at that point strands the live owner, reachable
      to its existing peers and invisible to every new connect. Device as well
      as inode, since a symlink dropped in our place could otherwise match on
      inode number alone.
    - :meth:`private` — we own the process that binds it, at an address nobody
      else can name. There is no one else it could belong to, and we never see
      the bind, so release removes whatever is there.
    - :meth:`none` — not ours to remove. Every connector holds this one.

    Two windows stay open, both the same shape and neither closable from here.
    A replacement that binds between OUR bind and :meth:`claim`'s stat gets
    recorded as ours, and one that binds between :meth:`release`'s stat and its
    unlink gets removed by us. Both are microseconds against the lifetime of a
    process, and libzmq exposes no descriptor for the listening socket, so
    there is nothing to fstat instead of the path — closing them needs a lock
    protocol the address by itself cannot carry. What the lease buys is the
    difference between "wrong whenever anyone rebinds" and "wrong only if the
    rebind lands in that window".
    """

    __slots__ = ("_path", "_file_id")

    def __init__(self, path: str | None, file_id: tuple[int, int] | None):
        self._path = path
        self._file_id = file_id

    @classmethod
    def none(cls) -> "IpcLease":
        return cls(None, None)

    @classmethod
    def claim(cls, address: str) -> "IpcLease":
        """Record the file just bound at *address*, so release removes that one
        and no other. Call AFTER the bind.

        A failed stat RAISES rather than degrading the claim. Suppressing it
        returned none() and silently discarded ownership — the file outlived
        every close — and falling back to the private model was worse: that
        blind-unlink would last the transport's whole LIFETIME, so a server
        legitimately rebinding the address minutes later would have its live
        endpoint removed by ours closing. Raising lands in ZmqTransport.open's
        rollback, whose own blind unlink spans microseconds, not a lifetime.
        """
        path = _ipc_path(address)
        if path is None:
            return cls.none()
        st = os.stat(path)
        return cls(path, (st.st_dev, st.st_ino))

    @classmethod
    def private(cls, address: str) -> "IpcLease":
        path = _ipc_path(address)
        return cls.none() if path is None else cls(path, None)

    def release(self) -> None:
        """Remove the file if it is ours. Idempotent, and never raises.

        The claim is cleared only once the outcome is known: clearing it first
        meant a transient stat or unlink failure orphaned the file for good,
        because the retry a later release() could have been found nothing left
        to do.
        """
        path, file_id = self._path, self._file_id
        if path is None:
            return
        try:
            if file_id is not None:
                st = os.stat(path)
                # Field by field, not a built tuple: the comparison runs after
                # ownership was detached, and the tuple was the one avoidable
                # allocation left on this path.
                if st.st_dev != file_id[0] or st.st_ino != file_id[1]:
                    self._path = self._file_id = None
                    return  # somebody rebound the address; it is theirs now
            os.unlink(path)
        except FileNotFoundError:
            pass  # already gone — released
        except KeyboardInterrupt:
            raise  # the operator's; the claim stays for the retry
        except BaseException:  # noqa: BLE001
            # Not just OSError: the stat's result is an allocation, and a
            # MemoryError escaping here broke the never-raises contract inside
            # somebody's finally. Whatever it was, the claim stays, so a later
            # release can retry.
            return
        self._path = self._file_id = None


class ZmqTransport:
    """One local ZMQ context, its socket, and the ipc claim that came with it.

    Acquired together, released exactly once. Nothing here reaches an owner's
    state, so a subsystem's own generation — its child process, reader, pending
    calls, send lock — stays that subsystem's business; this is only the part
    all of them acquire identically.

    Its IDENTITY is the generation. A loop that captured a transport asks
    ``t is owner._transport``, instead of comparing a socket against a field a
    concurrent start may already have replaced.
    """

    sock: Any
    address: str

    __slots__ = ("sock", "address", "_ctx", "_lease", "_closed")

    def __init__(self, sock: Any, ctx: Any, address: str, lease: IpcLease):
        self.sock = sock
        self._ctx = ctx
        self.address = address
        self._lease = lease
        self._closed = False

    def __del__(self):
        # The lease only: a full close would reach sockets whose loop or hub
        # may be gone at GC time. What this backstops is the claim a failed
        # release RETAINED — the owner closed us, dropped its reference, and
        # this is the last hand that can still unlink the file. Contained
        # completely: release re-raises the operator's interrupt, but an
        # exception escaping __del__ is printed and swallowed by the
        # interpreter anyway, and letting it out would discard the lease
        # mid-retry.
        try:
            if (lease := getattr(self, "_lease", None)) is not None:
                lease.release()
        except BaseException:  # noqa: BLE001 — GC context; nothing propagates
            pass

    @classmethod
    def open(
        cls,
        context_factory: Callable[[], Any],
        socket_type: int,
        address: str,
        *,
        bind: bool,
        options: Iterable[tuple[int, Any]] = (),
    ) -> "ZmqTransport":
        """Build context and socket together, or leave neither behind.

        Every step is inside: a socket allocation, a setsockopt or the bind
        itself can fail, and a context left open outlives the failure — under
        :func:`gisolate.serve` in a process that survives it, where it wedges
        the next term(). LINGER is always 0; every transport in this package
        wants it, and a caller that forgets is a lost frame at shutdown.

        Every allocation of its own comes FIRST — the carrier, and the blind
        lease its rollback would hand it. The rollback then only stores and
        closes: a version that allocated its way out could fail on the way,
        stranding a bound socket with the original error replaced, and the
        carrier built at the very end could itself be the allocation that
        failed after everything had succeeded.
        """
        import zmq

        carrier = cls(None, None, address, IpcLease.none())
        rollback_lease = IpcLease.private(address) if bind else IpcLease.none()
        sock = ctx = None
        bound = False
        try:
            ctx = context_factory()
            sock = ctx.socket(socket_type)
            sock.setsockopt(zmq.LINGER, 0)
            for option, value in options:
                sock.setsockopt(option, value)
            (sock.bind if bind else sock.connect)(address)
            bound = bind
            # Inside, because the claim stats the file — and can refuse: an
            # interrupt or a failed stat landing here used to leave a bound
            # socket and its context reachable only through the traceback, and
            # no lease to unlink the file either.
            lease = IpcLease.claim(address) if bind else IpcLease.none()
        except BaseException:
            # Through the pre-built carrier, so a failed open and a normal
            # close release by exactly the same path. Private rather than none
            # when our own bind is what created the file: the claim it would
            # have been is the step that failed, and after the close below
            # nobody else holds it.
            carrier.sock, carrier._ctx = sock, ctx
            if bound:
                carrier._lease = rollback_lease
            carrier.close()
            raise
        carrier.sock, carrier._ctx, carrier._lease = sock, ctx, lease
        return carrier

    def close(self) -> None:
        """Release everything acquired. Idempotent.

        It does not swallow everything: a BaseException from ``term()``'s switch
        point is the caller's own deadline or kill, and it propagates once every
        stage here has run.

        Nested, not sequential: ``term()`` is a switch point under zmq.green, so
        a caller's enclosing ``gevent.Timeout`` can land inside it, and each
        stage must still reach the next. Independently suppressed for the same
        reason — whatever brought us here is the diagnostic, and a failing close
        must neither replace it nor cost us the term().
        """
        if self._closed:
            # The lease keeps its claim through a failed release, and this is
            # the retry: everything else here really is done once.
            self._lease.release()
            return
        self._closed = True
        sock, ctx, lease = self.sock, self._ctx, self._lease
        self.sock = self._ctx = None
        # try/except rather than contextlib.suppress: suppress is an
        # allocation, and this is the one path that must not need one — a
        # MemoryError building the guard would have skipped the close the
        # guard was for.
        try:
            try:
                if sock is not None:
                    sock.close(linger=0)
            except Exception:
                pass
        finally:
            try:
                # Before term(): term is a switch point, and a blind rollback
                # lease held across it widened its unlink window from
                # microseconds to however long the greenlet stays switched out
                # — long enough for a replacement bind to land under it. The
                # file can go as soon as our socket is closed; term only reaps
                # the context.
                lease.release()
            finally:
                try:
                    if ctx is not None:
                        ctx.term()
                except Exception:
                    pass


_reimport_lock = RLock()
_reimport_users = 0
_reimport_orig: Any = None


@contextlib.contextmanager
def suppress_main_reimport():
    """Prevent child process from reimporting the caller's __main__ module.

    multiprocessing's spawn/forkserver contexts reimport __main__ in child
    processes via ``get_preparation_data``. Since gisolate workers live in
    their own modules, this reimport is unnecessary and causes errors
    (duplicate patches, side effects from re-executing main-module code).

    The patch is a process global, so while it is on it applies to everyone:
    an unrelated ``multiprocessing.Process`` started from another thread inside
    this window, whose target is defined in ``__main__``, loses the preparation
    it needs to resolve that target. Narrowing it to our own starts would mean
    not going through ``get_preparation_data`` at all — a different way of
    launching children, not a smaller patch.
    """
    import multiprocessing.spawn as mp_spawn

    global _reimport_users, _reimport_orig

    def _stripped(name):
        d = _reimport_orig(name)
        d.pop("init_main_from_path", None)
        d.pop("init_main_from_name", None)
        return d

    # Refcounted, because this patches a process global and two starts can
    # overlap: on a native thread each, or on two greenlets, since
    # Process.start() writes the pickled payload down a pipe and can block.
    # Saving and restoring per caller loses either way round — whoever leaves
    # first either puts the real function back under a start that still needs
    # it, or (worse, when the wrapper got saved as an "original") reinstates the
    # wrapper for good, so every later Process silently loses its __main__
    # preparation. The counter keeps it installed for exactly as long as
    # somebody is starting. Both critical sections are yield-free, so greenlets
    # cannot interleave inside them and the lock only has to hold threads apart.
    with _reimport_lock:
        if _reimport_users == 0:
            # Read HERE, not before the lock: between an unlocked read and this
            # point the last user could have left and restored the real
            # function, and we would save our own wrapper as the original — and
            # put it back for good on the way out.
            _reimport_orig = getattr(mp_spawn, "get_preparation_data", None)
            if _reimport_orig is not None:
                mp_spawn.get_preparation_data = _stripped  # type: ignore[assignment]
        _reimport_users += 1
        patched = _reimport_orig is not None
    if not patched:
        log.warning(
            "multiprocessing.spawn.get_preparation_data not found, "
            "cannot suppress __main__ reimport"
        )
    try:
        yield
    finally:
        with _reimport_lock:
            _reimport_users -= 1
            if _reimport_users == 0 and _reimport_orig is not None:
                mp_spawn.get_preparation_data = _reimport_orig  # type: ignore[assignment]


def require_ipc(address: str, who: str) -> None:
    """Reject a shared-worker address that is not an absolute ipc:// path.

    A call carries the caller's monotonic deadline and the payload is plain
    pickle — both are only meaningful between processes on one machine, under
    one privilege. A ``tcp://`` peer whose clock runs behind ours would honour
    requests the caller has already abandoned, and would let anyone who can
    reach the port deserialize into the host.

    Absolute, because the only protection on offer is the directory's: Linux
    abstract endpoints (``ipc://@name``) have no filesystem entry to protect,
    and a relative path names a different socket per working directory.
    """
    if not address.startswith("ipc:///"):
        raise ValueError(
            f"{who} supports absolute ipc:/// addresses only, got {address!r}"
        )


def type_name(e: BaseException) -> str:
    """The failure's class name — or a placeholder, because reading it is client
    code: a metaclass can make ``__name__`` a property that raises, or return
    something whose ``__format__`` does. Every site that names a failure it did
    not raise is a site that must not fail while doing so.
    """
    try:
        name = type(e).__name__
    except BaseException:  # noqa: BLE001
        return "UnknownError"
    return name if isinstance(name, str) else "UnknownError"


def remote_traceback(exc: Any) -> str | None:
    """The traceback attached to a failure that came off the wire, if any.

    Guarded for the same reason :func:`type_name` is, one step later: the reply
    deserialized, which proves nothing about READING it. A ``__getattribute__``
    of the sender's own making runs here, and every receiver in this package
    reaches for this attribute to log it — behind the guard that had just made
    the deserialize safe.
    """
    try:
        tb = getattr(exc, "__remote_traceback__", None)
    except BaseException:  # noqa: BLE001
        return None
    return tb if isinstance(tb, str) else None


def wrap_exception(e: BaseException, tb_str: str | None = None) -> Exception:
    """Ensure exception survives serialization round-trip, attach remote traceback."""
    exc: Exception | None = None
    # A non-Exception BaseException (gevent.Timeout, SystemExit) stays wrapped
    # even if it pickles: re-raised intact in the caller it reads as control
    # flow rather than as a failed call.
    if isinstance(e, Exception):
        # BaseException, not Exception: a __reduce__ raising SystemExit would
        # escape this probe and, since this runs inside somebody's `except`
        # block, be caught by nothing above it either — no reply for the
        # caller, and in the asyncio worker a dead worker for everyone.
        with contextlib.suppress(BaseException):
            SmartPickle.loads(SmartPickle.dumps(e))
            exc = e
    if exc is None:
        # Every read here goes through code the client controls, and this is
        # the last step before an error reply goes on the wire: a __str__ that
        # raises would strand the caller with no reply at all. Same width and
        # same reason as the probe above.
        name = type_name(e)
        try:
            detail = str(e)
        except BaseException:  # noqa: BLE001
            detail = "<unprintable>"
        try:
            # Both parts are real strings by now, so only their SIZE can fail
            # this — a client's __str__ can return as much text as it likes.
            # The fallback is literals only, so it cannot raise in turn.
            exc = RemoteError(f"{name}: {detail}", name)
        except BaseException:  # noqa: BLE001
            exc = RemoteError("UnknownError: <unprintable>", "UnknownError")
    if tb_str:
        # Not just AttributeError: __slots__ is the ordinary reason this fails,
        # but a custom __setattr__ can raise anything, and losing the reply over
        # a traceback we were only attaching for convenience is the worst trade
        # in this function.
        with contextlib.suppress(BaseException):
            exc.__remote_traceback__ = tb_str  # type: ignore[attr-defined]
    return exc
