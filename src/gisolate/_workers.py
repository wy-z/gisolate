"""Child process worker entry points: gevent and asyncio modes."""

import contextlib
import dataclasses
import time
import traceback
from typing import Any

from . import _internal


@dataclasses.dataclass(frozen=True, slots=True)
class WorkerConfig:
    """Configuration passed from ProcessProxy to child worker."""

    ipc_addr: str
    factory_bytes: bytes
    max_concurrency: int | None = None


OK = b"\x01"
ERR = b"\x00"
SHUTDOWN = b""

ID_MASK = 0xFFFFFFFFFFFFFFFF

# The teardown's graces: how long in-flight handlers get before what is left is
# killed or cancelled, and then how long the one client build (never cancelled)
# gets before it is abandoned. Module attributes so a test can shorten them.
_HANDLER_DRAIN_GRACE = 6.0
_BUILD_DRAIN_GRACE = 6.0

# Serialized once, at import, so the path that answers a failed serialization
# never has to serialize anything itself.
_LAST_RESORT = _internal.SmartPickle.dumps(
    _internal.RemoteError("UnknownError: <unserializable>", "UnknownError")
)


def safe_dumps(data: Any, ok: bool) -> tuple[bytes, bool]:
    """Serialize data, falling back to wrapped error on failure.

    BaseException too, for the reason the client call already catches it: this
    is the last step before a reply goes on the wire. A ``__reduce__`` raising
    one otherwise kills the handler with nothing sent, stranding the caller
    until its own grace expires; in the asyncio worker a SystemExit escaping a
    task takes the whole worker down with it.

    The cost is knowingly paid: a SIGINT delivered during these few
    instructions is indistinguishable from one a hostile reducer raised, so it
    becomes an error reply instead of stopping a ``serve()`` host. That window
    is one serialization call and the operator's next Ctrl-C still lands —
    against a worker that dies mid-reply for every client.
    """
    try:
        return _internal.SmartPickle.dumps(data), ok
    except BaseException as exc:
        err = _internal.wrap_exception(exc, traceback.format_exc())
        try:
            return _internal.SmartPickle.dumps(err), False
        except BaseException:  # noqa: BLE001
            # wrap_exception proves the error pickles ONCE — its probe — and
            # this is a second call: a reducer that answers differently, or a
            # MemoryError on a large error object, escaped from here with the
            # first reply's guard already behind it. A constant serialized at
            # import is the one reply that cannot fail to be one.
            return _LAST_RESORT, False


def safe_close(client: Any) -> None:
    """Safely call client.close() if it exists.

    SystemExit as well: a client calling sys.exit(2) in its own cleanup is that
    client's decision, and under serve() it would otherwise end a host that had
    already released everything. KeyboardInterrupt is the operator's and is not
    in the list.

    The lookup is inside the guard, not before it: a __getattr__ or a property
    runs the client's code just as close() does, and a SystemExit from there
    used to leave serve() by a route the call itself no longer could.
    """
    # try/except, not suppress: the guard is an allocation, and this runs on
    # teardown paths — see ZmqTransport.close for the rule.
    try:
        if close := getattr(client, "close", None):
            close()
    except (Exception, SystemExit):
        pass


def _malformed(exc: BaseException) -> Exception:
    """Wrap a request-parse failure as a serializable error response."""
    try:
        detail = repr(exc)
    except BaseException:  # noqa: BLE001
        # __repr__ is the client's code as much as __str__ is, and this runs
        # in the request loop: letting it out ends the worker.
        detail = "<unprintable>"
    return _internal.wrap_exception(
        ValueError(f"malformed request: {detail}"), traceback.format_exc()
    )


def gevent_worker(cfg: WorkerConfig, patch_kwargs: dict):
    """Gevent-based worker with greenlet concurrency."""
    import gevent.monkey

    gevent.monkey.patch_all(**patch_kwargs)

    # imported after patch_all so they pick up patched stdlib
    import dill
    import gevent
    import gevent.lock
    import gevent.pool
    import zmq
    import zmq.green as zmq_green

    gevent.get_hub()

    # Everything that can fail is built before the transport exists — the
    # loads, the locks, and every nested function object below (each def is an
    # allocation of its own) — so no failure here can strand a bound socket
    # outside the cleanup at the bottom: serve() runs this in a process that
    # survives the exception. The defs close over ``sock`` and read it at call
    # time, which is after the open.
    factory = dill.loads(cfg.factory_bytes)
    client = None
    client_lock = gevent.lock.RLock()
    # The teardown's admission fence. kill() is not one: a handler parked on
    # client_lock whose wake was queued by the release BEFORE the kill batch
    # ran resumes ahead of its own GreenletExit, passes the deadline check,
    # and starts its call after the grace expired (measured: the wake and the
    # throw are both hub callbacks, FIFO). Set between the join giving up and
    # the kill, read at _invoke's yield-free admission points.
    stopping = False
    send_lock = gevent.lock.Semaphore()
    # Unbounded group + explicit slot semaphore: admission happens inside
    # handle() with a deadline, so saturation can never block _drain.
    handlers = gevent.pool.Group()
    slots = gevent.lock.Semaphore(cfg.max_concurrency) if cfg.max_concurrency else None

    def send(identity: bytes, req_id: bytes, ok: bool, data: Any):
        resp, ok = safe_dumps(data, ok)
        with send_lock:
            with contextlib.suppress(zmq.ZMQError):
                sock.send_multipart([identity, req_id, OK if ok else ERR, resp])

    def _invoke(method: str, args: tuple, kwargs: dict, deadline: float):
        # Returns (ok, payload) instead of raising: an exception escaping
        # this greenlet would make gevent print an unhandled-greenlet
        # traceback to stderr for every client error, and handle() would
        # never send a reply. GreenletExit (kill, or client-raised) must
        # still propagate as the greenlet's outcome.
        nonlocal client
        try:
            with client_lock:
                if stopping:
                    # The fence: our wake was queued ahead of the kill's
                    # GreenletExit, but the grace is over. This is that kill,
                    # taken one wake early — before a build, not after.
                    raise gevent.GreenletExit("worker stopping")
                if client is None:
                    # An expired request must not trigger one-time client init.
                    if time.monotonic() >= deadline:
                        raise TimeoutError(f"{method} timed out")
                    client = factory()
            # The lookup comes BEFORE the fence: it is client code — a lazy
            # proxy's __getattribute__ can yield — and a yield between the
            # fence and the call would reopen the window the fence closes.
            fn = getattr(client, method)
            # Admission re-checked at the last yield-free instant before the
            # client call: hub-callback backlog, client_lock contention, a
            # slow factory() or attribute lookup can delay this greenlet past
            # the deadline even when the spawning handler saw budget
            # remaining — or past the teardown's fence, which no deadline
            # check notices.
            if stopping:
                raise gevent.GreenletExit("worker stopping")
            if time.monotonic() >= deadline:
                raise TimeoutError(f"{method} timed out")
            return True, fn(*args, **kwargs)
        except gevent.GreenletExit:
            raise
        except KeyboardInterrupt:
            # The operator's, not the client's. Measured: a real SIGINT is
            # raised in whatever greenlet is running on the main OS thread, so
            # under serve() it lands HERE — and answering it with a reply is how
            # Ctrl-C stops working on the host.
            raise
        except BaseException as e:
            # BaseException too: a client's own escaping gevent.Timeout is one,
            # and letting it kill this greenlet costs the caller a reply.
            return False, _internal.wrap_exception(e, traceback.format_exc())

    def handle(
        identity: bytes,
        req_id: bytes,
        method: str,
        args: tuple,
        kwargs: dict,
        deadline: float,
    ):
        try:
            # Budget from request-accept time so time spent waiting for a
            # slot counts against the timeout (else a queued call can run
            # after the caller already gave up).
            remaining = deadline - time.monotonic()
            if remaining <= 0 or (slots is not None and not slots.acquire(timeout=remaining)):
                raise TimeoutError(f"{method} timed out")
            # The client call runs on its own greenlet: a deadline raised
            # into client code can be swallowed by retry-on-Exception loops,
            # but kill()'s GreenletExit is a BaseException no such loop can
            # catch (a client catching BaseException leaks its slot until
            # restart — unbeatable). The slot is released only when the
            # greenlet is truly dead — kill(block=False) leaves the call
            # unwinding, and freeing the slot before that breaks
            # max_concurrency. Spawned into handlers so shutdown's join
            # waits for unwinding calls, not just their handle greenlets.
            # (rawlink runs in the hub; Semaphore.release never blocks.)
            try:
                g = handlers.spawn(_invoke, method, args, kwargs, deadline)
            except BaseException:
                # The slot is released by the link, and here there is no
                # greenlet to link to: a spawn the hub refuses would otherwise
                # consume a slot for the life of the worker, and with
                # max_concurrency=1 no call ever runs again.
                if slots is not None:
                    slots.release()
                raise
            if slots is not None:
                release = slots.release  # bind: pyright can't narrow `slots` inside the lambda
                try:
                    g.rawlink(lambda _g: release())
                except BaseException:
                    # The spawn took, so _invoke is scheduled and WILL run —
                    # releasing alone let a second call in beside it, and the
                    # error reply below claimed a call had failed while its
                    # side effects landed anyway. It has not started yet, so
                    # the kill keeps it from ever running, and both stay true.
                    # The release is in a finally: the kill can itself refuse
                    # under the same pressure, and skipping the release wedged
                    # max_concurrency=1 for the life of the worker.
                    try:
                        g.kill(block=False)
                    finally:
                        slots.release()
                    raise
            g.join(max(deadline - time.monotonic(), 0.0))
            if not g.ready():
                g.kill(block=False)
                raise TimeoutError(f"{method} timed out")
            result = g.get(block=False)
            if isinstance(result, gevent.GreenletExit):
                # gevent counts GreenletExit as success; a client raising
                # it must not surface as an OK result.
                raise RuntimeError(f"{method} killed: {result}")
            ok, payload = result
            send(identity, req_id, ok, payload)
        except Exception as e:
            send(identity, req_id, False, _internal.wrap_exception(e, traceback.format_exc()))

    def _drain() -> bool:
        """Drain all available messages. Returns False on shutdown."""
        while True:
            try:
                parts = sock.recv_multipart(zmq.NOBLOCK)
            except zmq.Again:
                return True
            if len(parts) < 3:
                continue
            identity, req_id, payload = parts[:3]
            if payload == SHUTDOWN:
                return False
            try:
                method, args, kwargs, deadline = _internal.SmartPickle.loads(payload)
            except KeyboardInterrupt:
                # The operator's. Answering it as a malformed request is how
                # Ctrl-C stops working on a serve() host.
                raise
            except gevent.GreenletExit:
                # A kill aimed at this loop, not a bad request. kill() blocks by
                # default, so answering it with an error reply and carrying on
                # leaves the stopper waiting on a greenlet that no longer means
                # to exit. Same precedent as _invoke below.
                raise
            # BaseException otherwise: unpickling runs client code, and a reduce
            # callable raising SystemExit here would end the worker — every
            # client of it, not just the one that sent the request.
            except BaseException as e:
                send(identity, req_id, False, _malformed(e))
                continue
            try:
                handlers.spawn(handle, identity, req_id, method, args, kwargs, deadline)
            except (KeyboardInterrupt, gevent.GreenletExit):
                # The operator's, and a kill aimed at this loop: both stop the
                # worker, not the request.
                raise
            except BaseException as e:
                # One request's failure, not the worker's: a spawn the hub
                # refuses used to unwind this loop and close the transport —
                # under serve() a host every attached process shares. Same
                # boundary as ProcessBridge._serve.
                send(identity, req_id, False, _internal.wrap_exception(e, traceback.format_exc()))

    transport = _internal.ZmqTransport.open(
        zmq_green.Context, zmq_green.ROUTER, cfg.ipc_addr, bind=True
    )
    sock = transport.sock
    try:
        while True:
            if sock.poll(500) and not _drain():
                break
    finally:
        # One nest, so the transport release is what every path ends at: an
        # enclosing gevent.Timeout, or a host killing the greenlet it ran
        # serve() in, lands in the join below — and under serve() no process
        # exit follows to reclaim what that skips.
        try:
            handlers.join(timeout=_HANDLER_DRAIN_GRACE)
        finally:
            # Before the kill, so a wake the kill batch cannot outrun reads it.
            stopping = True
            # Then stop whatever is left. A spawned child exits right after this
            # and takes its stragglers with it, but serve() runs this loop in a
            # process that survives it: a handler still running there holds the
            # old client and goes on producing side effects for a host that has
            # already moved on. Same shape as ProcessBridge._serve.
            # Blocking, briefly: kill() only SCHEDULES the GreenletExit, so
            # closing the client in the next statement ran the client's close()
            # while a handler was still unwinding through its own finally — with
            # the object being closed. One second, measured not to raise on
            # expiry, and in a try so a straggler that ignores the kill cannot
            # cost the transport.
            try:
                handlers.kill(block=True, timeout=1)
            finally:
                try:
                    # A client's own expiring gevent.Timeout is not safe_close's
                    # to swallow. It is ours not to leak the transport over.
                    safe_close(client)
                finally:
                    transport.close()


def asyncio_worker(cfg: WorkerConfig):
    """Asyncio-based worker for async clients."""
    import asyncio
    import functools
    import inspect

    import dill
    import zmq
    import zmq.asyncio

    factory = dill.loads(cfg.factory_bytes)
    build: Any = None  # the one client build: in flight, or finished
    send_lock = asyncio.Lock()
    sem: asyncio.Semaphore | None = (
        asyncio.Semaphore(cfg.max_concurrency) if cfg.max_concurrency else None
    )
    tasks: set[asyncio.Task] = set()

    async def off_loop(call):
        """Run one user callable without wedging the loop thread.

        Every call in flight shares that thread — under serve(), so does every
        attached process — so a synchronous hook that blocks it stops all of
        them, their deadlines included. Awaited if it is a coroutine function,
        otherwise run on the default executor; and awaited again if what came
        back is itself awaitable, since a sync wrapper around an ``async def``
        (what a decorator written for sync code leaves behind) is not a
        coroutine function but returns a coroutine.

        The executor thread is not ours to stop. Expiring the deadline cancels
        only the awaiting task: the thread runs to completion, so its side
        effects can land after the caller was told it timed out, and the slot it
        gave back on the way out lets a second call into a client the first has
        not finished with. Bounding that would take an executor of our own to
        join, which is a cost every correct client would pay for the incorrect
        ones; ``max_concurrency`` bounds awaiting handlers here, and a client
        whose sync methods must not overlap needs its own lock. The gevent
        worker has no such gap — kill() lands inside client code at its next
        switch.
        """
        if inspect.iscoroutinefunction(call):
            return await call()
        result = await asyncio.get_running_loop().run_in_executor(None, call)
        return await result if inspect.isawaitable(result) else result

    async def _dispose(c):
        """Best-effort client close; awaits an async close.

        SystemExit as well, for safe_close's reason. KeyboardInterrupt is the
        operator's and is not in the list.
        """
        try:
            if (close := getattr(c, "close", None)) is not None:
                await off_loop(close)
        except (Exception, SystemExit):
            pass

    async def _build():
        """Construct and connect the one client this worker serves.

        Never cancelled while the worker runs, so nothing it builds is ever
        abandoned: a caller's deadline gives up its own wait through the shield
        in :func:`get_client`, not this. The worker owns initialisation, not the
        call that happened to arrive first — a caller losing interest does not
        make the client it was waiting for invalid, and a late but successful
        one is the client every later call wants.

        The only client this closes is one whose own ``connect()`` failed, and
        it closes it after connect has RETURNED: ``off_loop`` runs a synchronous
        connect on an executor thread that cancellation cannot stop, so closing
        earlier could precede the acquisition it meant to release.

        The cost, alongside the ones ``off_loop`` names: an initialisation that
        hangs is no longer abandoned for a later call to retry — every caller
        times out on the same build.
        """
        try:
            return await _build_once()
        except KeyboardInterrupt:
            raise  # the operator's — see gevent_worker._invoke
        except SystemExit as exc:
            # Raised inside a task, this is re-raised into the event loop
            # by Task.__step and unwind asyncio.run — killing the worker, and
            # under serve() every attached client with it. A client library
            # calling sys.exit(2) over its own bad configuration is that
            # client's failure, not the host's, so it becomes a reply like any
            # other. handle() already does this for the call itself; the build
            # is a task of its own and had no such boundary.
            raise _internal.wrap_exception(exc, traceback.format_exc()) from None

    async def _build_once():
        c = await off_loop(factory)
        try:
            if (connect := getattr(c, "connect", None)) is not None:
                await off_loop(connect)
        except asyncio.CancelledError:
            # Cancelled from outside — teardown, once the join below gives up —
            # rather than a connect that failed. off_loop runs a synchronous
            # connect where cancellation does not reach, so closing here could
            # precede the acquisition it means to release, and nothing detached
            # would run instead: the loop is on its way out. The client such a
            # connect goes on to open is lost, which is the cost off_loop
            # already names. A CancelledError merely LEAKED by client code is
            # not this: no cancellation was requested of us, so it falls
            # through to the failed-connect path below.
            task = asyncio.current_task()
            if task is None or task.cancelling():
                raise
            try:
                await _dispose(c)
            except KeyboardInterrupt:
                raise  # the operator's — everything else stays the rollback's
            except BaseException:  # noqa: BLE001
                pass
            raise
        except BaseException:
            # The initialisation failure is what the caller is owed; a close
            # that fails on top of it must not take its place.
            try:
                await _dispose(c)
            except KeyboardInterrupt:
                raise  # the operator's
            except BaseException:  # noqa: BLE001
                pass
            raise
        return c

    def _forget_failure(done: Any) -> None:
        """Let the next call retry a build that failed.

        Identity-guarded so an older completion can never clear a newer build.
        """
        nonlocal build
        if build is done and (done.cancelled() or done.exception() is not None):
            build = None

    async def get_client():
        """The one client, built once and shared.

        No lock: asyncio cannot suspend between the test below and the
        assignment that follows it, so two calls in the same tick cannot both
        start a build — and an eager task factory only means the build may
        already be finished when it is assigned.
        """
        nonlocal build
        if build is None:
            build = asyncio.ensure_future(_build())
            tasks.add(build)
            build.add_done_callback(tasks.discard)
            build.add_done_callback(_forget_failure)
        # Shielded: an expiring deadline gives up its own wait. It does not
        # cancel the build that every other call is waiting for.
        return await asyncio.shield(build)

    async def send(sock, identity: bytes, req_id: bytes, ok: bool, data: Any):
        resp, ok = safe_dumps(data, ok)
        async with send_lock:
            with contextlib.suppress(zmq.ZMQError):
                await sock.send_multipart([identity, req_id, OK if ok else ERR, resp])

    async def _call(method: str, args: tuple, kwargs: dict):
        c = await get_client()
        fn = getattr(c, method)
        return await off_loop(functools.partial(fn, *args, **kwargs))

    async def handle(
        sock,
        identity: bytes,
        req_id: bytes,
        method: str,
        args: tuple,
        kwargs: dict,
        deadline: float,
    ):
        ok, result = False, None
        tm = None
        try:
            # The deadline bounds the slot wait as well as the call. A client
            # that swallows its cancellation keeps its slot for good (as
            # unbeatable here as in the gevent worker), but a handler blocked
            # on an unbounded acquire is worse than slow: it never sends the
            # TimeoutError its caller is owed, and the queued tasks sit on
            # their payloads until the process restarts.
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError
            async with asyncio.timeout(remaining) as tm:
                async with sem if sem else contextlib.nullcontext():
                    result = await _call(method, args, kwargs)
                    ok = True
        except TimeoutError as e:
            # A client-raised TimeoutError is indistinguishable by type from
            # our deadline (asyncio.TimeoutError IS TimeoutError since 3.10);
            # tm.expired() tells them apart so the client's message survives.
            if tm is not None and not tm.expired():
                result = _internal.wrap_exception(e, traceback.format_exc())
            else:
                result = TimeoutError(f"{method} timed out")
        except Exception as e:
            result = _internal.wrap_exception(e, traceback.format_exc())
        except KeyboardInterrupt:
            raise  # the operator's — see gevent_worker._invoke
        except asyncio.CancelledError as e:
            # Cancellation aimed at *this* task (loop shutdown) must stay
            # cancelled and send nothing. A CancelledError merely leaked by
            # client code — an inner cancelled await escaping the method —
            # leaves no cancellation request here, so it is an ordinary
            # failed call; letting it kill this task would send no reply at
            # all and strand the caller until its own grace period.
            task = asyncio.current_task()
            if task is None or task.cancelling():
                raise
            result = _internal.wrap_exception(e, traceback.format_exc())
        except BaseException as e:
            result = _internal.wrap_exception(e, traceback.format_exc())
        await send(sock, identity, req_id, ok, result)

    async def main():
        transport = _internal.ZmqTransport.open(
            zmq.asyncio.Context, zmq.ROUTER, cfg.ipc_addr, bind=True
        )
        sock = transport.sock
        try:
            # Inside the try: a poller that fails to build or register would
            # otherwise leave the transport it was for open, on a serve() host
            # that survives the failure.
            poller = zmq.asyncio.Poller()
            poller.register(sock, zmq.POLLIN)
            while True:
                if not await poller.poll(1000):
                    continue
                parts = await sock.recv_multipart()
                if len(parts) < 3:
                    continue
                identity, req_id, payload = parts[:3]
                if payload == SHUTDOWN:
                    break
                try:
                    method, args, kwargs, deadline = _internal.SmartPickle.loads(payload)
                except KeyboardInterrupt:
                    raise  # the operator's — see the gevent worker's _drain
                except BaseException as e:  # see the gevent worker's _drain
                    await send(sock, identity, req_id, False, _malformed(e))
                    continue
                coro = handle(sock, identity, req_id, method, args, kwargs, deadline)
                try:
                    task = asyncio.create_task(coro)
                except (KeyboardInterrupt, asyncio.CancelledError):
                    # The operator's, and a cancellation aimed at this loop:
                    # both stop the worker, not the request.
                    coro.close()
                    raise
                except BaseException as e:
                    # One request's failure, not the worker's — an installed
                    # task factory can refuse. Closed, or the never-started
                    # coroutine warns at collection; then answered, same
                    # boundary as the gevent worker's _drain.
                    coro.close()
                    await send(
                        sock, identity, req_id, False,
                        _internal.wrap_exception(e, traceback.format_exc()),
                    )
                    continue
                tasks.add(task)
                task.add_done_callback(tasks.discard)

            # Drained in a loop, not one snapshot: asyncio.wait fixes its set
            # at the call, and finishing tasks make more.
            # A grace, not a bound on returning: asyncio.run
            # cancels whatever is left and then waits for it with no timeout of
            # its own, so a handler that swallows its cancellation holds serve()
            # open for as long as it likes — the same cooperative limit off_loop
            # names for executor work, and unfixable in-process. A spawned
            # worker has the bound its parent's terminate/kill gives it.
            drain_until = time.monotonic() + _HANDLER_DRAIN_GRACE
            while tasks and (remaining := drain_until - time.monotonic()) > 0:
                await asyncio.wait(set(tasks), timeout=remaining)
        finally:
            try:
                # Waited for, not inspected: off_loop's executor work does not
                # stop when its await is cancelled, so letting asyncio.run
                # cancel a pending build would lose the client its thread goes
                # on to return, or begin closing one whose connect is still
                # running.
                #
                # Bounded, and this one really is a bound: the build is never
                # cancelled, so an ordinary async factory waiting on a network
                # that never arrives is not the swallow-your-cancellation case
                # above — nothing would ever end it, and serve() would never
                # return. A second grace beyond the drain's, after which the client
                # such a build may still produce is lost, which is the same cost
                # off_loop already names for work it cannot stop.
                pending = build
                if pending is not None:
                    # Handlers are cancelled BEFORE the build wait, not after:
                    # a handler parked on the shield had its wake registered
                    # when it first awaited — before the wait below registers
                    # its own — so when the build completes, that handler
                    # resumes AHEAD of us, and off_loop SUBMITS its call to
                    # the executor before the await where a later cancel
                    # could land. Measured: it runs the very call the
                    # shutdown exists to prevent, into an object the dispose
                    # below is about to close. Cancelled here, the pending
                    # wake delivers CancelledError instead of the client, and
                    # the build itself — never in this set once discarded —
                    # runs on to be disposed.
                    tasks.discard(pending)
                    while tasks:
                        # Destructive pop, no list copy: the copy was an
                        # allocation, and its refusal skipped both the cancels
                        # and the dispose below. Nothing runs between pops —
                        # done-callbacks only fire at the next await.
                        tasks.pop().cancel()
                    if not pending.done():
                        # wait(), not await: it neither cancels nor raises —
                        # and while it parks, the cancelled handlers get the
                        # loop to unwind through their own finallys.
                        await asyncio.wait({pending}, timeout=_BUILD_DRAIN_GRACE)
                    if (
                        pending.done()
                        and not pending.cancelled()
                        and pending.exception() is None
                    ):
                        # _dispose suppresses Exception only, and an async
                        # close() that leaks a CancelledError is not an
                        # Exception — see the gevent worker's teardown for why
                        # the transport goes either way.
                        await _dispose(pending.result())
            finally:
                transport.close()

    asyncio.run(main())
