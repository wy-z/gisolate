"""ProcessBridge: ZMQ-based RPC bridge for cross-process function calls."""

import asyncio
import contextlib
import enum
import itertools
import logging
from typing import Any, Callable

import gevent

from . import _internal, _workers

log = logging.getLogger(__name__)


class ProcessBridge:
    """ZMQ-based RPC bridge for cross-process function calls.

    Server mode: Listens for requests, executes functions locally (gevent).
    Client mode: Sends requests to server, awaits results (asyncio).

    One bridge belongs to one thread. ``start()`` and ``close()`` carry no
    synchronization, so a ``close()`` racing a ``start()`` across two native
    threads can leave the closer holding the generation the starter published —
    and no ordering of those two fixes it without a lock. That lock is not
    offered because it would only be half of one: the ZMQ socket underneath is
    not thread-safe either, and neither is the gevent greenlet nor the asyncio
    task that drive it, so a bridge shared across threads is already outside
    what any flag here could rescue. Concurrent ``call()`` from many coroutines
    on the owning thread is supported and is what the send lock is for; see
    :class:`gisolate.ProcessProxy` when calls must come from several threads.

    Args:
        address: IPC address (e.g., "ipc:///tmp/rpc.sock").
        mode: ProcessBridge.Mode.SERVER or ProcessBridge.Mode.CLIENT — or the
            string either one is worth, since the enum accepts it.
    """

    class Mode(enum.StrEnum):
        SERVER = "server"
        CLIENT = "client"

    def __init__(self, address: str, mode: "ProcessBridge.Mode | str"):
        self._addr = address
        # Normalised, because every dispatch below compares by identity: a
        # plain ``"client"`` would otherwise fall through to the server branch
        # and BIND — taking over the address a real server holds — and then
        # fail inside call() on a send lock that was never created.
        self._mode = ProcessBridge.Mode(mode)
        self._started = False
        self._req_id = itertools.count()
        self._pending: dict[bytes, Any] = {}
        self._reader_task: Any = None
        self._server_greenlet: gevent.Greenlet | None = None
        self._send_lock: Any = None
        self._loop: Any = None  # the loop the client's lock and reader belong to
        self._transport: _internal.ZmqTransport | None = None

    def __del__(self):
        # try/except, not suppress: the guard is an allocation, and this is a
        # cleanup path — see ZmqTransport.close for the rule.
        try:
            if getattr(self, "_started", False):
                self.close()
        except Exception:
            pass

    @property
    def address(self) -> str:
        """IPC address."""
        return self._addr

    def start(self) -> "ProcessBridge":
        """Start the bridge. Idempotent. Returns self for chaining."""
        if self._started:
            return self
        if self._mode is ProcessBridge.Mode.CLIENT:
            self._start_client()
        else:
            self._start_server()
        return self

    async def call(self, func: Callable, *args, timeout: float = 60.0, **kwargs) -> Any:
        """Execute func on server. Starts client connection if needed.

        Safe for concurrent calls from multiple coroutines.
        """
        if self._mode is ProcessBridge.Mode.SERVER:
            raise RuntimeError("Cannot call() in server mode")
        loop = asyncio.get_running_loop()
        if not self._started or self._transport is None:
            self._start_client()
        elif self._loop is not loop:
            # A second asyncio.run(), and nothing of the old generation carries
            # over. pyzmq migrates the FD watcher but keeps ONE receive queue,
            # so the previous reader's outstanding recv stays ahead of any new
            # one and the reply is handed to a future belonging to a loop that
            # will never run again — the call here times out although the server
            # answered. Cancelling that reader does not help: its cancellation
            # would be scheduled on the stopped loop. asyncio.Lock does not
            # migrate either. Closing the transport is what clears the queue, so
            # the client is rebuilt whole.
            self.close()
            self._start_client()
        elif self._reader_task is None or self._reader_task.done():
            self._reader_task = loop.create_task(
                self._read_responses(self._transport)
            )

        req_id = (next(self._req_id) & _workers.ID_MASK).to_bytes(8)
        fut: asyncio.Future[tuple[bytes, bytes]] = (
            asyncio.get_running_loop().create_future()
        )
        self._pending[req_id] = fut

        # Bound to THIS generation's transport, like _serve's. Under DEALER
        # backpressure a send waits on the lock; a close()+start() in between
        # has already told us our call failed, and reading self._transport at send
        # time would put the callable on the new socket anyway — the server
        # running it while we report a ConnectionError.
        transport, send_lock = self._transport, self._send_lock
        try:
            # The budget covers the send, not just the wait for a reply. A
            # DEALER whose peer is absent queues until its high-water mark and
            # then blocks in send_multipart — behind which every later call
            # queues on the lock — so a timeout starting after the send bounded
            # nothing in exactly the case it was for.
            async with asyncio.timeout(timeout):
                async with send_lock:
                    # Re-checked inside the lock, because waiting for it is
                    # where a close() lands: it detaches the generation and
                    # releases the socket with it, so what we captured is a
                    # transport whose sock is now None. The ConnectionError
                    # close() already put on our reply future is the answer
                    # this call is owed — nothing will read that future now, so
                    # raise the same thing here.
                    if transport is None or transport is not self._transport:
                        raise ConnectionError("Bridge closed")
                    await transport.sock.send_multipart(
                        [req_id, _internal.SmartPickle.dumps((func, args, kwargs))]
                    )
                status, payload = await fut
        except TimeoutError:
            raise TimeoutError(f"Timed out after {timeout}s") from None
        except asyncio.CancelledError:
            # close() cancels the socket's pending send futures (pyzmq does it
            # in Socket.close), so a call waiting out DEALER backpressure is
            # cancelled by a bridge going away under it — while the
            # ConnectionError close() put on its reply future goes unread.
            # Cancellation aimed at OUR task must stay cancellation; the same
            # distinction the asyncio worker draws, and by the same means —
            # including its limit, which is asyncio's own: a caller that
            # swallowed an earlier cancellation without calling uncancel()
            # still counts as cancelled here, and gets the CancelledError this
            # would otherwise have replaced.
            task = asyncio.current_task()
            if task is None or task.cancelling():
                raise
            raise ConnectionError("Bridge closed") from None
        finally:
            self._pending.pop(req_id, None)

        try:
            result = _internal.SmartPickle.loads(payload)
        except KeyboardInterrupt:
            # The operator's, arriving while a legitimate reply is rebuilt.
            # Reporting it as this one call's failure absorbs Ctrl-C for the
            # whole application.
            raise
        except BaseException as e:
            # Reconstruction runs the sender's code — a __setstate__, a reduce
            # callable — so a reply whose unpickling raises SystemExit used to
            # leave here as the CALLER's exit, past every `except Exception` it
            # has. One bad reply is one failed call. `from None` because the
            # cause is that same hostile object, and rendering it would run its
            # __str__ in whatever prints the traceback.
            log.warning("Failed to deserialize reply", exc_info=True)
            raise _internal.ProcessError(
                f"Bad response: {_internal.type_name(e)}"
            ) from None
        if status != _workers.OK:
            if tb := _internal.remote_traceback(result):
                log.error(f"Remote traceback:\n{tb}")
            raise result
        return result

    async def _read_responses(self, transport: _internal.ZmqTransport) -> None:
        """Single reader task: dispatch responses to pending futures.

        Bound to the transport it started on, like every other loop here: a
        close+start in between must not leave this one recv-ing on the socket
        the new generation just published.
        """
        sock = transport.sock
        try:
            while True:
                if transport is not self._transport:
                    return
                parts = await sock.recv_multipart()
                if len(parts) < 3:
                    continue
                resp_id, status, payload = parts[:3]
                if (fut := self._pending.get(resp_id)) and not fut.done():
                    fut.set_result((status, payload))
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.warning(f"Bridge reader task died: {e}", exc_info=True)
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(e)
            self._pending.clear()

    def _start_server(self):
        """Initialize server (gevent ROUTER socket)."""
        import zmq.green as zmq_mod

        transport = _internal.ZmqTransport.open(
            zmq_mod.Context, zmq_mod.ROUTER, self._addr, bind=True
        )
        # Published only once the transport is really ours: close() and __del__
        # both gate teardown on ``_started``, so a bridge that set it first
        # would, after a failed setup, tear down state it never acquired — and
        # skip the retry, since close() flips the flag on its way out.
        self._transport = transport
        self._started = True
        try:
            self._server_greenlet = gevent.spawn(self._serve, transport)
        except BaseException:
            # A spawn can refuse. Nothing may stay claimed: start() would no-op
            # on the retry, over a bound socket with no server behind it — the
            # same rollback the client's task creation has.
            self._started = False
            self._transport = None
            transport.close()
            raise

    def _start_client(self):
        """Initialize client (asyncio DEALER socket + reader task)."""
        import zmq.asyncio

        # Demanded before anything is allocated, as the publisher does: on 3.12
        # and 3.13 ensure_future does NOT refuse a caller with no running loop —
        # it puts the reader on a dormant policy loop, which never runs it, so a
        # live server's replies are never consumed and every call times out.
        loop = asyncio.get_running_loop()

        # The lock is built BEFORE the transport: it is an allocation, and a
        # refusal after the open published the transport left it live behind
        # _started=False — close() and __del__ skipped it, and a retried
        # start() overwrote the only reference.
        send_lock = asyncio.Lock()
        transport = _internal.ZmqTransport.open(
            zmq.asyncio.Context, zmq.DEALER, self._addr, bind=False
        )
        self._transport = transport
        self._send_lock = send_lock
        try:
            # Published first: with an eager task factory the reader runs inside
            # create_task and must already see the transport. If it raises
            # instead — an installed factory can refuse — nothing may stay
            # claimed, or start() would no-op on the retry over a live
            # transport nobody can reach.
            reader = loop.create_task(self._read_responses(transport))
        except BaseException:
            self._transport = self._send_lock = None
            transport.close()
            raise
        self._started = True
        self._reader_task = reader
        self._loop = loop

    def _serve(self, transport: _internal.ZmqTransport):
        """Server loop: dispatch each request to a greenlet for concurrency."""
        try:
            import gevent.lock
            import gevent.pool
            import zmq.green as zmq_mod

            group = gevent.pool.Group()
            send_lock = gevent.lock.Semaphore()
            # Bound to THIS generation. A handler that outlives close() —
            # client code catching the GreenletExit below can — would otherwise
            # send on whatever socket a later start() installed, holding a send
            # lock that generation's handlers know nothing about.
            sock = transport.sock

            def _handle(identity: bytes, req_id: bytes, payload: bytes) -> None:
                import traceback

                try:
                    func, args, kwargs = _internal.SmartPickle.loads(payload)
                    data = func(*args, **kwargs)
                    ok = True
                except KeyboardInterrupt:
                    # The operator's, not the bridged function's. Measured: a
                    # real SIGINT is raised in whatever greenlet runs on the
                    # main OS thread, so it lands here rather than in the main
                    # greenlet.
                    raise
                # BaseException too: a bridged function raising GreenletExit
                # ends this greenlet the way a normal return does, so the
                # caller was told nothing at all and waited out its own timeout
                # instead.
                except BaseException as exc:
                    data = _internal.wrap_exception(exc, traceback.format_exc())
                    ok = False
                resp, ok = _workers.safe_dumps(data, ok)
                with send_lock:
                    with contextlib.suppress(zmq_mod.ZMQError):
                        sock.send_multipart(
                            [identity, req_id, _workers.OK if ok else _workers.ERR, resp]
                        )
        except BaseException:
            # The prelude allocates — a group, a semaphore, the handler's own
            # function object — and a refusal here died with _started still
            # true over the bound transport: start() no-opped for good. Same
            # release the loop's own failure gets, allocating nothing on the
            # way out.
            if transport is self._transport:
                self._started = False
                self._transport = None
                transport.close()
            raise

        reclaim = False
        try:
            # Bound like every other loop in this package: it stops when it
            # stops being the published generation. close() detaches
            # ``_transport`` before it yields, so no old loop can outlive its
            # closer — and no shared flag is needed that a concurrent start()
            # could clear under it.
            while transport is self._transport:
                if not sock.poll(100):
                    continue
                parts: list[bytes] = sock.recv_multipart()  # type: ignore[assignment]
                if len(parts) < 3:
                    continue
                identity, req_id, payload = parts[:3]
                if payload == _workers.SHUTDOWN:
                    break
                try:
                    group.spawn(_handle, identity, req_id, payload)
                except (KeyboardInterrupt, gevent.GreenletExit):
                    # The operator's, and close()'s kill: both are aimed at this
                    # loop, not at the request.
                    raise
                except BaseException as exc:
                    # One request's failure, not the server's: a spawn the hub
                    # refuses used to end this loop while _started stayed true
                    # over a bound socket, so start() no-opped and every later
                    # caller timed out. The caller of THIS one is owed an answer
                    # like any other — under the send lock, because a handler
                    # may be mid-send: a green multipart send yields between
                    # frames, and interleaving ours corrupts both replies.
                    resp, _ok = _workers.safe_dumps(
                        _internal.wrap_exception(exc), False
                    )
                    with send_lock:
                        with contextlib.suppress(zmq_mod.ZMQError):
                            sock.send_multipart(
                                [identity, req_id, _workers.ERR, resp]
                            )
        except BaseException:
            # A real mid-serve failure — a ZMQError out of poll, an allocation
            # refused while receiving — and nothing will serve this generation
            # again. Left claimed, start() no-ops over a bound socket and every
            # later caller times out; the claim is released so a retry can
            # rebuild, while the raise stays loud in gevent's unhandled-greenlet
            # report. Guarded, because close()'s kill lands after IT detached
            # the generation: the teardown is then the closer's, not ours.
            if transport is self._transport:
                reclaim = True
                self._started = False
                self._transport = None
            raise
        finally:
            try:
                group.join(timeout=6)
            finally:
                # Then stop whatever is left. Unlike the worker loops, this
                # greenlet is not followed by the process exiting: close()
                # returns to a caller that considers the bridge done, and a
                # handler still looping would go on running its side effects for
                # the life of the process. Nested, because close() kills this
                # greenlet after two seconds — cutting the join short is exactly
                # the case where handlers are still running. Blocking, briefly:
                # kill() only SCHEDULES the GreenletExit, so close() could
                # return — and its caller move on — while a handler was still
                # unwinding through its own finally. Same shape as the gevent
                # worker's teardown; measured not to raise on expiry.
                try:
                    group.kill(block=True, timeout=1)
                finally:
                    if reclaim:
                        transport.close()

    def close(self):
        """Cleanup resources. Idempotent."""
        if not self._started:
            return
        # Every allocation first — the replacement dict AND the error the
        # pending futures get — before the claim below, let alone the
        # detaches: raising after either left a live transport behind a flag
        # already cleared — no later close() or __del__ would come back for
        # it — or detached futures whose callers waited out their timeouts.
        fresh: dict[bytes, Any] = {}
        closed_error = ConnectionError("Bridge closed")
        # The waiter list too: iterating the detached dict builds a view and
        # an iterator, and a refusal there left the old futures uncompleted —
        # their callers waiting out full RPC timeouts. Nothing new can join
        # _pending between here and the detach below: this thread owns the
        # loop, and there is no await in between.
        waiters = list(self._pending.values())
        # Claimed before anything is torn down, not after: joining the server
        # greenlet switches, and a second closer that got in behind us would
        # otherwise wait on that same greenlet and then read ``.dead`` off the
        # None the first one left in its place.
        self._started = False
        # Detached up front, and never read off ``self`` again: waiting for the
        # server greenlet switches, and whoever gets in behind us — a second
        # closer, or a start() rebuilding the bridge — leaves these fields
        # holding a generation that is not the one we are tearing down.
        transport, self._transport = self._transport, None
        greenlet, self._server_greenlet = self._server_greenlet, None
        reader, self._reader_task = self._reader_task, None
        self._pending = fresh  # the old futures live on in `waiters`

        try:
            if reader is not None and not reader.done():
                # Suppressed, because a reader belonging to a loop that has
                # since closed cannot be cancelled: the cancellation would be
                # scheduled there, and cancel() raises "Event loop is closed"
                # instead — failing the very call that came to rebuild the
                # client. Closing the transport below is what actually retires
                # that reader, and the only thing that clears pyzmq's receive
                # queue in any case.
                try:
                    reader.cancel()
                except RuntimeError:
                    pass

            while waiters:
                fut = waiters.pop()
                if not fut.done():
                    # Contained for the reader's reason one line up: a future
                    # belonging to a loop that has closed cannot be completed —
                    # set_exception schedules its callbacks there and raises
                    # "Event loop is closed". Its waiter went with that loop;
                    # what must not happen is the failure taking down the call
                    # that came to rebuild the client.
                    try:
                        fut.set_exception(closed_error)
                    except RuntimeError:
                        pass

            if self._mode is ProcessBridge.Mode.SERVER:
                if greenlet is not None:
                    greenlet.join(timeout=2)
                    if not greenlet.dead:
                        greenlet.kill(block=True, timeout=1)
        finally:
            # From a finally: the join switches, so a caller's enclosing
            # gevent.Timeout can land inside it — and having claimed the flag,
            # we are the only closer left, so neither a later close() nor
            # __del__ would come back for this transport.
            if transport is not None:
                transport.close()
