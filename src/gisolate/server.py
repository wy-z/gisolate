"""serve(): turn THIS process into the isolated worker, so clients can share it."""

from typing import Any, Callable

import dill

from . import _internal, _workers


def serve(
    factory: Callable[[], Any],
    address: str,
    *,
    patch_kwargs: dict | None = None,
    max_concurrency: int | None = None,
) -> None:
    """Run the worker loop in this process, bound to *address*. Blocks.

    The counterpart to :meth:`gisolate.ProcessProxy.attach`. A plain
    ``ProcessProxy()`` spawns a worker only it can reach — the address is private
    and the proxy owns the process. Where several client processes need the same
    isolated library (each one otherwise paying its own copy of whatever the
    factory imports), one of them instead runs this, and the rest attach.

    Blocks until the process is interrupted or terminated — no signal handling is
    installed here, so whatever the host process does about SIGTERM is what
    happens; an attached client never asks the worker to exit. ``max_concurrency``
    bounds the WORKER, not each client, so it is shared by every attached process.

    ``ipc://`` only, and not merely by convention: a call carries the caller's
    monotonic deadline, which means nothing on a second machine's clock — a peer
    running behind ours would honour an expired request rather than reject it.

    Choose *address* the way you would choose a unix socket for anything else
    privileged. The wire is unauthenticated pickle and an empty frame stops the
    worker, so whoever can connect can run code here or shut the host down for
    every client. Enforcing that is the caller's, not ours — the safe recipe is
    a directory created ``0700``, since the socket file's own mode is whatever
    the host's umask makes it (a spawned ``ProcessProxy`` gets this for free;
    its addresses live in a private per-uid directory).
    """
    _internal.require_ipc(address, "gisolate.serve()")
    config = _workers.WorkerConfig(
        ipc_addr=address,
        # The round trip through dill is redundant in-process, but it keeps ONE
        # worker entry point: the same config a spawned child is handed.
        factory_bytes=dill.dumps(factory),
        max_concurrency=max_concurrency,
    )
    if patch_kwargs is not None:
        _workers.gevent_worker(config, patch_kwargs)
    else:
        _workers.asyncio_worker(config)
