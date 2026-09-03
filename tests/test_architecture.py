"""Rules that hold across the package, checked instead of remembered.

Everything else here tests behaviour. These test shape — because the bugs
they stand for were never a wrong line, they were a site that forgot a rule
written down in somebody else's comment.
"""

import ast
import pathlib

SRC = pathlib.Path(__file__).resolve().parent.parent / "src" / "gisolate"

# Acquiring a ZMQ transport by hand is four lines that must not be written
# again: a context, a socket, LINGER, a bind or connect — plus, on the bind
# side, the inode to release later. Every one of those has been forgotten at
# some site at some point.
ACQUISITION = {"Context", "socket", "bind", "connect"}

# _internal owns the carrier. Tests may still drive raw sockets — that is what
# a test double is for — so only the package itself is scanned, all of it:
# rglob, because a subpackage added later is exactly where this would be
# forgotten. By path, not by name: a second file called _internal.py in a
# subpackage would otherwise exempt itself.
OWNER = SRC / "_internal.py"


def _acquisitions(path: pathlib.Path) -> list[tuple[int, str]]:
    tree = ast.parse(path.read_text(), filename=str(path))
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
        if name in ACQUISITION:
            found.append((node.lineno, name))
    return found


def test_only_internal_acquires_a_zmq_transport():
    """A new subsystem cannot quietly grow its own sock/ctx pair.

    ZmqTransport is only a carrier for the rules — it makes them easy to
    follow, it cannot make them followed. This is what notices when a site
    does not.

    It catches forgetting, not evading: a call reached through an alias
    (``make = ctx.socket``) or through getattr passes. That is the honest
    limit of reading the syntax, and the case worth catching is the one where
    somebody writes the four familiar lines again without thinking about the
    fifth.
    """
    offenders = {
        str(path.relative_to(SRC)): hits
        for path in sorted(SRC.rglob("*.py"))
        if path != OWNER and (hits := _acquisitions(path))
    }
    assert not offenders, (
        "these acquire a transport outside _internal.ZmqTransport:\n"
        + "\n".join(
            f"  {name}:{line} calls {call}()"
            for name, hits in offenders.items()
            for line, call in hits
        )
    )


def _own_nodes(func: ast.AST):
    """Walk a function's body without descending into nested ones, so a closure's
    handlers are attributed to the closure and not to whatever encloses it."""
    stack = list(ast.iter_child_nodes(func))
    while stack:
        node = stack.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            continue
        yield node
        stack.extend(ast.iter_child_nodes(node))


def _marshals_itself(path: pathlib.Path) -> list[tuple[int, str]]:
    """Methods that hand their OWN name to run_on_main_hub.

    ``current_thread()`` is per greenlet, so the greenlet a marshal spawns tests
    the guard again, decides it is foreign, and marshals itself for ever. The
    target has to be an owner-only body. Round 26 found this in three methods;
    round 36 found it again in a fourth, measured at 21 hops and climbing.
    """
    tree = ast.parse(path.read_text(), filename=str(path))
    found = []
    for func in ast.walk(tree):
        if not isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for node in _own_nodes(func):
            if not isinstance(node, ast.Call):
                continue
            target = node.func
            name = target.attr if isinstance(target, ast.Attribute) else getattr(target, "id", "")
            if name != "run_on_main_hub":
                continue
            for arg in [*node.args, *(kw.value for kw in node.keywords)]:
                # Either the method itself, or functools.partial(self.method, …)
                candidates = [arg]
                if isinstance(arg, ast.Call):  # functools.partial(self.method, …)
                    candidates = [*arg.args[:1], *(kw.value for kw in arg.keywords)]
                for candidate in candidates:
                    named = (
                        candidate.attr
                        if isinstance(candidate, ast.Attribute)
                        else getattr(candidate, "id", "")
                    )
                    if named == func.name:
                        found.append((node.lineno, func.name))
    return found


def test_no_method_marshals_itself_to_the_hub():
    offenders = {
        str(path.relative_to(SRC)): hits
        for path in sorted(SRC.rglob("*.py"))
        if (hits := _marshals_itself(path))
    }
    assert not offenders, (
        "these schedule themselves on the hub, which re-tests the guard and "
        "marshals again for ever — schedule an owner-only body instead:\n"
        + "\n".join(
            f"  {name}:{line} in {func}()"
            for name, hits in offenders.items()
            for line, func in hits
        )
    )


# A broad catch that CONTAINS its exception — one that does not re-raise — must
# let the operator's interrupt through first. These contain one on purpose, each
# with its reason.
INTERRUPT_CONTAINED_ON_PURPOSE = {
    # Formatting an error reply, where letting the interrupt out would lose the
    # reply the caller is owed and the operator's next Ctrl-C still lands.
    ("_workers.py", "safe_dumps"),
    ("_workers.py", "_malformed"),
    ("_internal.py", "wrap_exception"),
    # Naming a failure, and reading the traceback attached to it, for the same
    # reason and on the same paths: every caller is mid-way through turning one
    # bad object into the reply somebody is waiting for, and none of them can
    # afford the describing step to be the one that fails.
    ("_internal.py", "type_name"),
    ("_internal.py", "remote_traceback"),
    # A one-shot child: the parent is given the error and the process ends.
    ("subprocess.py", "_worker"),
    # GC context: an exception escaping __del__ is printed and swallowed by
    # the interpreter anyway, and there is no caller for the interrupt to
    # reach — re-raising would only lose the teardown below the catch (the
    # socket for the proxy, the lease's last retry for the transport).
    ("proxy.py", "__del__"),
    ("_internal.py", "__del__"),
    # A raw native thread (not a greenlet on a hub): no operator interrupt is
    # ever raised there, and the gevent-side results it relays to are the only
    # way out — re-raising would only skip the exit signal below the catch.
    ("asyncio_thread.py", "_run"),
    ("asyncio_thread.py", "_close"),
}


def _reraises(handler: ast.ExceptHandler) -> bool:
    """True when the handler ends up re-raising, so nothing is contained."""
    return any(
        isinstance(node, ast.Raise) and node.exc is None
        for node in ast.walk(handler)
    )


def _caught(handler: ast.ExceptHandler) -> set[str]:
    """What one ``except`` clause catches, by name.

    A tuple counts as all of its members, and a bare ``except`` as
    BaseException — both were ways to write the same catch and slip past a
    check that only read ``except <Name>``.
    """
    if handler.type is None:
        return {"BaseException"}
    parts = (
        handler.type.elts if isinstance(handler.type, ast.Tuple) else [handler.type]
    )
    return {
        p.attr if isinstance(p, ast.Attribute) else getattr(p, "id", "") for p in parts
    }


def _unguarded_broad_catches(path: pathlib.Path) -> list[tuple[int, str]]:
    tree = ast.parse(path.read_text(), filename=str(path))
    found = []
    for func in ast.walk(tree):
        if not isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if (path.name, func.name) in INTERRUPT_CONTAINED_ON_PURPOSE:
            continue
        for node in _own_nodes(func):
            # TryStar too: `except* BaseException` contains the same way.
            if not isinstance(node, (ast.Try, ast.TryStar)):
                continue
            caught = [_caught(h) for h in node.handlers]
            broad = next(
                (i for i, names in enumerate(caught) if "BaseException" in names), None
            )
            if broad is None:
                continue
            if _reraises(node.handlers[broad]):
                continue  # the interrupt passes through with everything else
            # The earlier clause must RE-RAISE, not merely exist: an
            # `except KeyboardInterrupt: pass` above the broad catch is the
            # same containment wearing the guard's shape.
            if not any(
                "KeyboardInterrupt" in names and _reraises(node.handlers[i])
                for i, names in enumerate(caught[:broad])
            ):
                found.append((node.handlers[broad].lineno, func.name))
    return found


def test_a_broad_catch_lets_the_operators_interrupt_through():
    """Measured: a real SIGINT is raised in whatever greenlet is running on the
    main OS thread, so it lands inside client code rather than in the main
    greenlet. A bare ``except BaseException`` there absorbs Ctrl-C — four
    consecutive rounds went site by site fixing exactly that.

    Reads ``except`` clauses only: a ``contextlib.suppress(BaseException)``
    would pass. Every current use of that is a teardown path releasing
    resources, where there is nothing left to interrupt.
    """
    offenders = {
        str(path.relative_to(SRC)): hits
        for path in sorted(SRC.rglob("*.py"))
        if (hits := _unguarded_broad_catches(path))
    }
    assert not offenders, (
        "these catch BaseException without letting KeyboardInterrupt past "
        "first — add `except KeyboardInterrupt: raise`, or list the site in "
        "INTERRUPT_CONTAINED_ON_PURPOSE with its reason:\n"
        + "\n".join(
            f"  {name}:{line} in {func}()"
            for name, hits in offenders.items()
            for line, func in hits
        )
    )


# Teardown that must not allocate on the way out: a MemoryError building a
# ``contextlib.suppress`` guard skipped the very release the guard was for,
# after the resource had already been detached from every owner. These
# functions were swept to plain try/except (zero allocation) in round 61;
# this keeps them swept. Matched by (file, function) name, nested functions
# included.
ZERO_ALLOCATION_TEARDOWN = {
    ("_internal.py", "close"),
    ("_internal.py", "release"),
    ("_internal.py", "__del__"),
    ("_workers.py", "safe_close"),
    ("_workers.py", "_dispose"),
    ("_workers.py", "_build_once"),
    ("bridge.py", "__del__"),
    ("bridge.py", "close"),
    ("proxy.py", "_publish_failed_launch"),
    ("proxy.py", "_forget_reaped"),
    ("proxy.py", "_cleanup_process"),
    ("proxy.py", "__del__"),
    ("proxy.py", "_undo"),
    ("proxy.py", "_teardown"),
    ("proxy.py", "_stop"),
    ("subprocess.py", "_make_pipe"),
    ("subprocess.py", "cleanup"),
}


def _suppress_uses(path: pathlib.Path) -> list[tuple[int, str]]:
    tree = ast.parse(path.read_text(), filename=str(path))
    found = []
    for func in ast.walk(tree):
        if not isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if (path.name, func.name) not in ZERO_ALLOCATION_TEARDOWN:
            continue
        for node in _own_nodes(func):
            if not isinstance(node, ast.Call):
                continue
            target = node.func
            name = target.attr if isinstance(target, ast.Attribute) else getattr(target, "id", "")
            if name == "suppress":
                found.append((node.lineno, func.name))
    return found


def test_teardown_paths_do_not_build_suppress_guards():
    """A guard that must be built is a guard that can refuse. On these paths
    the resource is already detached from every owner, so the refusal — a
    MemoryError constructing ``contextlib.suppress`` — skipped the one release
    left. try/except compiles to jumps and allocates nothing.

    This enforces exactly the suppress rule, nothing wider: allocations by
    other names — a tuple, a view, an allocating callee — pass, and a zone
    function renamed away silently stops being checked. It catches the one
    regression that has actually been written, which is reaching for
    contextlib.suppress out of habit."""
    offenders = {
        str(path.relative_to(SRC)): hits
        for path in sorted(SRC.rglob("*.py"))
        if (hits := _suppress_uses(path))
    }
    assert not offenders, (
        "these teardown functions build a suppress guard — use try/except, "
        "which allocates nothing:\n"
        + "\n".join(
            f"  {name}:{line} in {func}()"
            for name, hits in offenders.items()
            for line, func in hits
        )
    )
