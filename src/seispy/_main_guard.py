"""Reject entry scripts that worker pools cannot re-import.

``spawn`` and ``forkserver`` re-run the caller's ``__main__`` module inside every
worker process. A script that calls a pool-based function while that module is
still executing therefore repeats the same call in each worker instead of
processing files. The worker dies during bootstrapping, and because the parent
is still writing the initializer payload (an inventory can be tens of megabytes)
into that worker's pipe, the parent blocks there and the run shows no progress
for as long as the user is willing to wait.

The shape is detectable before any process is created, without guessing: the
call is reached from the live ``__main__`` frame, that frame is at module level
outside an ``if __name__ == "__main__":`` guard, and the start method in use
re-executes that same file in workers. Fail with the fix instead of hanging.

seispy pools use the platform default start method, so this check is inert under
``fork`` (the Linux default) and only guards ``spawn`` and ``forkserver``
platforms, where the guard is a real requirement.

The check follows the live module-level frame of ``__main__``, so a pool call
reached through a module that the entry script imports at module level is
attributed to that import line. Calls made by worker threads, and calls that the
workers themselves repeat during bootstrapping, are left to their parent or to
the worker guard.
"""

import ast
import multiprocessing
import sys
from pathlib import Path
from types import FrameType

REIMPORTING_METHODS = frozenset({"spawn", "forkserver"})


def require_reimport_safe_entry_point(
    operation: str, *, start_method: str | None = None
) -> None:
    """Raise when the caller's own script would re-run inside every worker.

    Args:
        operation: Public function name used in the error message.
        start_method: Start method the caller uses. Defaults to the effective
            method of the default multiprocessing context.

    Raises:
        RuntimeError: If ``start_method`` re-imports the entry script and the
            current call sits at its module level, outside any
            ``if __name__ == "__main__":`` guard.
    """
    method = start_method or _effective_start_method()
    if method not in REIMPORTING_METHODS:
        return
    if getattr(multiprocessing.current_process(), "_inheriting", False):
        # Only reachable by re-running an unguarded entry script: a guarded
        # script does no work during bootstrapping.
        raise RuntimeError(_bootstrapping_message(operation, method))
    frame = _entry_module_frame()
    if frame is None:
        return
    path = Path(frame.f_code.co_filename)
    if not _workers_reimport(path):
        return
    source = _read_source(path)
    if source is None:
        return
    if frame.f_lineno in _guarded_lines(source, str(path)):
        return
    raise RuntimeError(_message(operation, method, path, source, frame.f_lineno))


def _effective_start_method() -> str:
    method = multiprocessing.get_start_method(allow_none=True)
    if method is None:
        method = multiprocessing.get_context().get_start_method()
    return method


def _entry_module_frame() -> FrameType | None:
    """Return the live module-level frame of ``__main__``, if it is on the stack.

    An interactive session, ``python -c``, a notebook cell, a test runner, or an
    imported library has no such frame while the call is made, and neither has a
    script that reaches the call from inside its ``__main__`` guard.
    """
    frame = sys._getframe(1)
    while frame is not None:
        if (
            frame.f_code.co_name == "<module>"
            and frame.f_globals.get("__name__") == "__main__"
        ):
            return frame
        frame = frame.f_back
    return None


def _workers_reimport(path: Path) -> bool:
    """Report whether the start method re-executes ``path`` in every worker."""
    if getattr(sys, "frozen", False):
        return False
    main = sys.modules.get("__main__")
    if main is None:
        return False
    spec_name = getattr(getattr(main, "__spec__", None), "name", None)
    origin = None
    if spec_name is not None:
        # ``python -m package`` runs its ``__main__`` module unconditionally in
        # the parent, and spawn deliberately does not re-run it.
        if spec_name == "__main__" or spec_name.endswith(".__main__"):
            return False
        origin = getattr(main.__spec__, "origin", None)
    origin = origin or getattr(main, "__file__", None)
    # Interactive sessions, ``python -c`` and frozen executables have no script
    # for spawn to re-execute.
    if not origin or origin.startswith("<"):
        return False
    if sys.platform == "win32" and origin.lower().endswith(".exe"):
        return False
    return _same_file(Path(origin), path)


def _same_file(first: Path, second: Path) -> bool:
    try:
        return first.resolve() == second.resolve()
    except OSError:
        return str(first) == str(second)


def _read_source(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError, ValueError):
        return None


def _guarded_lines(source: str, filename: str) -> frozenset[int]:
    """Return every line of a module-level ``if __name__ == "__main__":`` body."""
    try:
        tree = ast.parse(source, filename=filename)
    except (SyntaxError, ValueError):
        return frozenset()
    guarded = set()
    for statement in tree.body:
        if _is_main_guard(statement):
            guarded.update(range(statement.lineno, (statement.end_lineno or 0) + 1))
    return frozenset(guarded)


def _is_main_guard(statement: ast.stmt) -> bool:
    if not isinstance(statement, ast.If) or not isinstance(statement.test, ast.Compare):
        return False
    test = statement.test
    if len(test.ops) != 1 or not isinstance(test.ops[0], ast.Eq):
        return False
    if len(test.comparators) != 1:
        return False
    return {_comparison_atom(test.left), _comparison_atom(test.comparators[0])} == {
        "name",
        "main",
    }


def _comparison_atom(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name) and node.id == "__name__":
        return "name"
    if isinstance(node, ast.Constant) and node.value == "__main__":
        return "main"
    return None


def _message(operation: str, method: str, path: Path, source: str, line: int) -> str:
    return (
        f'{operation} starts worker processes with the "{method}" start method, '
        "which re-runs the entry script in every worker process:\n\n"
        f"    {_location(path, source, line)}\n\n"
        'That line runs at module level, outside any `if __name__ == "__main__":` '
        "guard, so each worker repeats the call instead of processing files. The "
        "run reports no progress and the parent can stall while starting "
        "workers.\n\n"
        "Move the work into a function and call it behind a guard:\n\n"
        "    def main():\n"
        "        ...  # everything the script now does at module level\n\n"
        '    if __name__ == "__main__":\n'
        "        main()"
    )


def _bootstrapping_message(operation: str, method: str) -> str:
    entry = ""
    if sys.argv and sys.argv[0].endswith(".py"):
        entry = f" ({sys.argv[0]})"
    return (
        f"{operation} was called while a worker process was starting, so the "
        f'"{method}" start method re-ran the entry script{entry} and that script '
        "started work at module level.\n\n"
        "Put the work in a function and call it behind a guard:\n\n"
        "    def main():\n"
        "        ...  # everything the script now does at module level\n\n"
        '    if __name__ == "__main__":\n'
        "        main()"
    )


def _location(path: Path, source: str, line: int) -> str:
    lines = source.splitlines()
    if 1 <= line <= len(lines):
        return f"{path}:{line}  {lines[line - 1].strip()}"
    return f"{path}:{line}"
