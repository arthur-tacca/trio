from __future__ import annotations

import ast
import contextlib
import inspect
import sys
import warnings
from code import InteractiveConsole
from types import CodeType, FrameType, FunctionType
from typing import TYPE_CHECKING

import outcome

import trio
import trio.lowlevel
from trio._util import final

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


class SuppressDecorator(contextlib.ContextDecorator, contextlib.suppress):
    pass


@SuppressDecorator(KeyboardInterrupt)
@trio.lowlevel.disable_ki_protection
def terminal_newline() -> None:  # TODO: test this line
    import fcntl
    import termios

    # Fake up a newline char as if user had typed it at the terminal
    try:
        fcntl.ioctl(sys.stdin, termios.TIOCSTI, b"\n")  # type: ignore[attr-defined, unused-ignore]
    except OSError as e:
        print(f"\nPress enter! Newline injection failed: {e}", end="", flush=True)


async def _run_interruptible(
    func: Callable[[], Awaitable[object]],
) -> outcome.Outcome[object]:
    # Runs in the Trio thread, inside the task that hosts the console thread.
    # While the statement runs, control-C should interrupt just this
    # statement; left to Trio's own handler, it would cancel the whole REPL.
    from signal import SIGINT, signal

    from trio._core._ki import ki_protection_enabled

    token = trio.lowlevel.current_trio_token()
    with trio.CancelScope() as scope:

        def handler(sig: int, frame: FrameType | None) -> None:
            # Mirror Trio's own handler: if the main thread is running the
            # user's (unprotected) code, raise right here, so that CPU-bound
            # statements can be interrupted. Otherwise, we may be deep inside
            # Trio's machinery, so cancel the statement from a safe point.
            if not ki_protection_enabled(frame):
                raise KeyboardInterrupt
            token.run_sync_soon(scope.cancel)

        prev_handler = signal(SIGINT, handler)
        try:
            result = await outcome.acapture(func)
        finally:
            signal(SIGINT, prev_handler)
    if (
        scope.cancel_called
        and isinstance(result, outcome.Error)
        and isinstance(result.error, trio.Cancelled)
    ):
        # Report it the way the stdlib console does, keeping the traceback
        # so the user can see where the statement was interrupted.
        ki = KeyboardInterrupt().with_traceback(result.error.__traceback__)
        return outcome.Error(ki)
    return result


@final
class TrioInteractiveConsole(InteractiveConsole):
    def __init__(self, repl_locals: dict[str, object] | None = None) -> None:
        super().__init__(locals=repl_locals)
        self.token: trio.lowlevel.TrioToken | None = None
        self.compile.compiler.flags |= ast.PyCF_ALLOW_TOP_LEVEL_AWAIT
        self.interrupted = False

    def runcode(self, code: CodeType) -> None:
        func = FunctionType(code, self.locals)
        if inspect.iscoroutinefunction(func):
            result = trio.from_thread.run(_run_interruptible, func)
        else:
            result = trio.from_thread.run_sync(outcome.capture, func)
        if isinstance(result, outcome.Error):
            # If it is SystemExit, quit the repl. Otherwise, print the traceback.
            # If there is a SystemExit inside a BaseExceptionGroup, it probably isn't
            # the user trying to quit the repl, but rather an error in the code. So, we
            # don't try to inspect groups for SystemExit. Instead, we just print and
            # return to the REPL.
            if isinstance(result.error, SystemExit):
                raise result.error
            else:
                # Inline our own version of self.showtraceback that can use
                # outcome.Error.error directly to print clean tracebacks.
                # This also means overriding self.showtraceback does nothing.
                sys.last_type, sys.last_value = type(result.error), result.error
                sys.last_traceback = result.error.__traceback__
                # see https://docs.python.org/3/library/sys.html#sys.last_exc
                if sys.version_info >= (3, 12):
                    sys.last_exc = result.error

                # We always use sys.excepthook, unlike other implementations.
                # This means that overriding self.write also does nothing to tbs.
                sys.excepthook(sys.last_type, sys.last_value, sys.last_traceback)

    def raw_input(self, prompt: str = "") -> str:
        # While we wait at the prompt, control-C is ours to handle: left to
        # Trio's handler, it would cancel the task hosting the REPL.
        from signal import SIGINT, signal

        assert not self.interrupted

        def install_handler() -> Callable[[int, FrameType | None], None] | int | None:
            def handler(
                sig: int, frame: FrameType | None
            ) -> None:  # TODO: test this line
                self.interrupted = True
                if sys.platform != "win32":
                    # Make input() return, as if the user had pressed enter.
                    # (On Windows, a control-C in a console makes input()
                    # raise EOFError by itself; with piped input, the user
                    # has to press enter.)
                    token.run_sync_soon(terminal_newline, idempotent=True)

            token = trio.lowlevel.current_trio_token()

            return signal(SIGINT, handler)

        prev_handler = trio.from_thread.run_sync(install_handler)
        try:
            return input(prompt)
        finally:
            trio.from_thread.run_sync(signal, SIGINT, prev_handler)
            if self.interrupted:  # TODO: test this line
                raise KeyboardInterrupt

    def write(self, output: str) -> None:
        if self.interrupted:  # TODO: test this line
            assert output == "\nKeyboardInterrupt\n"
            # The injected newline already ended the prompt's line.
            sys.stderr.write(output if sys.platform == "win32" else output[1:])
            self.interrupted = False
        else:
            sys.stderr.write(output)


async def run_repl(console: TrioInteractiveConsole) -> None:
    banner = (
        f"trio REPL {sys.version} on {sys.platform}\n"
        f'Use "await" directly instead of "trio.run()".\n'
        f'Type "help", "copyright", "credits" or "license" '
        f"for more information.\n"
        f'{getattr(sys, "ps1", ">>> ")}import trio'
    )
    try:
        await trio.to_thread.run_sync(console.interact, banner)
    finally:
        warnings.filterwarnings(
            "ignore",
            message=r"^coroutine .* was never awaited$",
            category=RuntimeWarning,
        )


def main(original_locals: dict[str, object]) -> None:
    with contextlib.suppress(ImportError):
        import readline  # noqa: F401

    repl_locals: dict[str, object] = {"trio": trio}
    for key in {
        "__name__",
        "__package__",
        "__loader__",
        "__spec__",
        "__builtins__",
        "__file__",
    }:
        repl_locals[key] = original_locals[key]

    console = TrioInteractiveConsole(repl_locals)
    trio.run(run_repl, console)
