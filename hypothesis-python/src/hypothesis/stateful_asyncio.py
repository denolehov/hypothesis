import asyncio
import inspect
import warnings

from .stateful import RuleBasedStateMachine


class AsyncioRuleBasedStateMachine(RuleBasedStateMachine):
    """A RuleBasedStateMachine that supports both synchronous and asynchronous rules.

    This class allows defining both sync and async methods for rules, invariants,
    and other state machine functions. Async functions will be properly awaited
    in an asyncio event loop.
    """

    def __init__(self):
        super().__init__()

        self._loop = None
        self._is_own_loop = False

    def _execute_fn(self, fn, **data):
        if inspect.iscoroutinefunction(fn):
            self._ensure_loop()

            return self._loop.run_until_complete(fn(self, **data))

        return super()._execute_fn(fn, **data)

    def teardown(self):
        super().teardown()

        if self._is_own_loop and self._loop and not self._loop.is_closed():
            try:
                self._loop.run_until_complete(self._loop.shutdown_asyncgens())
            except Exception as e:
                warnings.warn(f"Error cleaning up asyncio loop: {e}")
            finally:
                self._loop.close()
                self._loop = None

    def _ensure_loop(self):
        if self._loop:
            return

        try:
            self._loop = asyncio.get_running_loop()
            self._is_own_loop = False
        except RuntimeError:
            self._loop = asyncio.new_event_loop()
            self._is_own_loop = True
            asyncio.set_event_loop(self._loop)
