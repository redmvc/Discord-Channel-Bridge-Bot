import asyncio
import importlib
import pkgutil
import sys
import traceback
from pathlib import Path
from types import TracebackType
from typing import Any

# Add src/tests/ and src/ to sys.path so that all test modules and
# top-level modules (common, bridge, events, etc.) can be imported.
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import test_cases
import test_runner
import tester_bot
from tester_bot import start_client as start_tester_bot

import common
from main import start_client as start_bridge_bot
from validations import logger

# Auto-discover and import all test case modules
for _, module_name, _ in pkgutil.iter_modules(test_cases.__path__):
    importlib.import_module(f"test_cases.{module_name}")


class Bots:
    async def __aenter__(self):
        self.running_bridge_bot_client_task = asyncio.create_task(
            start_bridge_bot(False)
        )
        self.bridge_bot_client = common.client

        self.running_tester_bot_client_task = asyncio.create_task(start_tester_bot())
        self.tester_bot_client = tester_bot.client

        try:
            await asyncio.gather(
                common.wait_until_ready(),
                tester_bot.wait_until_ready(),
            )
        except BaseException:
            await asyncio.gather(
                asyncio.shield(self.bridge_bot_client.close()),
                asyncio.shield(self.tester_bot_client.close()),
                return_exceptions=True,
            )
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type | None,
        exc_value: Any | None,
        tb: TracebackType | None,
    ):
        # Delete the role first (needs an open bot connection). Wrapped so a
        # failure here doesn't skip the bot close below.
        if test_runner.webhook_permissions_role is not None:
            try:
                await test_runner.webhook_permissions_role.delete()
            except Exception:
                logger.exception(
                    "Failed to delete webhook_permissions_role during shutdown"
                )

        # Shield close() from outer cancellation so Ctrl+C can't abort the
        # websocket/aiohttp teardown mid-flight.
        await asyncio.gather(
            asyncio.shield(self.bridge_bot_client.close()),
            asyncio.shield(self.tester_bot_client.close()),
            return_exceptions=True,
        )

        await asyncio.gather(
            self.running_bridge_bot_client_task,
            self.running_tester_bot_client_task,
            return_exceptions=True,
        )

        if test_runner.failures:
            num_tests = len(test_runner.failures)
            num_failures = sum(len(f) for _, f in test_runner.failures.items())
            test_runner.log_expectation(
                f"A total of {num_failures} failure{'s' if num_failures >= 2 else ''} happened in {num_tests} test{'s' if num_tests >= 2 else ''}:\n - {'\n - '.join(t + ' (' + str(len(f)) + ')' for t, f in test_runner.failures.items())}",
                "failure",
            )
        elif exc_type:
            if (exc_type is KeyboardInterrupt) or (exc_type is asyncio.CancelledError):
                test_runner.log_expectation(
                    "All tests so far passed, but execution was interrupted.",
                    "failure",
                )
            else:
                test_runner.log_expectation(
                    f"All tests so far passed, but an uncaught exception occurred: {exc_type!r} ({exc_value!r})",
                    "failure",
                )
        else:
            test_runner.log_expectation(
                "All tests passed!",
                "success",
                print_success_to_console=True,
            )

        if (
            exc_value
            and (exc_type is not KeyboardInterrupt)
            and (exc_type is not asyncio.CancelledError)
        ):
            traceback.print_tb(tb)
            raise exc_value.with_traceback(tb)


async def run_tests():
    """Run all tests registered to the test runner."""
    filter_prefix = sys.argv[1] if len(sys.argv) > 1 else None
    async with Bots():
        await test_runner.test_runner.run_tests(
            tester_bot.testing_server,
            filter_prefix=filter_prefix,
        )


if __name__ == "__main__":
    try:
        asyncio.run(run_tests())
    except KeyboardInterrupt:
        pass
