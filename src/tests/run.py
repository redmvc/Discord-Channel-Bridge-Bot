import asyncio
import traceback
from types import TracebackType
from typing import Any

import test_runner
import tester_bot
from tester_bot import start_client as start_tester_bot

import globals
from main import start_client as start_bridge_bot


class Bots:
    async def __aenter__(self):
        self.running_bridge_bot_client_task = asyncio.create_task(
            start_bridge_bot(False)
        )
        self.bridge_bot_client = globals.client

        self.running_tester_bot_client_task = asyncio.create_task(start_tester_bot())
        self.tester_bot_client = tester_bot.client

        await asyncio.gather(globals.wait_until_ready(), tester_bot.wait_until_ready())
        return self

    async def __aexit__(
        self,
        exc_type: type | None,
        exc_value: Any | None,
        tb: TracebackType | None,
    ):
        if test_runner.webhook_permissions_role is not None:
            await test_runner.webhook_permissions_role.delete()

        await asyncio.gather(
            asyncio.create_task(self.bridge_bot_client.close()),
            self.running_bridge_bot_client_task,
            asyncio.create_task(self.tester_bot_client.close()),
            self.running_tester_bot_client_task,
        )

        if exc_type:
            traceback.print_tb(tb)
            raise exc_type(exc_value)

        return True


async def run_tests():
    """Run all tests registered to the test runner."""
    async with Bots():
        await test_runner.test_runner.run_tests(tester_bot.testing_server)


if __name__ == "__main__":
    asyncio.run(run_tests())
