import asyncio

import tester_bot
from test_runner import test_runner
from tester_bot import start_client as start_tester_bot
from tester_bot import testing_server

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

    async def __aexit__(self, exc_type, exc_value, traceback):
        await asyncio.gather(
            asyncio.create_task(self.bridge_bot_client.close()),
            self.running_bridge_bot_client_task,
            asyncio.create_task(self.tester_bot_client.close()),
            self.running_tester_bot_client_task,
        )

        return True


async def run_tests():
    """Run all tests registered to the test runner."""
    async with Bots() as bots:
        await test_runner.run_tests(
            bots.bridge_bot_client,
            bots.tester_bot_client,
            testing_server,
        )


if __name__ == "__main__":
    asyncio.run(run_tests())
