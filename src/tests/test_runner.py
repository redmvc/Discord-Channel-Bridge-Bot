import asyncio
import sys
from abc import ABC
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Coroutine, TypeVar, cast

from aiolimiter import AsyncLimiter
from discord import Client, Guild

sys.path.append(str(Path(__file__).parent.parent))
from validations import setup_logger
import globals

if TYPE_CHECKING:
    from discord import TextChannel

logger = setup_logger("test_logger", "test_logs.log", "DEBUG")

# Helper to prevent us from being rate limited
rate_limiter = AsyncLimiter(1, 10)

CoroT = TypeVar(
    "CoroT",
    bound=Callable[
        [
            Client,
            Client,
            Guild,
            tuple["TextChannel", "TextChannel", "TextChannel", "TextChannel"],
        ],
        Coroutine[Any, Any, None],
    ],
)


class TestRunner:
    """A class that runs all registered tests.

    Attributes
    ----------
    test_cases : list[:class:`TestCase`]
        The list of registered test cases.
    """

    def __init__(self):
        self.test_cases: list["TestCase"] = []

    def register_test_case(self, test_case: "TestCase"):
        """Register a test case to this object.

        Parameters
        ----------
        test_case : :class:`TestCase`
        """
        self.test_cases.append(test_case)
        logger.debug(
            "%s has successfully been registered as a test case.",
            type(test_case).__name__,
        )

    async def run_tests(
        self,
        bridge_bot: Client,
        tester_bot: Client,
        testing_server: Guild,
    ):
        """Run the tests registered to this object.

        Parameters
        ----------
        bridge_bot : :class:`~discord.Client`
            The Bridge Bot client.
        tester_bot : :class:`~discord.Client`
            The Tester Bot client.
        testing_server : :class:`~discord.Guild`
            The server in which to run the tests.
        """
        logger.info("Starting to run tests.")
        if not rate_limiter.has_capacity():
            raise ConnectionError(
                "Rate limiter does not have capacity for running the tests."
            )
        logger.debug("Rate limiter has capacity.")

        async with rate_limiter:
            # Fetch the view of the testing server from the Bridge Bot's perspective
            logger.debug("Fetching testing server...")
            if not (
                bridge_bot_testing_server := bridge_bot.get_guild(testing_server.id)
            ):
                bridge_bot_testing_server = await bridge_bot.fetch_guild(
                    testing_server.id
                )
            testing_server = bridge_bot_testing_server
            logger.debug("Fetched.")

            # Delete all channels in the server
            logger.info("Deleting server channels...")
            server_channels = await testing_server.fetch_channels()
            delete_channels: list[Coroutine[Any, Any, None]] = []
            for channel in server_channels:
                delete_channels.append(channel.delete())
            await asyncio.gather(*delete_channels)
            logger.info("Deleted.")

            # Create four channels for testing
            logger.info("Creating test channels...")
            create_testing_channels: list[Coroutine[Any, Any, "TextChannel"]] = []
            for i in range(4):
                create_testing_channels.append(
                    testing_server.create_text_channel(f"testing_channel_{i + 1}")
                )
            testing_channels = tuple(
                tester_bot.get_channel(channel.id)
                for channel in await asyncio.gather(*create_testing_channels)
            )
            assert len(testing_channels) == 4
            if TYPE_CHECKING:
                testing_channels = cast(
                    tuple[
                        "TextChannel",
                        "TextChannel",
                        "TextChannel",
                        "TextChannel",
                    ],
                    testing_channels,
                )
            logger.info("Created.")

            # Register the test bot in globals
            assert tester_bot.user
            globals.test_app = await bridge_bot.fetch_user(tester_bot.user.id)

            # Run the tests
            logger.info("Running tests.")
            for test_case in self.test_cases:
                logger.debug(f"Starting test case {type(test_case).__name__}.")
                for test in test_case.tests:
                    logger.debug(f"Starting test {test.__name__}.")
                    await test(bridge_bot, tester_bot, testing_server, testing_channels)


class TestCase(ABC):
    """An abstract class to register test cases.

    Attributes
    ----------
    tests : list[(:class:`~discord.Client`, :class:`~discord.Client`, :class:`~discord.Guild`, tuple[:class:`~discord.TextChannel`, :class:`~discord.TextChannel`, :class:`~discord.TextChannel`, :class:`~discord.TextChannel`]) -> Coroutine[Any, Any, None]]
        The list of registered tests.
    """

    def __init__(self, test_base: TestRunner):
        test_base.register_test_case(self)
        self.tests: list[
            Callable[
                [
                    Client,
                    Client,
                    Guild,
                    tuple[
                        "TextChannel",
                        "TextChannel",
                        "TextChannel",
                        "TextChannel",
                    ],
                ],
                Coroutine[Any, Any, None],
            ]
        ] = []

    def test(self, coro: CoroT) -> CoroT:
        """Decorator to register a test function to this object.

        Parameters
        ----------
        coro : (:class:`~discord.Client`, :class:`~discord.Client`, :class:`~discord.Guild`, tuple[:class:`~discord.TextChannel`, :class:`~discord.TextChannel`, :class:`~discord.TextChannel`, :class:`~discord.TextChannel`]) -> Coroutine[Any, Any, None]
            The test function to run. Must be a coroutine whose arguments are, respectively: the Bridge Bot client, the Tester Bot client, the Discord server for testing, and a tuple with four Discord text channels in that server as seen by the Tester Bot.

        Returns
        -------
        (:class:`~discord.Client`, :class:`~discord.Client`, :class:`~discord.Guild`, tuple[:class:`~discord.TextChannel`, :class:`~discord.TextChannel`, :class:`~discord.TextChannel`, :class:`~discord.TextChannel`]) -> Coroutine[Any, Any, None]
        """
        if not asyncio.iscoroutinefunction(coro):
            raise TypeError("Test registered must be a coroutine function.")

        setattr(self, coro.__name__, coro)
        self.tests.append(coro)
        logger.debug("%s has successfully been registered as a test.", coro.__name__)
        return coro


test_runner = TestRunner()
