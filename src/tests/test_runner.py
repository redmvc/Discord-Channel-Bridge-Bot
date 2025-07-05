import asyncio
from abc import ABC
from typing import Any, Callable, Coroutine, TypeVar

from discord import Client

from validations import setup_logger

logger = setup_logger("test_logger", "test_logs.log", "DEBUG")

CoroT = TypeVar("CoroT", bound=Callable[[Client, Client], Coroutine[Any, Any, None]])


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

    async def run_tests(self, bridge_bot: Client, tester_bot: Client):
        """Run the tests registered to this object.

        Parameters
        ----------
        bridge_bot : :class:`~discord.Client`
            The Bridge Bot client.
        tester_bot : :class:`~discord.Client`
            The Tester Bot client.
        """
        for test_case in self.test_cases:
            for test in test_case.tests:
                await test(bridge_bot, tester_bot)


class TestCase(ABC):
    """An abstract class to register test cases.

    Attributes
    ----------
    tests : list[(:class:`~discord.Client`, :class:`~discord.Client`) -> Coroutine[Any, Any, None]]
        The list of registered tests.
    """

    def __init__(self, test_base: TestRunner):
        test_base.register_test_case(self)
        self.tests: list[Callable[[Client, Client], Coroutine[Any, Any, None]]] = []

    def test(self, coro: CoroT) -> CoroT:
        """Decorator to register a test function to this object.

        Parameters
        ----------
        coro : (:class:`~discord.Client`, :class:`~discord.Client`) -> Coroutine[Any, Any, None]
            The test function to run. Must be a coroutine whose first argument is the Bridge Bot and whose second argument is the Tester Bot.

        Returns
        -------
        (:class:`~discord.Client`, :class:`~discord.Client`) -> Coroutine[Any, Any, None]
        """
        if not asyncio.iscoroutinefunction(coro):
            raise TypeError("Test registered must be a coroutine function.")

        setattr(self, coro.__name__, coro)
        self.tests.append(coro)
        logger.debug("%s has successfully been registered as a test.", coro.__name__)
        return coro


test_runner = TestRunner()
