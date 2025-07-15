import asyncio
import sys
from abc import ABC
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from re import finditer
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    Literal,
    Sequence,
    TypedDict,
    TypeVar,
    cast,
    overload,
)

import discord
import tester_bot
from aiolimiter import AsyncLimiter
from beartype import beartype
from tester_bot import logger

sys.path.append(str(Path(__file__).parent.parent))
import globals

if TYPE_CHECKING:
    from typing import NotRequired


# Helper to prevent us from being rate limited
rate_limiter = AsyncLimiter(1, 10)

webhook_permissions_role: discord.Role | None = None

test_function_type = Callable[
    [
        discord.Client,
        discord.Client,
        discord.Guild,
        tuple[
            discord.TextChannel,
            discord.TextChannel,
            discord.TextChannel,
            discord.TextChannel,
        ],
    ],
    Coroutine[Any, Any, list[str]],
]
CoroT = TypeVar("CoroT", bound=test_function_type)

failures: dict[str, list[str]] = {}


@beartype
def camel_case_split(
    string: str,
    *,
    join_str: str = " ",
    lowercase_after_first: bool = True,
    other_split_strs: list[str] = ["_"],
) -> str:
    """Split `string` along CamelCase divisions then return a string joining the result using `join_str`.

    Parameters
    ----------
    string : str
        The string to split and reijoin.
    join_str : str, optional
        Which string to use to join the split string. Defaults to " ".
    lowercase_after_first : bool, optional
        Whether to lowercase every word other than the first one. Defaults to True.
    other_split_strs : list[str], optional
        Other strings to split the string along. Defaults to a list containing only "_".

    Returns
    -------
    str

    Examples
    --------
    >>> camel_case_split("CamelCase_withUnderscore")
    'Camel case with underscore'
    >>> camel_case_split("CamelCase_withUnderscore*and*Asterisk", join_str=" & ", lowercase_after_first=False, other_split_strs=["_", "*"])
    'Camel & Case & with & Underscore & and & Asterisk'
    """
    matches = finditer(".+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)", string)
    split_str = [m.group(0) for m in matches]

    if lowercase_after_first or other_split_strs:
        for i, s in enumerate(split_str):
            if lowercase_after_first:
                s = s.lower()

            for split_char in other_split_strs:
                s = join_str.join(s.split(split_char))

            split_str[i] = s

        if lowercase_after_first:
            split_str[0] = string[0] + (
                split_str[0][1:] if (len(split_str[0]) > 1) else ""
            )

    return join_str.join(split_str)


@beartype
def log_expectation(
    message: str,
    type: Literal["success", "failure"],
    *,
    print_success_to_console: bool = False,
    print_failure_to_console: bool = True,
):
    """Log an expectation and optionally print it to console.

    Parameters
    ----------
    message : str
        The message to be logged.
    type : Literal["success", "failure"]
        Whether it's a success or a failure. Will add emoji to the start of the message depending on which.
    print_success_to_console : bool, optional
        Whether to also print a success message to console. Defaults to False.
    print_failure_to_console : bool, optional
        Whether to also print a failure message to console. Defaults to True.
    """
    if type == "failure":
        message = f"FAILURE: {message}"
        logger.error(message)
        message = f"❌ {message}"
        if print_failure_to_console:
            print(message)
    else:
        message = f"SUCCESS: {message}"
        logger.info(message)
        message = f"✅ {message}"
        if print_success_to_console:
            print(message)


@beartype
async def give_manage_webhook_perms(
    tester_bot: discord.Client,
    testing_server: discord.Guild,
):
    """Give a bot Manage Webhook permissions in a server.

    Parameters
    ----------
    tester_bot : :class:`~discord.Client`
        The tester bot client.
    testing_server : :class:`~discord.Guild`
        The testing server from the perspective of the bridge bot client.
    """
    await _give_or_remove_manage_webhook_perms(tester_bot, testing_server, give=True)


@beartype
async def remove_manage_webhook_perms(
    tester_bot: discord.Client,
    testing_server: discord.Guild,
):
    """Remove Manage Webhook permissions from a bot in a server.

    Parameters
    ----------
    tester_bot : :class:`~discord.Client`
        The tester bot client.
    testing_server : :class:`~discord.Guild`
        The testing server from the perspective of the bridge bot client.
    """
    await _give_or_remove_manage_webhook_perms(tester_bot, testing_server, give=False)


async def _give_or_remove_manage_webhook_perms(
    tester_bot: discord.Client,
    testing_server: discord.Guild,
    *,
    give: bool,
):
    """Gives the tester bot Manage Webhook permissions or take them away from it in the testing server.

    Parameters
    ----------
    tester_bot : :class:`~discord.Client`
        The tester bot client.
    testing_server : :class:`~discord.Guild`
        The testing server from the perspective of the bridge bot client.
    give : bool
        Whether to give or remove Manage Webhook permissions.
    """
    assert tester_bot.user
    tester_bot_member = await globals.get_server_member(
        testing_server,
        tester_bot.user.id,
    )

    global webhook_permissions_role
    if tester_bot_member and webhook_permissions_role:
        if give and (webhook_permissions_role not in tester_bot_member.roles):
            await tester_bot_member.add_roles(webhook_permissions_role)
        elif not give and (webhook_permissions_role in tester_bot_member.roles):
            await tester_bot_member.remove_roles(webhook_permissions_role)


@overload
async def create_bridge(
    source_channel: discord.TextChannel | discord.Thread | int,
    target_channel: discord.TextChannel | discord.Thread | int,
    *,
    direction: Literal["inbound", "outbound"] | None = None,
) -> None:
    """Create a bridge between `source_channel` and `target_channel` without sending a message in `source_channel` to do so.

    Parameters
    ----------
    source_channel : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
        The channel from which to create a bridge, or ID of same.
    target_channel : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
        The channel to which to create a bridge, or ID of same.
    direction : Literal["inbound", "outbound"] | None, optional
        The direction of bridge to create. If set to "inbound", will create a bridge from `target_channel` to `source_channel`; if set to "outbound", will create a bridge from `source_channel` to `target_channel`; if set to None, will create both. Defaults to None.
    """
    ...


@overload
async def create_bridge(
    source_channel: discord.TextChannel | discord.Thread | int,
    target_channel: discord.TextChannel | discord.Thread | int,
    *,
    direction: Literal["inbound", "outbound"] | None = None,
    send_message: Literal[True],
) -> discord.Message:
    """Send a message in `source_channel` to create a bridge between it and `target_channel`, then return that message.

    Parameters
    ----------
    source_channel : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
        The channel from which to create a bridge, or ID of same.
    target_channel : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
        The channel to which to create a bridge, or ID of same.
    direction : Literal["inbound", "outbound"] | None, optional
        The direction of bridge to create. If set to "inbound", will create a bridge from `target_channel` to `source_channel`; if set to "outbound", will create a bridge from `source_channel` to `target_channel`; if set to None, will create both. Defaults to None.

    Returns
    -------
    :class:`~discord.Message`
    """
    ...


@beartype
async def create_bridge(
    source_channel: discord.TextChannel | discord.Thread | int,
    target_channel: discord.TextChannel | discord.Thread | int,
    *,
    direction: Literal["inbound", "outbound"] | None = None,
    send_message: bool = False,
) -> discord.Message | None:
    """Create a bridge between `source_channel` and `target_channel` and, if a message was sent in `source_channel` to do so, return it.

    Parameters
    ----------
    source_channel : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
        The channel from which to create a bridge, or ID of same.
    target_channel : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
        The channel to which to create a bridge, or ID of same.
    direction : Literal["inbound", "outbound"] | None, optional
        The direction of bridge to create. If set to "inbound", will create a bridge from `target_channel` to `source_channel`; if set to "outbound", will create a bridge from `source_channel` to `target_channel`; if set to None, will create both. Defaults to None.
    send_message : bool, optional
        Whether to send an actual message in `source_channel` instead of faking it. Defaults to False.

    Returns
    -------
    :class:`~discord.Message` | None
    """

    target_channel_id = globals.get_id_from_channel(target_channel)

    command = f"/bridge {target_channel_id}{(' ' + direction) if direction else ''}"
    if send_message:
        source_channel = await globals.get_channel_from_id(
            source_channel,
            ensure_text_or_thread=True,
            bot_client=tester_bot.client,
        )
        return await source_channel.send(command)

    source_channel = await globals.get_channel_from_id(
        source_channel,
        ensure_text_or_thread=True,
        bot_client=globals.client,
    )
    message = tester_bot.FakeMessage(command, source_channel)
    assert globals.test_app
    if not await tester_bot.process_tester_bot_command(message, globals.test_app):
        raise Exception(f"{command} command failed to be executed")
    return None


@overload
async def demolish_bridges(
    source_channel: discord.TextChannel | discord.Thread | int,
) -> None:
    """Demolish all bridges to and from `source_channel` without sending a message in `source_channel` to do so..

    Parameters
    ----------
    source_channel : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
        The channel to and from which to demolish bridges, or ID of same.
    """
    ...


@overload
async def demolish_bridges(
    source_channel: discord.TextChannel | discord.Thread | int,
    *,
    send_message: Literal[True],
) -> discord.Message:
    """Send a message in `source_channel` to demolish all bridges to and from it, then return that message.

    Parameters
    ----------
    source_channel : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
        The channel to and from which to demolish bridges, or ID of same.

    Returns
    -------
    :class:`~discord.Message`
    """
    ...


@overload
async def demolish_bridges(
    source_channel: discord.TextChannel | discord.Thread | int,
    *,
    channel_and_threads: Literal[True],
) -> None:
    """Demolish all bridges to and from `source_channel`, as well as those to and from its threads (if it's a text channel) or to and from its parent channel and its parent channel's threads (if it's a thread), without sending a message in `source_channel` to do so.

    Parameters
    ----------
    source_channel : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
        The channel to and from which to demolish bridges, or ID of same.
    """
    ...


@overload
async def demolish_bridges(
    source_channel: discord.TextChannel | discord.Thread | int,
    *,
    channel_and_threads: Literal[True],
    send_message: Literal[True],
) -> discord.Message:
    """Send a message in `source_channel` to demolish all bridges to and from it, as well as those to and from its threads (if it's a text channel) or to and from its parent channel and its parent channel's threads (if it's a thread), then return that message.

    Parameters
    ----------
    source_channel : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
        The channel to and from which to demolish bridges, or ID of same.

    Returns
    -------
    :class:`~discord.Message`
    """
    ...


@overload
async def demolish_bridges(
    source_channel: discord.TextChannel | discord.Thread | int,
    target_channel: discord.TextChannel | discord.Thread | int,
) -> None:
    """Demolish the bridge between `source_channel` and `target_channel` without sending a message in `source_channel` to do so.

    Parameters
    ----------
    source_channel : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
        One of the channels to and from which to demolish bridges, or ID of same.
    target_channel : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
        The other channel to and from which to demolish bridges, or ID of same.
    """
    ...


@overload
async def demolish_bridges(
    source_channel: discord.TextChannel | discord.Thread | int,
    target_channel: discord.TextChannel | discord.Thread | int,
    *,
    send_message: Literal[True],
) -> discord.Message:
    """Send a message in `source_channel` to demolish the bridge between it and `target_channel`, then return that message.

    Parameters
    ----------
    source_channel : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
        One of the channels to and from which to demolish bridges, or ID of same.
    target_channel : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
        The other channel to and from which to demolish bridges, or ID of same.

    Returns
    -------
    :class:`~discord.Message`
    """
    ...


@beartype
async def demolish_bridges(
    source_channel: discord.TextChannel | discord.Thread | int,
    target_channel: discord.TextChannel | discord.Thread | int | None = None,
    *,
    channel_and_threads: bool = False,
    send_message: bool = False,
) -> discord.Message | None:
    """Demolish bridges to and from `source_channel` and, if a message was sent in `source_channel` to do so, return it.

    Parameters
    ----------
    source_channel : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
        The channel to and from which to demolish bridges, or ID of same.
    target_channel : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int | None, optional
        If this argument is provided, only bridges between it and `source_channel` will be destroyed. Defaults to None, in which case all bridges to and from `source_channel` will be demolished.
    channel_and_threads : bool, optional
        Whether to demolish all bridges, including those of the parent channel and/or channel threads. Only used if `target_channel` is None. Defaults to False.
    send_message : bool, optional
        Whether to send an actual message in `source_channel` instead of faking it. Defaults to False.

    Returns
    -------
    :class:`~discord.Message` | None
    """
    if target_channel:
        target_channel_id = globals.get_id_from_channel(target_channel)
        command = f"/demolish {target_channel_id}"
    else:
        command = f"/demolish_all{' True' if channel_and_threads else ''}"

    if send_message:
        source_channel = await globals.get_channel_from_id(
            source_channel,
            ensure_text_or_thread=True,
            bot_client=tester_bot.client,
        )
        return await source_channel.send(command)

    source_channel = await globals.get_channel_from_id(
        source_channel,
        ensure_text_or_thread=True,
        bot_client=globals.client,
    )
    message = tester_bot.FakeMessage(command, source_channel)
    assert globals.test_app
    if not await tester_bot.process_tester_bot_command(message, globals.test_app):
        raise Exception(f"{command} command failed to be executed")
    return None


class TestRunner:
    """A class that runs all registered tests.

    Attributes
    ----------
    test_cases : list[:class:`TestCase`]
        The list of registered test cases.
    """

    @beartype
    def __init__(self, bridge_bot: discord.Client, tester_bot: discord.Client):
        """Create a class to run all registered tests.

        Parameters
        ----------
        bridge_bot : :class:`~discord.Client`
            The Bridge Bot's client.
        tester_bot : :class:`~discord.Client`
            The Tester Bot's client.
        """
        self._test_cases: list["TestCase"] = []
        self.tester_bot = tester_bot
        self.bridge_bot = bridge_bot

    @beartype
    def register_test_case(self, test_case: "TestCase"):
        """Register a test case to this object.

        Parameters
        ----------
        test_case : :class:`~TestCase`
        """
        self._test_cases.append(test_case)
        logger.debug(
            "%s has successfully been registered as a test case.",
            type(test_case).__name__,
        )

    @property
    def test_cases(self) -> list["TestCase"]:
        """The test cases registered to this object."""
        return self._test_cases

    @beartype
    async def run_tests(self, testing_server: discord.Guild):
        """Run the tests registered to this object.

        Parameters
        ----------
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
                bridge_bot_testing_server := self.bridge_bot.get_guild(
                    testing_server.id
                )
            ):
                bridge_bot_testing_server = await self.bridge_bot.fetch_guild(
                    testing_server.id
                )
            testing_server = bridge_bot_testing_server
            logger.debug("Fetched.")

            # Register the test bot in globals
            assert self.tester_bot.user
            globals.test_app = await self.bridge_bot.fetch_user(self.tester_bot.user.id)

            # Create a role in the testing server with the necessary permissions
            global webhook_permissions_role
            webhook_permissions_role = await testing_server.create_role(
                name="webhook_permissions_role",
                permissions=discord.Permissions(manage_webhooks=True),
            )

            # Delete all channels in the server
            logger.info("Deleting server channels...")
            server_channels = await testing_server.fetch_channels()
            delete_channels: list[Coroutine[Any, Any, None]] = []
            await give_manage_webhook_perms(self.tester_bot, testing_server)
            for channel in server_channels:
                await demolish_bridges(channel.id, channel_and_threads=True)
                delete_channels.append(channel.delete())
            await asyncio.gather(*delete_channels)
            logger.info("Deleted.")

            # Create four channels for testing
            logger.info("Creating test channels...")
            create_testing_channels: list[Coroutine[Any, Any, discord.TextChannel]] = []
            for i in range(4):
                create_testing_channels.append(
                    testing_server.create_text_channel(f"testing_channel_{i + 1}")
                )
            testing_channels = tuple(
                self.tester_bot.get_channel(channel.id)
                for channel in await asyncio.gather(*create_testing_channels)
            )
            assert len(testing_channels) == 4
            if TYPE_CHECKING:
                testing_channels = cast(
                    tuple[
                        discord.TextChannel,
                        discord.TextChannel,
                        discord.TextChannel,
                        discord.TextChannel,
                    ],
                    testing_channels,
                )
            logger.info("Created.")

            # Run the tests
            logger.info("")
            logger.info("Running tests.")
            logger.info("")
            print("\nRunning tests.\n")
            for test_case in self.test_cases:
                test_case_name = type(test_case).__name__
                logger.info(f'Starting test case "{test_case_name}".')
                print(f"{camel_case_split(test_case_name)}...")
                test_case_had_failures = False
                for test in test_case.tests:
                    test_name = test.__name__
                    logger.info(f'Starting test "{test_name}".')
                    print(f"...{camel_case_split(test_name)}.")

                    tester_bot.received_messages = defaultdict(lambda: [])
                    try:
                        failure_messages = await test(
                            self.bridge_bot,
                            self.tester_bot,
                            testing_server,
                            testing_channels,
                        )
                        if failure_messages:
                            failures[test_name] = failure_messages
                    except Exception as e:
                        failure_messages = [
                            f"An error occurred while running the test: {e}"
                        ]
                        failures[test_name] = failure_messages
                        log_expectation(failure_messages[0], "failure")
                    logger.info("")

                    if failure_messages:
                        test_case_had_failures = True
                    else:
                        print("✅")

                if not test_case_had_failures:
                    log_expectation(
                        f'All "{camel_case_split(test_case_name)}" tests passed!',
                        "success",
                        print_success_to_console=True,
                    )
                logger.info("")
                print("")

            for ch in testing_channels:
                await demolish_bridges(ch, channel_and_threads=True)
            if webhook_permissions_role:
                await webhook_permissions_role.delete()
                webhook_permissions_role = None


class TestCase(ABC):
    """An abstract class to register test cases.

    Attributes
    ----------
    tests : list[(:class:`~discord.Client`, :class:`~discord.Client`, :class:`~discord.Guild`, tuple[:class:`~discord.TextChannel`, :class:`~discord.TextChannel`, :class:`~discord.TextChannel`, :class:`~discord.TextChannel`]) -> Coroutine[Any, Any, None]]
        The list of registered tests.
    """

    @beartype
    def __init__(self, test_runner: TestRunner):
        """Initialise a test case.

        Parameters
        ----------
        test_runner : :class:`~TestRunner`
            The test runner object to register this test case to.
        """
        test_runner.register_test_case(self)
        self._tests: list[test_function_type] = []

    @property
    def tests(self) -> list[test_function_type]:
        """The tests registered to this object."""
        return self._tests

    @beartype
    def test(self, coro: CoroT) -> CoroT:
        """Decorator to register a test function to this object.

        Parameters
        ----------
        coro : (:class:`~discord.Client`, :class:`~discord.Client`, :class:`~discord.Guild`, tuple[:class:`~discord.TextChannel`, :class:`~discord.TextChannel`, :class:`~discord.TextChannel`, :class:`~discord.TextChannel`]) -> Coroutine[Any, Any, list[str]]
            The test function to run. Must be a coroutine that returns a list with all of the failures in the test whose arguments are, respectively:
            - the Bridge Bot client;
            - the Tester Bot client;
            - the Discord server for testing as seen by the Bridge Bot;
            - and a tuple with four Discord text channels in that server as seen by the Tester Bot.

        Returns
        -------
        (:class:`~discord.Client`, :class:`~discord.Client`, :class:`~discord.Guild`, tuple[:class:`~discord.TextChannel`, :class:`~discord.TextChannel`, :class:`~discord.TextChannel`, :class:`~discord.TextChannel`]) -> Coroutine[Any, Any, list[str]]
        """
        if not asyncio.iscoroutinefunction(coro):
            raise TypeError("Test registered must be a coroutine function.")

        setattr(self, coro.__name__, coro)
        self._tests.append(coro)
        logger.debug("%s has successfully been registered as a test.", coro.__name__)
        return coro


class Expectation(TypedDict, total=False):
    pass


class MessageExpectation(Expectation, total=False):
    contain: "NotRequired[str]"
    not_contain: "NotRequired[str]"
    equal: "NotRequired[str]"
    not_equal: "NotRequired[str]"
    be_a_reply_to: "NotRequired[discord.Message]"
    not_be_a_reply_to: "NotRequired[discord.Message]"
    be_from: "NotRequired[int | discord.User | discord.Member | discord.Client]"
    not_be_from: "NotRequired[int | discord.User | discord.Member | discord.Client]"


class ExistingMessageExpectation(MessageExpectation, total=False):
    be_in_channel: "NotRequired[int | discord.TextChannel | discord.Thread]"


@overload
async def expect(
    obj: Literal["next_message"],
    *,
    in_channel: int | discord.TextChannel | discord.Thread,
    to: list[MessageExpectation] | MessageExpectation,
    timeout: float | int = 10,
    heartbeat: float | int = 0.5,
) -> tuple[discord.Message | None, list[str]]:
    """Check that a message will arrive in `in_channel` within `timeout` seconds. If it does, also check that the given list of expectations is true of it, then return a tuple whose first element is the message and whose second element is a list of all the failing tests; otherwise, return a tuple whose first element is None and whose second element is a list with the failing test.

    Parameters
    ----------
    obj : Literal["next_message"]
    in_channel : int | :class:`~discord.TextChannel` | :class:`~discord.Thread`
        A channel in which a message should be expected, or ID of same.
    to : list[:class:`~MessageExpectation`] | :class:`~MessageExpectation`
        A list of things to expect of that message. The valid expectations are: "contain", "not_contain", "equal", "not_equal", "be_a_reply_to", "not_be_a_reply_to", "be_from", and "not_be_from".
    timeout : float | int, optional
        How long to wait, in seconds, for the message to arrive. If set to less than 1, will be set to 1. Defaults to 10.
    heartbeat : float | int, optional
        How long to wait, in seconds, between each check that the expected event occurred. If set to less than 0.5, will be set to 0.5; if set to a value greater than `timeout`, will be set to `timeout - 0.5`. Defaults to 0.5.

    Returns
    -------
    tuple[:class:`~discord.Message` | None, list[str]]
    """
    ...


@overload
async def expect(
    obj: discord.Message,
    *,
    in_channel: int | discord.TextChannel | discord.Thread | None = None,
    to: list[ExistingMessageExpectation] | ExistingMessageExpectation,
) -> tuple[discord.Message, list[str]]:
    """Check that a given list of expectations is true of a message, then return a tuple whose first element is the message and whose second element is a list of all the failing tests.

    Parameters
    ----------
    obj : :class:`~discord.Message`
    in_channel : int | :class:`~discord.TextChannel` | :class:`~discord.Thread` | None, optional
        A channel in which the message should be expected. Equivalent to setting the "be_in_channel" expectation in `to`.
    to : list[:class:`~MessageExpectation`] | :class:`~MessageExpectation`
        A list of things to expect of that message. The valid expectations are: "contain", "not_contain", "equal", "not_equal", "be_a_reply_to", "not_be_a_reply_to", "be_from", "not_be_from", and "be_in_channel".

    Returns
    -------
    tuple[:class:`~discord.Message`, list[str]]
    """
    ...


@overload
async def expect(
    obj: Literal["no_new_message"],
    *,
    in_channel: int | discord.TextChannel | discord.Thread,
    timeout: float | int = 10,
    heartbeat: float | int = 0.5,
) -> tuple[None, list[str]]:
    """Check that no message will be sent in `in_channel` within the next `timeout` seconds. If it is not, return a tuple `(None, [])`; if it is, return a tuple whose first element is None and whose second element is a list with the test failure message.

    Parameters
    ----------
    obj : Literal["no_new_message"]
    in_channel : int | :class:`~discord.TextChannel` | :class:`~discord.Thread`
        The channel in which that no new message should be sent, or ID of same.
    timeout : float | int, optional
        How long to wait, in seconds, before declaring that no message was sent in `in_channel`. If set to less than 1, will be set to 1. Defaults to 10.
    heartbeat : float | int, optional
        How long to wait, in seconds, between each check for new messages. If set to less than 0.5, will be set to 0.5; if set to a value greater than `timeout`, will be set to `timeout - 0.5`. Defaults to 0.5.

    Returns
    -------
    tuple[None, list[str]]
    """
    ...


@overload
async def expect(
    obj: Literal["thread"],
    *,
    in_channel: int | discord.TextChannel,
    with_name: str,
    to: Literal["exist"],
    timeout: float | int = 10,
    heartbeat: float | int = 0.5,
) -> tuple[discord.Thread | None, list[str]]: ...


@overload
async def expect(
    obj: Literal["thread"],
    *,
    in_channel: int | discord.TextChannel,
    with_name: str,
    to: Literal["not_exist"],
    timeout: float | int = 10,
    heartbeat: float | int = 0.5,
) -> tuple[None, list[str]]: ...


@beartype
async def expect(
    obj: Literal["next_message", "no_new_message", "thread"] | discord.Message,
    *,
    in_channel: int | discord.TextChannel | discord.Thread | None = None,
    with_name: str | None = None,
    to: (
        Sequence[Expectation] | Expectation | Literal["exist", "not_exist"] | None
    ) = None,
    timeout: float | int = 10,
    heartbeat: float | int = 0.5,
) -> tuple[discord.Message | discord.Thread | None, list[str]]:
    """Check that a given list of expectations will be true of a certain object within `timeout` seconds, then return a tuple whose first element is the object (if it exists) and whose second element is the list of all failing tests.

    Parameters
    ----------
    obj : Literal["next_message", "no_new_message"] | :class:`~discord.Message`
        The object of which to expect things.
    in_channel : int | :class:`~discord.TextChannel` | :class:`~discord.Thread` | None, optional
        A channel in which that object should be expected, or ID of same. Defaults to None.
    with_name : str | None, optional
        The name the object should have. Defaults to None.
    to : Sequence[:class:`~Expectation`] | :class:`~Expectation` | Literal["exist", "not_exist"] | None, optional
        A list of things to expect of that object. Defaults to None.
    timeout : float | int, optional
        How long to wait, in seconds, for the expected event to occur. If set to less than 1, will be set to 1. Defaults to 10.
    heartbeat : float | int, optional
        How long to wait, in seconds, between each check that the expected event occurred. If set to less than 0.5, will be set to 0.5; if set to a value greater than `timeout`, will be set to `timeout - 0.5`. Defaults to 0.5.

    Returns
    -------
    tuple[:class:`~discord.Message` | :class:`~discord.Thread` | None, list[str]]
    """
    timeout = max(float(timeout), 1)
    heartbeat = min(max(float(heartbeat), 0.5), timeout - 0.5)

    if to is None:
        to = []
    elif not isinstance(to, Sequence):
        to = [to]

    if in_channel:
        in_channel = globals.get_id_from_channel(in_channel)

    if obj in ("next_message", "no_new_message"):
        assert in_channel

        end_time = datetime.now() + timedelta(seconds=timeout)
        while (
            not (received_messages := tester_bot.received_messages[in_channel])
        ) and (datetime.now() <= end_time):
            await asyncio.sleep(heartbeat)

        if not received_messages:
            if obj == "next_message":
                failure_message = [
                    f"expecting next message in channel <#{in_channel}> timed out"
                ]
                log_expectation(failure_message[0], "failure")
            else:
                failure_message = []
                log_expectation(
                    f"expected no new messages in channel <#{in_channel}>",
                    "success",
                )
            return (None, failure_message)

        received_message = tester_bot.received_messages[in_channel].pop(0)
        if obj == "no_new_message":
            failure_message = [
                f"expected no new messages in channel <#{in_channel}> but received at least one message instead: https://discord.com/channels/1/{in_channel}/{received_message.id}"
            ]
            log_expectation(failure_message[0], "failure")
            tester_bot.received_messages[in_channel] = []
            return (None, failure_message)

        obj = received_message
        log_expectation(
            f"expected next message in channel <#{in_channel}>: https://discord.com/channels/1/{in_channel}/{received_message.id}",
            "success",
        )
    elif obj == "thread":
        assert in_channel
        assert with_name
        assert isinstance(to, str)

        thread = None
        end_time = datetime.now() + timedelta(seconds=timeout)
        while (
            not (
                (created_threads := tester_bot.created_threads.get(in_channel))
                and (thread := created_threads.get(with_name))
            )
        ) and (datetime.now() <= end_time):
            await asyncio.sleep(heartbeat)

        message = f"expected{' no' if to == 'not_exist' else ''} thread named {with_name} to exist in channel <#{in_channel}>"

        if to == "not_exist":
            return_object = None
        else:
            return_object = thread

        if (thread and (to == "exist")) or ((not thread) and (to == "not_exist")):
            result = "success"
            failure_message = []
        else:
            result = "failure"
            failure_message = [message]

        log_expectation(message, result)
        return (return_object, failure_message)
    else:
        if in_channel:
            cast(ExistingMessageExpectation, to[0])["be_in_channel"] = in_channel

    assert not isinstance(to, str)

    content = obj.content
    expectations = [(e, v) for exp in to for e, v in exp.items()]
    failure_messages = []
    for expectation, value in expectations:
        if negation := expectation.startswith("not_"):
            expectation = expectation[4:]

        if expectation == "be_in_channel":
            if TYPE_CHECKING:
                assert isinstance(value, int | discord.TextChannel | discord.Thread)

            log_message = f"expected message to {' not' if negation else ''}be in channel <#{value}>"
            if ((message_channel_id := obj.channel.id) == value) != negation:
                log_expectation(log_message, "success")
            elif not negation:
                failure_message = (
                    f"{log_message} but it was actually in <#{message_channel_id}>"
                )
                failure_messages.append(failure_message)
                log_expectation(failure_message, "failure")
            else:
                failure_message = f"{log_message} but it was"
                failure_messages.append(failure_message)
                log_expectation(failure_message, "failure")
            continue

        if expectation == "be_a_reply_to":
            if TYPE_CHECKING:
                assert isinstance(value, discord.Message)

            log_message = f"expected message to {' not' if negation else ''}be a reply to message with ID {value.id}"
            if not (message_reference := obj.reference):
                message = f"{log_message} {'but' if not negation else 'and'} it was not a reply"
                if not negation:
                    failure_messages.append(message)
                    log_expectation(message, "failure")
                else:
                    log_expectation(message, "success")
            elif (reference_id := message_reference.message_id) != value.id:
                if not negation:
                    failure_message = f"{log_message} but it was a reply to message with ID {reference_id} instead"
                    failure_messages.append(failure_message)
                    log_expectation(failure_message, "failure")
                else:
                    log_expectation(log_message, "success")
            else:
                message = f"{log_message}{' but it was' if negation else ''}"
                if not negation:
                    log_expectation(message, "success")
                else:
                    failure_messages.append(message)
                    log_expectation(message, "failure")

            continue

        if expectation == "be_from":
            if isinstance(value, discord.Client):
                assert value.user
                value = value.user.id
            elif isinstance(value, discord.User | discord.Member):
                value = value.id

            log_message = f"expected message to {' not' if negation else ''}be from user with ID {value}"
            if value in [
                (application_id := obj.application_id),
                (author_id := obj.author.id),
            ]:
                message = f"{log_message}{' but it was' if negation else ''}"
                if not negation:
                    log_expectation(message, "success")
                else:
                    failure_messages.append(message)
                    log_expectation(message, "failure")
            else:
                message = f"{log_message}" + (
                    f" but it was from {application_id or author_id} instead"
                    if not negation
                    else ""
                )
                if negation:
                    log_expectation(message, "success")
                else:
                    failure_messages.append(message)
                    log_expectation(message, "failure")

            continue

        assert isinstance(value, str)
        if expectation == "contain":
            log_message = f"expected message to {' not' if negation else ''}contain text\n    {value}"
            if value in content:
                message = f"{log_message}{' but it did' if negation else ''}"
                if not negation:
                    log_expectation(message, "success")
                else:
                    failure_messages.append(message)
                    log_expectation(message, "failure")
            else:
                message = f"{log_message}" + (
                    f"\n  was instead:\n    {content}" if not negation else ""
                )
                if negation:
                    log_expectation(message, "success")
                else:
                    failure_messages.append(message)
                    log_expectation(message, "failure")
        elif expectation == "equal":
            log_message = (
                f"expected message to {' not' if negation else ''}equal\n    {value}"
            )
            if content == value:
                message = f"{log_message}{'  \nbut it did' if negation else ''}"
                if not negation:
                    log_expectation(message, "success")
                else:
                    failure_messages.append(message)
                    log_expectation(message, "failure")
            else:
                message = f"{log_message}" + (
                    f"\n  was instead:\n    {content}" if not negation else ""
                )
                if negation:
                    log_expectation(message, "success")
                else:
                    failure_messages.append(message)
                    log_expectation(message, "failure")

        # TODO: be ephemeral

    return (obj, failure_messages)


test_runner = TestRunner(globals.client, tester_bot.client)
