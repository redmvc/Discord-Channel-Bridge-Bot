import asyncio
import sys
from abc import ABC
from datetime import datetime, timedelta
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    Literal,
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

CoroT = TypeVar(
    "CoroT",
    bound=Callable[
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
        Coroutine[Any, Any, None],
    ],
)


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
    source_channel = await globals.get_channel_from_id(
        source_channel,
        ensure_text_or_thread=True,
        bot_client=tester_bot.client,
    )

    target_channel_id = globals.get_id_from_channel(target_channel)

    command = f"/bridge {target_channel_id}{' ' + direction if direction else ''}"
    if send_message:
        return await source_channel.send(command)

    message = tester_bot.FakeMessage(command, source_channel)
    assert globals.test_app
    await tester_bot.process_tester_bot_command(message, globals.test_app)
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
    source_channel = await globals.get_channel_from_id(
        source_channel,
        ensure_text_or_thread=True,
        bot_client=tester_bot.client,
    )
    if target_channel:
        target_channel_id = globals.get_id_from_channel(target_channel)
        command = f"/demolish {target_channel_id}"
    else:
        command = f"/demolish_all{' True' if channel_and_threads else ''}"

    if send_message:
        return await source_channel.send(command)

    message = tester_bot.FakeMessage(command, source_channel)
    assert globals.test_app
    await tester_bot.process_tester_bot_command(message, globals.test_app)
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
        self._test_cases: list["TestCase"] = []
        self.tester_bot = tester_bot
        self.bridge_bot = bridge_bot

    @beartype
    def register_test_case(self, test_case: "TestCase"):
        """Register a test case to this object.

        Parameters
        ----------
        test_case : :class:`TestCase`
        """
        self._test_cases.append(test_case)
        logger.debug(
            "%s has successfully been registered as a test case.",
            type(test_case).__name__,
        )

    @property
    def test_cases(self) -> list["TestCase"]:
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

            # Register the test bot in globals
            assert self.tester_bot.user
            globals.test_app = await self.bridge_bot.fetch_user(self.tester_bot.user.id)

            # Create a role in the testing server with the necessary permissions
            global webhook_permissions_role
            webhook_permissions_role = await testing_server.create_role(
                name="webhook_permissions_role",
                permissions=discord.Permissions(manage_webhooks=True),
            )

            # Run the tests
            logger.info("")
            logger.info("Running tests.")
            logger.info("")
            print("\nRunning tests.\n")
            for test_case in self.test_cases:
                logger.info(f"Starting test case {type(test_case).__name__}.")
                print(f"Starting test case {type(test_case).__name__}.")
                for test in test_case.tests:
                    logger.info(f"Starting test {test.__name__}.")
                    print(f"Starting test {test.__name__}.")

                    tester_bot.received_messages = {}
                    await test(
                        self.bridge_bot,
                        self.tester_bot,
                        testing_server,
                        testing_channels,
                    )
                    logger.info("")
                    print("")
                logger.info("")
                print("")

            if webhook_permissions_role:
                await webhook_permissions_role.delete()


class TestCase(ABC):
    """An abstract class to register test cases.

    Attributes
    ----------
    tests : list[(:class:`~discord.Client`, :class:`~discord.Client`, :class:`~discord.Guild`, tuple[:class:`~discord.TextChannel`, :class:`~discord.TextChannel`, :class:`~discord.TextChannel`, :class:`~discord.TextChannel`]) -> Coroutine[Any, Any, None]]
        The list of registered tests.
    """

    @beartype
    def __init__(self, test_base: TestRunner):
        test_base.register_test_case(self)
        self._tests: list[
            Callable[
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
                Coroutine[Any, Any, None],
            ]
        ] = []

    @property
    def tests(
        self,
    ) -> list[
        Callable[
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
            Coroutine[Any, Any, None],
        ]
    ]:
        return self._tests

    @beartype
    def test(self, coro: CoroT) -> CoroT:
        """Decorator to register a test function to this object.

        Parameters
        ----------
        coro : (:class:`~discord.Client`, :class:`~discord.Client`, :class:`~discord.Guild`, tuple[:class:`~discord.TextChannel`, :class:`~discord.TextChannel`, :class:`~discord.TextChannel`, :class:`~discord.TextChannel`]) -> Coroutine[Any, Any, None]
            The test function to run. Must be a coroutine whose arguments are, respectively: the Bridge Bot client, the Tester Bot client, the Discord server for testing as seen by the Bridge Bot, and a tuple with four Discord text channels in that server as seen by the Tester Bot.

        Returns
        -------
        (:class:`~discord.Client`, :class:`~discord.Client`, :class:`~discord.Guild`, tuple[:class:`~discord.TextChannel`, :class:`~discord.TextChannel`, :class:`~discord.TextChannel`, :class:`~discord.TextChannel`]) -> Coroutine[Any, Any, None]
        """
        if not asyncio.iscoroutinefunction(coro):
            raise TypeError("Test registered must be a coroutine function.")

        setattr(self, coro.__name__, coro)
        self._tests.append(coro)
        logger.debug("%s has successfully been registered as a test.", coro.__name__)
        return coro


@beartype
def log_expectation(
    message: str,
    type: Literal["success", "failure"],
    *,
    print_to_console: bool = True,
):
    """Log an expectation and optionally print it to console.

    Parameters
    ----------
    message : str
        The message to be logged.
    type : Literal["success", "failure"]
        Whether it's a success or a failure. Will add emoji to the start of the message depending on which.
    print_to_console : bool, optional
        Whether to also print the message to console. Defaults to True.
    """
    if type == "failure":
        message = f"FAILURE: {message}"
        logger.error(message)
        message = f"❌ {message}"
    else:
        message = f"SUCCESS: {message}"
        logger.info(message)
        message = f"✅ {message}"

    if print_to_console:
        print(message)


class Expectation(TypedDict, total=False):
    contain: "NotRequired[str]"
    not_contain: "NotRequired[str]"
    equal: "NotRequired[str]"
    not_equal: "NotRequired[str]"
    be_a_reply_to: "NotRequired[discord.Message]"
    not_be_a_reply_to: "NotRequired[discord.Message]"
    be_from: "NotRequired[int | discord.User | discord.Member | discord.Client]"
    not_be_from: "NotRequired[int | discord.User | discord.Member | discord.Client]"
    be_in_channel: "NotRequired[int | discord.TextChannel | discord.Thread]"


@overload
async def expect(
    obj: Literal["next_message"],
    *,
    in_channel: int | discord.TextChannel | discord.Thread,
    to: list[Expectation] | Expectation,
    timeout: float = 10,
    heartbeat: float = 0.5,
) -> discord.Message | None: ...


@overload
async def expect(
    obj: discord.Message,
    *,
    in_channel: int | discord.TextChannel | discord.Thread | None = None,
    to: list[Expectation] | Expectation,
    timeout: float = 10,
    heartbeat: float = 0.5,
) -> discord.Message | None: ...


@overload
async def expect(
    obj: Literal["no_new_message"],
    *,
    in_channel: int | discord.TextChannel | discord.Thread,
    to: None = None,
    timeout: float = 10,
    heartbeat: float = 0.5,
) -> None: ...


@beartype
async def expect(
    obj: Literal["next_message", "no_new_message"] | discord.Message,
    *,
    in_channel: int | discord.TextChannel | discord.Thread | None = None,
    to: list[Expectation] | Expectation | None = None,
    timeout: float = 10,
    heartbeat: float = 0.5,
) -> discord.Message | None:
    timeout = max(timeout, 1)
    heartbeat = min(max(heartbeat, 0.5), timeout - 0.5)

    if to is None:
        to = []
    elif not isinstance(to, list):
        to = [to]

    if in_channel:
        in_channel = globals.get_id_from_channel(in_channel)

    if obj in ("next_message", "no_new_message"):
        assert in_channel

        end_time = datetime.now() + timedelta(seconds=timeout)
        while not (received_messages := tester_bot.received_messages[in_channel]) and (
            datetime.now() <= end_time
        ):
            await asyncio.sleep(heartbeat)

        if not received_messages:
            if obj == "next_message":
                log_expectation(
                    f"expecting next message in channel <#{in_channel}> timed out",
                    "failure",
                )
            else:
                log_expectation(
                    f"expected no new messages in channel <#{in_channel}>",
                    "success",
                )
            return None
        elif obj == "no_new_message":
            log_expectation(
                f"expected no new messages in channel <#{in_channel}> but received at least one message instead",
                "failure",
            )
            return None

        obj = received_messages.pop(0)
        log_expectation(f"expected next message in channel <#{in_channel}>", "success")
    else:
        if in_channel:
            to[0]["be_in_channel"] = in_channel

    content = obj.content
    expectations = [(e, v) for exp in to for e, v in exp.items()]
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
                log_expectation(
                    f"{log_message} but it was actually in <#{message_channel_id}>",
                    "failure",
                )
            else:
                log_expectation(f"{log_message} but it was", "failure")
            continue

        if expectation == "be_a_reply_to":
            if TYPE_CHECKING:
                assert isinstance(value, discord.Message)

            log_message = f"expected message to {' not' if negation else ''}be a reply to message with ID {value.id}"
            if not (message_reference := obj.reference):
                log_expectation(
                    f"{log_message} {'but' if not negation else 'and'} it was not a reply",
                    "success" if negation else "failure",
                )
            elif (reference_id := message_reference.message_id) != value.id:
                if not negation:
                    log_expectation(
                        f"{log_message} but it was a reply to message with ID {reference_id} instead",
                        "failure",
                    )
                else:
                    log_expectation(log_message, "success")
            else:
                log_expectation(
                    f"{log_message}{' but it was' if negation else ''}",
                    "success" if not negation else "failure",
                )

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
                log_expectation(
                    f"{log_message}{' but it was' if negation else ''}",
                    "success" if not negation else "failure",
                )
            else:
                log_expectation(
                    (
                        f"{log_message}"
                        + (
                            f" but it was from {application_id or author_id} instead"
                            if not negation
                            else ""
                        )
                    ),
                    "success" if negation else "failure",
                )

            continue

        assert isinstance(value, str)
        if expectation == "contain":
            log_message = f"expected message to {' not' if negation else ''}contain text\n    {value}"
            if value in content:
                log_expectation(
                    f"{log_message}{' but it did' if negation else ''}",
                    "success" if not negation else "failure",
                )
            else:
                log_expectation(
                    (
                        f"{log_message}"
                        + (f"\n  was instead:\n    {content}" if not negation else "")
                    ),
                    "success" if negation else "failure",
                )
        elif expectation == "equal":
            log_message = (
                f"expected message to {' not' if negation else ''}equal\n    {value}"
            )
            if content == value:
                log_expectation(
                    f"{log_message}{'  \nbut it did' if negation else ''}",
                    "success" if not negation else "failure",
                )
            else:
                log_expectation(
                    (
                        f"{log_message}"
                        + (f"\n  was instead:\n    {content}" if not negation else "")
                    ),
                    "success" if negation else "failure",
                )

        # TODO: be ephemeral

    return obj


test_runner = TestRunner(globals.client, tester_bot.client)
