import asyncio
import inspect
import io
import json
from collections import defaultdict
from hashlib import md5
from typing import TYPE_CHECKING, Any, AsyncIterator, TypeVar, overload

import aiohttp
import discord
from aiolimiter import AsyncLimiter

from validations import (
    ArgumentError,
    ChannelTypeError,
    HTTPResponseError,
    TextChannelOrThread,
    logger,
    validate_channels,
)

if TYPE_CHECKING:
    from typing import Literal, NotRequired, SupportsInt, TypeAlias, TypedDict

    class SettingsRoot(TypedDict):
        """
        A TypedDict with the bot's settings. The `settings.json` file must contain a `"context"` entry whose value is another key in the file with the attributes defined in the `BridgeBotSettings` typed dictionary below. For example:

        .. code-block:: json
            {
                "context": "production",
                "production": {
                    "app_token": "...",
                    "db_dialect": "...",
                    ...
                },
                "development": {
                    "app_token": "...",
                    "db_dialect": "...",
                    ...
                }
            }

        Attributes
        ----------
        context : Literal['production', 'development']
            The current execution context of the bot. The default template includes "production" and "development" entries.
        production : BridgeBotSettings
            The production bot settings.
        development : BridgeBotSettings
            The development bot settings.
        tests : NotRequired[TestBotSettings]
            Optionally, settings for the bot set up for unit/integration testing, if such a bot is set up.
        """

        context: Literal["production", "development"]
        production: "BridgeBotSettings"
        development: "BridgeBotSettings"
        tests: NotRequired["TestBotSettings"]

    class BridgeBotSettings(TypedDict):
        """
        A TypedDict with the bot's settings. The `settings.json` file must contain a `"context"` entry whose value is another key in the file with the attributes below. For example:

        .. code-block:: json
            {
                "context": "production",
                "production": {
                    "app_token": "...",
                    "db_dialect": "...",
                    ...
                },
                "development": {
                    "app_token": "...",
                    "db_dialect": "...",
                    ...
                }
            }

        Attributes
        ----------
        app_token : str
            The token used by the Discord developers API.
        db_dialect : Literal['mysql'] | Literal['postgresql'] | Literal['sqlite']
            The database dialect.
        db_driver : Literal['aiomysql'] | Literal['asyncpg'] | Literal['aiosqlite']
            The async database driver. Must match the dialect: ``'aiomysql'`` for MySQL, ``'asyncpg'`` for PostgreSQL, ``'aiosqlite'`` for SQLite.
        db_host : str
            The server host.
        db_port : int
            The server port.
        db_user : str
            The root username.
        db_pwd : str
            The root password.
        db_name : str
            The database name.
        emoji_server_id : NotRequired[SupportsInt | str]
            The ID of a Discord server for storing custom emoji. The bot must have `Create Expressions` and `Manage Expressions` permissions in the server.
        whitelisted_apps : NotRequired[list[SupportsInt | str]]
            A list of IDs of applications whose outputs are bridged.
        """

        app_token: str
        db_dialect: Literal["mysql", "postgresql", "sqlite"]
        db_driver: Literal["aiomysql", "asyncpg", "aiosqlite"]
        db_host: str
        db_port: int
        db_user: str
        db_pwd: str
        db_name: str
        emoji_server_id: NotRequired[SupportsInt | str]
        whitelisted_apps: NotRequired[list[SupportsInt | str]]

    class TestBotSettings(TypedDict):
        """A TypedDict with the bot's settings. The `settings.json` file may contain a `"tests"` entry with the attributes below. For example:

        .. code-block:: json
            {
                "...",
                "tests": {
                    "app_token": "...",
                    "testing_server_id": "..."
                }
            }

        Furthermore, this bot must be in the Bridge Bot's whitelist.

        Attributes
        ----------
        app_token : str
            The token used by the Discord developers API.
        testing_server_id : SupportsInt | str
            The ID of a Discord server to run testing. The Bridge Bot itself must have administrator permissions in it but the unit testing bot must not.
        """

        app_token: str
        testing_server_id: SupportsInt | str


settings_root: "SettingsRoot" = json.load(open("settings.json"))
settings = settings_root[settings_root["context"]]
if whitelisted_apps := settings.get("whitelisted_apps"):
    settings["whitelisted_apps"] = [int(app_id) for app_id in whitelisted_apps]

# Variables for connection to the Discord client
client = discord.Client(
    intents=discord.Intents(
        emojis_and_stickers=True,
        guilds=True,
        members=True,
        message_content=True,
        messages=True,
        reactions=True,
        typing=True,
        webhooks=True,
    )
)
command_tree = discord.app_commands.CommandTree(client)

# This one is set to True once the bot has been initialised in main.py
is_ready: bool = False

# Set to True when the bot is connected and false when it's disconnected
is_connected: bool = False

# Channels which will automatically create threads in bridged channels
auto_bridge_thread_channels: set[int] = set()

# Server which can be used to store unknown emoji for mirroring reactions
emoji_server: discord.Guild | None = None

# User referring to a bot for unit tests, should only be set by the testing procedures
test_app: discord.User | None = None

# Dictionary listing all apps whitelisted per channel
per_channel_whitelist: dict[int, set[int]] = defaultdict(set)

# Helper to prevent us from being rate limited
rate_limiter = AsyncLimiter(1, 10)

# Rate limiter for startup procedures (30 requests per second to Discord API)
startup_rate_limiter = AsyncLimiter(30, 1)

# Variable to keep track of messages that are still being bridged/edited before they can be edited/deleted
message_lock: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)

# Variable to keep track of messages that need to be deleted
# mostly used in case we receive a deletion event prior to a message being bridged
messages_to_delete: set[int] = set()

# Variable to keep track of messages that need to be edited
# mostly used in case we receive an edit event prior to a message being bridged
messages_to_edit: dict[int, discord.RawMessageUpdateEvent] = {}

# Variable to keep track of channels that are being sent messages to to try to preserve ordering
channel_lock: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)

# Cache of pinned message IDs per channel, for detecting what changed on pin update events
pinned_messages_cache: dict[int, set[int]] = {}

# Counter of self-triggered pin/unpin events to ignore per channel
expected_pin_changes: dict[int, int] = defaultdict(int)

# Type wildcard
T = TypeVar("T", bound=Any)

DiscordChannel: "TypeAlias" = (
    discord.abc.GuildChannel
    | discord.abc.PrivateChannel
    | discord.Thread
    | discord.PartialMessageable
)
CH = TypeVar("CH", bound=DiscordChannel)


@overload
async def get_channel_from_id(
    channel_or_id: int,
    *,
    bot_client: discord.Client | None = None,
) -> DiscordChannel | None:
    """Return a channel with the ID passed as argument, or None if it couldn't be found.

    Parameters
    ----------
    channel_or_id : int
        The ID of a channel.
    bot_client : :class:`~discord.Client` | None, optional
        The client of the bot from whose perspective to fetch the channel. Defaults to None, in which case the Bridge Bot's client will be used.

    Returns
    -------
    :class:`~discord.abc.GuildChannel` | :class:`~discord.abc.PrivateChannel` | :class:`~discord.Thread` | :class:`~discord.PartialMessageable` | None

    Raises
    ------
    :class:`~discord.InvalidData`
        An unknown channel type was received from Discord when trying to find a channel from the ID.
    :class:`~discord.HTTPException`
        Retrieving a channel from the ID failed.
    :class:`~discord.NotFound`
        Invalid channel ID.
    :class:`~discord.Forbidden`
        The client does not not have permission to fetch the channel with that ID.
    """
    pass


@overload
async def get_channel_from_id(
    channel_or_id: int,
    *,
    ensure_text_or_thread: "Literal[False]",
    bot_client: discord.Client | None = None,
) -> DiscordChannel | None:
    """Return a channel with the ID passed as argument, or None if it couldn't be found.

    Parameters
    ----------
    channel_or_id : int
        The ID of a channel.
    ensure_text_or_thread : bool, optional
        Whether to assert that the channel is either a Discord text channel or a Thread before returning. Defaults to False.
    bot_client : :class:`~discord.Client` | None, optional
        The client of the bot from whose perspective to fetch the channel. Defaults to None, in which case the Bridge Bot's client will be used.

    Returns
    -------
    :class:`~discord.abc.GuildChannel` | :class:`~discord.abc.PrivateChannel` | :class:`~discord.Thread` | :class:`~discord.PartialMessageable` | None

    Raises
    ------
    :class:`~discord.InvalidData`
        An unknown channel type was received from Discord when trying to find a channel from the ID.
    :class:`~discord.HTTPException`
        Retrieving a channel from the ID failed.
    :class:`~discord.NotFound`
        Invalid channel ID.
    :class:`~discord.Forbidden`
        The client does not not have permission to fetch the channel with that ID.
    """
    pass


@overload
async def get_channel_from_id(
    channel_or_id: int,
    *,
    ensure_text_or_thread: "Literal[True]",
    bot_client: discord.Client | None = None,
) -> TextChannelOrThread:
    """Return the TextChannel or Thread with the ID passed as argument, or None if it couldn't be found.

    Parameters
    ----------
    channel_or_id : int
        The ID of a channel.
    ensure_text_or_thread : bool, optional
        Whether to assert that the channel is either a Discord text channel or a Thread before returning. Defaults to False.
    bot_client : :class:`~discord.Client` | None, optional
        The client of the bot from whose perspective to fetch the channel. Defaults to None, in which case the Bridge Bot's client will be used.

    Returns
    -------
    :class:`~discord.TextChannel` | :class:`~discord.Thread`

    Raises
    ------
    ChannelTypeError
        The channel with the ID passed as argument is not a Discord text channel or a Thread.
    :class:`~discord.InvalidData`
        An unknown channel type was received from Discord when trying to find a channel from the ID.
    :class:`~discord.HTTPException`
        Retrieving a channel from the ID failed.
    :class:`~discord.NotFound`
        Invalid channel ID.
    :class:`~discord.Forbidden`
        The client does not not have permission to fetch the channel with that ID.
    """
    pass


@overload
async def get_channel_from_id(
    channel_or_id: CH,
    *,
    bot_client: discord.Client | None = None,
) -> CH:
    """Return the channel passed as argument.

    Parameters
    ----------
    channel_or_id : :class:`~discord.abc.GuildChannel` | :class:`~discord.abc.PrivateChannel` | :class:`~discord.Thread` | :class:`~discord.PartialMessageable`
        A Discord channel.
    bot_client : :class:`~discord.Client` | None, optional
        The client of the bot from whose perspective to fetch the channel. Defaults to None, in which case the Bridge Bot's client will be used.

    Returns
    -------
    :class:`~discord.abc.GuildChannel` | :class:`~discord.abc.PrivateChannel` | :class:`~discord.Thread` | :class:`~discord.PartialMessageable`
    """
    pass


@overload
async def get_channel_from_id(
    channel_or_id: CH,
    *,
    ensure_text_or_thread: "Literal[False]",
    bot_client: discord.Client | None = None,
) -> CH:
    """Return the channel passed as argument.

    Parameters
    ----------
    channel_or_id : :class:`~discord.abc.GuildChannel` | :class:`~discord.abc.PrivateChannel` | :class:`~discord.Thread` | :class:`~discord.PartialMessageable`
        A Discord channel.
    ensure_text_or_thread : bool, optional
        Whether to assert that the channel is either a Discord text channel or a Thread before returning. Defaults to False.
    bot_client : :class:`~discord.Client` | None, optional
        The client of the bot from whose perspective to fetch the channel. Defaults to None, in which case the Bridge Bot's client will be used.

    Returns
    -------
    :class:`~discord.abc.GuildChannel` | :class:`~discord.abc.PrivateChannel` | :class:`~discord.Thread` | :class:`~discord.PartialMessageable`
    """
    pass


@overload
async def get_channel_from_id(
    channel_or_id: DiscordChannel,
    *,
    ensure_text_or_thread: "Literal[True]",
    bot_client: discord.Client | None = None,
) -> TextChannelOrThread:
    """Return the channel passed as argument.

    Parameters
    ----------
    channel_or_id : :class:`~discord.TextChannel` | :class:`~discord.Thread`
        A Discord channel.
    ensure_text_or_thread : bool, optional
        Whether to assert that the channel is either a Discord text channel or a Thread before returning. Defaults to False.
    bot_client : :class:`~discord.Client` | None, optional
        The client of the bot from whose perspective to fetch the channel. Defaults to None, in which case the Bridge Bot's client will be used.

    Returns
    -------
    :class:`~discord.TextChannel` | :class:`~discord.Thread`

    Raises
    ------
    ChannelTypeError
        The channel passed as argument is not a Discord text channel or a Thread.
    """
    pass


async def get_channel_from_id(
    channel_or_id: DiscordChannel | int,
    *,
    ensure_text_or_thread: bool = False,
    bot_client: discord.Client | None = None,
) -> DiscordChannel | None:
    """If the argument is a channel, return it unchanged; otherwise, return a channel with the ID passed as argument, or None if it couldn't be found.

    Parameters
    ----------
    channel_or_id : :class:`~discord.abc.GuildChannel` | :class:`~discord.abc.PrivateChannel` | :class:`~discord.Thread` | :class:`~discord.PartialMessageable` | int
        Either a Discord channel or an ID of same.
    ensure_text_or_thread : bool, optional
        Whether to assert that the channel is either a Discord text channel or a Thread before returning. Defaults to False.
    bot_client : :class:`~discord.Client` | None, optional
        The client of the bot from whose perspective to fetch the channel. Defaults to None, in which case the Bridge Bot's client will be used. If this argument is not None and `channel_or_id` is a channel, will get the channel's ID and then try to fetch it from `bot_client`'s perspective.

    Returns
    -------
    :class:`~discord.abc.GuildChannel` | :class:`~discord.abc.PrivateChannel` | :class:`~discord.Thread` | :class:`~discord.PartialMessageable` | None

    Raises
    ------
    ChannelTypeError
        `ensure_text_or_thread` was set to True but the channel or ID passed as argument does not refer to a Discord text channel or a Thread.
    :class:`~discord.InvalidData`
        An unknown channel type was received from Discord when trying to find a channel from the ID.
    :class:`~discord.HTTPException`
        Retrieving a channel from the ID failed.
    :class:`~discord.NotFound`
        Invalid channel ID.
    :class:`~discord.Forbidden`
        The client does not not have permission to fetch the channel with that ID.
    """
    if (bot_client is not None) and (not isinstance(channel_or_id, int)):
        channel_or_id = channel_or_id.id
    global client
    bot_client = bot_client or client

    if isinstance(channel_or_id, int):
        channel = bot_client.get_channel(channel_or_id)
        if not channel:
            try:
                channel = await bot_client.fetch_channel(channel_or_id)
            except Exception:
                channel = None
    else:
        channel = channel_or_id

    if ensure_text_or_thread:
        try:
            assert isinstance(channel, TextChannelOrThread)
        except AssertionError:
            raise ChannelTypeError(
                "`ensure_text_or_thread` was set to True but the channel or ID passed as argument does not refer to a Discord text channel or a Thread."
            )

    return channel


@overload
def get_id_from_channel(channel_or_id: int) -> int:
    """Return the argument unchanged.

    Parameters
    ----------
    channel_or_id : int
        The ID of a Discord channel.

    Returns
    -------
    int
    """
    pass


@overload
def get_id_from_channel(channel_or_id: DiscordChannel) -> int:
    """Return the ID of the Discord channel passed as argument.

    Parameters
    ----------
    channel_or_id : :class:`~discord.abc.GuildChannel` | :class:`~discord.abc.PrivateChannel` | :class:`~discord.Thread` | :class:`~discord.PartialMessageable`
        A Discord channel.

    Returns
    -------
    int
    """
    pass


def get_id_from_channel(channel_or_id: DiscordChannel | int) -> int:
    """Return the ID of the Discord channel passed as argument, or the argument itself if it is already an ID.

    Parameters
    ----------
    channel_or_id : :class:`~discord.abc.GuildChannel` | :class:`~discord.abc.PrivateChannel` | :class:`~discord.Thread` | :class:`~discord.PartialMessageable` | int
        A Discord channel.

    Returns
    -------
    int
    """
    if isinstance(channel_or_id, int):
        return channel_or_id

    return channel_or_id.id


@overload
async def get_channel_parent(channel_or_id: int) -> discord.TextChannel:
    """Fetch the channel the ID passed as argument matches and return its parent or the channel itself if it does not have a parent. Raises a ChannelTypeError if the channel referred to by the argument is not a Discord text channel or a thread off one.

    Parameters
    ----------
    channel_or_id : int
        The ID of a Discord channel.

    Returns
    -------
    :class:`~discord.TextChannel`

    Raises
    ------
    ChannelTypeError
        The ID passed as argument does not refer to a Discord text channel or a Thread.
    """
    pass


@overload
async def get_channel_parent(channel_or_id: DiscordChannel) -> discord.TextChannel:
    """Return the parent channel of the argument, or the argument itself if it does not have a parent. Raises a ChannelTypeError if the channel passed as argument is not a Discord text channel or a thread off one.

    Parameters
    ----------
    channel_or_id : :class:`~discord.abc.GuildChannel` | :class:`~discord.abc.PrivateChannel` | :class:`~discord.Thread` | :class:`~discord.PartialMessageable`
        A Discord channel.

    Returns
    -------
    :class:`~discord.TextChannel`

    Raises
    ------
    ChannelTypeError
        The channel passed as argument is not a Discord text channel or a thread off one.
    """
    pass


async def get_channel_parent(
    channel_or_id: DiscordChannel | int,
) -> discord.TextChannel:
    """Return the parent channel of the argument or the channel it refers to, or the argument itself if it does not have a parent. Raises a ChannelTypeError if the channel passed as argument is not a Discord text channel, a thread off one or the ID of one of those.

    Parameters
    ----------
    channel_or_id : :class:`~discord.abc.GuildChannel` | :class:`~discord.abc.PrivateChannel` | :class:`~discord.Thread` | :class:`~discord.PartialMessageable` | int
        Either a Discord channel or an ID of same.

    Returns
    -------
    :class:`~discord.TextChannel`

    Raises
    ------
    ChannelTypeError
        The channel or ID passed as argument does not refer to a Discord text channel or a thread off one.
    """
    channel = await get_channel_from_id(channel_or_id, ensure_text_or_thread=True)

    if isinstance(channel, discord.Thread):
        channel = channel.parent

    if not isinstance(channel, discord.TextChannel):
        raise ChannelTypeError(
            "The channel or ID passed as argument does not refer to a Discord text channel or a Thread."
        )

    return channel


async def get_channel_guild_id(
    channel_or_id: DiscordChannel | int,
    *,
    bot_client: discord.Client | None = None,
) -> int | None:
    """Return the ID of the Discord server the channel given by `channel_or_id` is in, if any, or None if there isn't any.

    Parameters
    ----------
    channel_or_id : :class:`~discord.abc.GuildChannel` | :class:`~discord.abc.PrivateChannel` | :class:`~discord.Thread` | :class:`~discord.PartialMessageable` | int
        Either a Discord channel or an ID of same.
    bot_client : :class:`~discord.Client` | None, optional
        The client of the bot from whose perspective to fetch the channel. Defaults to None, in which case the Bridge Bot's client will be used. If this argument is not None and `channel_or_id` is a channel, will get the channel's ID and then try to fetch it from `bot_client`'s perspective.

    Returns
    -------
    int | None
    """
    try:
        channel = await get_channel_from_id(channel_or_id, bot_client=bot_client)
    except Exception:
        return None

    return (
        channel
        and (not isinstance(channel, discord.abc.PrivateChannel))
        and channel.guild
        and channel.guild.id
    ) or None


async def channel_is_in_guild(
    channel_or_id: DiscordChannel | int,
    guild_or_id: discord.Guild | int,
    *,
    bot_client: discord.Client | None = None,
) -> bool:
    """Return True if `channel_or_id` is a channel (or an ID of same) in `guild_or_id` (or a server with it as ID) and False otherwise.

    Parameters
    ----------
    channel_or_id : :class:`~discord.abc.GuildChannel` | :class:`~discord.abc.PrivateChannel` | :class:`~discord.Thread` | :class:`~discord.PartialMessageable` | int
        Either a Discord channel or an ID of same.
    guild_or_id : :class:`~discord.Guild` | int
        Either a Discord server or an ID of same.
    bot_client : :class:`~discord.Client` | None, optional
        The client of the bot from whose perspective to fetch the channel. Defaults to None, in which case the Bridge Bot's client will be used. If this argument is not None and `channel_or_id` is a channel, will get the channel's ID and then try to fetch it from `bot_client`'s perspective.

    Returns
    -------
    bool
    """
    try:
        channel = await get_channel_from_id(channel_or_id, bot_client=bot_client)
    except Exception:
        return False

    if not channel:
        return False

    guild_id = guild_or_id if isinstance(guild_or_id, int) else guild_or_id.id
    channel_guild_id = await get_channel_guild_id(channel, bot_client=bot_client)
    return guild_id == channel_guild_id


async def get_channel_member(
    channel: discord.abc.GuildChannel | discord.Thread,
    member_id: int,
) -> discord.Member | None:
    """Return a channel's member by their ID, or None if they can't be found.

    Parameters
    ----------
    channel : :class:`~discord.abc.GuildChannel` | :class:`~discord.Thread`
        A Discord channel in a server.
    member_id : int
        The ID of the channel member.

    Returns
    -------
    :class:`~discord.Member` | None

    Raises
    ------
    :class:`~discord.HTTPException`
        Fetching the member failed.
    :class:`~discord.NotFound`
        The member could not be found.
    :class:`~discord.Forbidden`
        The client does not not have access to the server the channel is in.
    """
    return await get_server_member(channel.guild, member_id)


async def get_server_member(
    server: discord.Guild,
    member_id: int,
) -> discord.Member | None:
    """Return a server's member by their ID, or None if they can't be found.

    Parameters
    ----------
    server : :class:`~discord.abc.GuildChannel` | :class:`~discord.Thread`
        A Discord server.
    member_id : int
        The ID of the server member.

    Returns
    -------
    :class:`~discord.Member` | None

    Raises
    ------
    :class:`~discord.HTTPException`
        Fetching the member failed.
    :class:`~discord.NotFound`
        The member could not be found.
    :class:`~discord.Forbidden`
        The client does not not have access to the server the channel is in.
    """
    server_member = server.get_member(member_id)
    if not server_member:
        try:
            server_member = await server.fetch_member(member_id)
        except Exception:
            return None

    return server_member


async def get_users_from_iterator(
    user_iterator: AsyncIterator[discord.Member | discord.User],
) -> set[int]:
    """Run an asynchronous for loop on an iterator of users and return a set with the ID of every user that is not the bot itself in that iterator.

    Parameters
    ----------
    user_iterator : :class:`~typing.AsyncIterator`[:class:`~discord.Member`  |  :class:`~discord.User`]
        The asynchronous iterator.

    Returns
    -------
    set[int]
    """
    user_ids: set[int] = set()
    if client.user:
        bot_user_id = client.user.id
    else:
        bot_user_id = None
    async for user in user_iterator:
        if user.id != bot_user_id:
            user_ids.add(user.id)
    return user_ids


@overload
async def get_emoji_information(
    emoji: discord.PartialEmoji | discord.Emoji,
    *,
    emoji_size: int | None = 96,
) -> tuple[int, str, bool, str]:
    """Process the custom emoji passed as argument and return a tuple whose elements are:
    - its ID;
    - its name;
    - whether the emoji is animated;
    - and the URL for its image.

    Parameters
    ----------
    emoji : :class:`~discord.PartialEmoji` | :class:`~discord.Emoji`
        A custom Discord emoji.
    emoji_size : int | None, optional
        A specific emoji size to get the URL for. If set to None, will not limit the emoji size. Defaults to 96.

    Returns
    -------
    tuple[int, str, bool, str]

    Raises
    ------
    ValueError
        `emoji` had type :class:`~discord.PartialEmoji` but it was not a custom emoji.
    """
    pass


@overload
async def get_emoji_information(
    emoji: None,
    emoji_id: int | str,
    *,
    emoji_size: int | None = 96,
) -> tuple[int, str, bool, str]:
    """Process the custom emoji passed as argument and return a tuple whose elements are:
    - its ID;
    - its name;
    - whether the emoji is animated;
    - and the URL for its image.

    Parameters
    ----------
    emoji_id : int | str
        The ID of a a custom emoji.
    emoji_size : int | None, optional
        A specific emoji size to get the URL for. If set to None, will not limit the emoji size. Defaults to 96.

    Returns
    -------
    tuple[int, str, bool, str]

    Raises
    ------
    ArgumentError
        The client couldn't find an accessible emoji with ID `emoji_id`.
    ValueError
        `emoji_id` argument had type `str` but it was not a valid numerical ID.
    """
    pass


@overload
async def get_emoji_information(
    emoji: None,
    emoji_id: int | str,
    emoji_name: str,
    *,
    emoji_size: int | None = 96,
) -> tuple[int, str, bool, str]:
    """Process the custom emoji passed as argument and return a tuple whose elements are:
    - its ID;
    - its name;
    - whether the emoji is animated;
    - and the URL for its image.

    Parameters
    ----------
    emoji_id : int | str
        The ID of a a custom emoji.
    emoji_name : str
        The name of the emoji. It must start with the string "a:" if the emoji is animated.
    emoji_size : int | None, optional
        A specific emoji size to get the URL for. If set to None, will not limit the emoji size. Defaults to 96.

    Returns
    -------
    tuple[int, str, bool, str]

    Raises
    ------
    ValueError
        `emoji_id` had type `str` but it was not a valid numerical ID.
    """
    pass


@overload
async def get_emoji_information(
    emoji: discord.PartialEmoji | discord.Emoji | None = None,
    emoji_id: int | str | None = None,
    emoji_name: str | None = None,
    *,
    emoji_size: int | None = 96,
) -> tuple[int, str, bool, str]:
    pass


async def get_emoji_information(
    emoji: discord.PartialEmoji | discord.Emoji | None = None,
    emoji_id: int | str | None = None,
    emoji_name: str | None = None,
    *,
    emoji_size: int | None = 96,
) -> tuple[int, str, bool, str]:
    """Process the custom emoji passed as argument and return a tuple whose elements are:
    - its ID;
    - its name;
    - whether the emoji is animated;
    - and the URL for its image.

    Parameters
    ----------
    emoji : :class:`~discord.PartialEmoji` | :class:`~discord.Emoji` | None, optional
        A custom Discord emoji. Defaults to None, in which case `emoji_id` and `emoji_name` are used instead.
    emoji_id : int | str | None, optional
        The ID of a custom emoji. Defaults to None. Only used if `emoji` is not present.
    emoji_name : str | None, optional
        The name of the emoji. Defaults to None, in which case the client will try to find an emoji with ID `emoji_id`. If it's included, it must start with the string "a:" if the emoji animated. Only used if `emoji` is not present.
    emoji_size : int | None, optional
        A specific emoji size to get the URL for. If set to None, will not limit the emoji size. Defaults to 96.

    Returns
    -------
    tuple[int, str, bool, str]

    Raises
    ------
    ArgumentError
        Neither `emoji` nor `emoji_id` were passed, or `emoji_id` was passed, `emoji` and `emoji_name` weren't, and the client couldn't find an accessible emoji with ID `emoji_id`.
    ValueError
        `emoji` argument was passed and had type `PartialEmoji` but it was not a custom emoji, or `emoji_id` argument was passed and had type `str` but it was not a valid numerical ID.
    """
    if emoji:
        if not emoji.id:
            err = ValueError(
                f"Error in function {inspect.stack()[1][3]}(): PartialEmoji passed as argument to get_emoji_information() is not a custom emoji."
            )
            logger.error(err)
            raise err

        emoji_id = emoji.id
        emoji_name = emoji.name
        emoji_animated = emoji.animated
    else:
        if not emoji_id:
            err = ArgumentError(
                f"Error in function {inspect.stack()[1][3]}(): at least one of emoji or emoji_id must be passed as argument to get_emoji_information()."
            )
            logger.error(err)
            raise err

        try:
            emoji_id = int(emoji_id)
        except ValueError:
            err = ValueError(
                f"Error in function {inspect.stack()[1][3]}(): emoji_id was passed as an argument to get_emoji_information() and had type str but was not convertible to an ID."
            )
            logger.error(err)
            raise err

        if emoji_name:
            if emoji_animated := emoji_name.startswith("a:"):
                emoji_name = emoji_name[2:]
            elif emoji_name.startswith(":"):
                emoji_name = emoji_name[1:]
        else:
            # If I don't have the name for the emoji, I'll try to find it
            for e in client.emojis:
                if e.id == emoji_id:
                    emoji = e
                    break

            if not emoji:
                emoji = client.get_emoji(emoji_id)

            if not emoji:
                try:
                    emoji = await client.fetch_application_emoji(emoji_id)
                except Exception:
                    err = ArgumentError(
                        f"Error in function {inspect.stack()[1][3]}(): emoji_id was passed as argument to get_emoji_information(), emoji_name wasn't, and couldn't find the emoji accessible to the client."
                    )
                    logger.error(err)
                    raise err

            emoji_name = emoji.name
            emoji_animated = emoji.animated

    return (
        emoji_id,
        emoji_name,
        emoji_animated,
        get_emoji_url(emoji_id, emoji_animated, emoji_size),
    )


@overload
async def get_emoji_image(
    emoji: discord.PartialEmoji | discord.Emoji,
    emoji_size: int | None = 96,
) -> bytes:
    """Return an emoji's image.

    Parameters
    ----------
    emoji : :class:`~discord.PartialEmoji` | :class:`~discord.Emoji`
        A custom Discord emoji.
    emoji_size : int | None, optional
        A specific emoji image size. If set to None, will not limit the emoji size. Defaults to 96.

    Returns
    -------
    bytes

    Raises
    ------
    ValueError
        `emoji` had type `PartialEmoji` but it was not a custom emoji.
    """
    pass


@overload
async def get_emoji_image(
    emoji_id: int | str,
    emoji_animated: bool,
    emoji_size: int | None = 96,
) -> bytes:
    """Return an emoji's image.

    Parameters
    ----------
    emoji_id : int | str
        The ID of a custom emoji.
    emoji_animated : bool
        Whether the emoji is animated.
    emoji_size : int | None, optional
        A specific emoji image size. If set to None, will not limit the emoji size. Defaults to 96.

    Returns
    -------
    bytes

    Raises
    ------
    ValueError
        `emoji_id` had type `str` but it was not a valid numerical ID.
    """
    pass


async def get_emoji_image(  # pyright: ignore[reportInconsistentOverload]
    *args: discord.PartialEmoji | discord.Emoji | int | str | bool,
) -> bytes:
    """Return an emoji's image.

    Parameters
    ----------
    emoji : :class:`~discord.PartialEmoji` | :class:`~discord.Emoji` | None, optional
        A custom Discord emoji. Defaults to None, in which case `emoji_id` and `emoji_animated` are used instead.
    emoji_id : int | str | None, optional
        The ID of a custom emoji. Only used if `emoji` is not present. Defaults to None.
    emoji_animated : bool | None, optional
        Whether the emoji is animated. Only used if `emoji` is not present. Defaults to None.
    emoji_size : int | None, optional
        A specific emoji image size. If set to None, will not limit the emoji size. Defaults to 96.

    Returns
    -------
    bytes

    Raises
    ------
    ValueError
        `emoji` argument was passed and had type `PartialEmoji` but it was not a custom emoji, or `emoji_id` argument was passed and had type `str` but it was not a valid numerical ID.
    """
    emoji_url = get_emoji_url(*args)
    return await get_image_from_URL(emoji_url)


@overload
def get_emoji_url(
    emoji: discord.PartialEmoji | discord.Emoji,
    emoji_size: int | None = 96,
) -> str:
    """Return the standardised URL for an emoji.

    Parameters
    ----------
    emoji : :class:`~discord.PartialEmoji` | :class:`~discord.Emoji`
        A custom Discord emoji.
    emoji_size : int | None, optional
        A specific emoji size to get the URL for. If set to None, will not limit the emoji size. Defaults to 96.

    Returns
    -------
    str

    Raises
    ------
    ValueError
        `emoji` had type `PartialEmoji` but it was not a custom emoji.
    """
    pass


@overload
def get_emoji_url(
    emoji_id: int | str,
    emoji_animated: bool,
    emoji_size: int | None = 96,
) -> str:
    """Return the standardised URL for an emoji.

    Parameters
    ----------
    emoji_id : int | str
        The ID of a custom emoji.
    emoji_animated : bool
        Whether the emoji is animated.
    emoji_size : int | None, optional
        A specific emoji size to get the URL for. If set to None, will not limit the emoji size. Defaults to 96.

    Returns
    -------
    str

    Raises
    ------
    ValueError
        `emoji_id` had type `str` but it was not a valid numerical ID.
    """
    pass


@overload
def get_emoji_url(
    *args: discord.PartialEmoji | discord.Emoji | int | str | bool,
) -> str:
    pass


def get_emoji_url(  # pyright: ignore[reportInconsistentOverload]
    *args: discord.PartialEmoji | discord.Emoji | int | str | bool,
) -> str:
    """Return the standardised URL for an emoji.

    Parameters
    ----------
    emoji : :class:`~discord.PartialEmoji` | :class:`~discord.Emoji` | None, optional
        A custom Discord emoji. Defaults to None, in which case `emoji_id` and `emoji_animated` are used instead.
    emoji_id : int | str | None, optional
        The ID of a custom emoji. Only used if `emoji` is not present. Defaults to None.
    emoji_animated : bool | None, optional
        Whether the emoji is animated. Only used if `emoji` is not present. Defaults to None.
    emoji_size : int | None, optional
        A specific emoji size to get the URL for. If set to None, will not limit the emoji size. Defaults to 96.

    Returns
    -------
    str

    Raises
    ------
    ValueError
        `emoji` argument was passed and had type `PartialEmoji` but it was not a custom emoji, or `emoji_id` argument was passed and had type `str` but it was not a valid numerical ID.
    """
    emoji_animated = False
    emoji_size = 96
    if isinstance(emoji := args[0], discord.PartialEmoji | discord.Emoji):
        if not emoji.id:
            err = ValueError(
                f"Error in function {inspect.stack()[1][3]}(): PartialEmoji passed as argument to get_emoji_url() is not a custom emoji."
            )
            logger.error(err)
            raise err

        emoji_id = emoji.id
        emoji_animated = emoji.animated

        if len(args) > 1:
            if isinstance(args[1], int):
                emoji_size = args[1]
            else:
                emoji_size = None
    elif isinstance(emoji_id := args[0], int | str):
        if not emoji_id:
            err = ArgumentError(
                f"Error in function {inspect.stack()[1][3]}(): at least one of emoji or emoji_id must be passed as argument to get_emoji_information()."
            )
            logger.error(err)
            raise err
        elif isinstance(emoji_id, str):
            try:
                emoji_id = int(emoji_id)
            except ValueError:
                err = ValueError(
                    f"Error in function {inspect.stack()[1][3]}(): emoji_id was passed as an argument to get_emoji_information() and had type str but was not convertible to an ID."
                )
                logger.error(err)
                raise err

        if len(args) > 1:
            emoji_animated = not not args[1]

            if len(args) > 2:
                if isinstance(args[2], int):
                    emoji_size = args[2]
                else:
                    emoji_size = None
    else:
        raise AttributeError("Emoji arguments were not passed to get_emoji_url().")

    emoji_url = f"https://cdn.discordapp.com/emojis/{emoji_id}.webp"
    arguments = []
    if emoji_size:
        arguments.append(f"size={emoji_size}")
    if emoji_animated:
        arguments.append("animated=true")
    if arguments:
        emoji_url += "?" + "&".join(arguments)

    return emoji_url


async def get_image_from_URL(url: str) -> bytes:
    """Return an image stored in a URL.

    Parameters
    ----------
    url : str
        The URL of the image to get.

    Returns
    -------
    bytes

    Raises
    ------
    HTTPResponseError
        HTTP request to fetch image returned a status other than 200.
    InvalidURL
        Argument was not a valid URL.
    RuntimeError
        Session connection failed.
    ServerTimeoutError
        Connection to server timed out.
    """
    image_bytes: io.BytesIO | None = None
    async with aiohttp.ClientSession(
        headers={"User-Agent": "Discord Channel Bridge Bot/1.0"}
    ) as session:
        async with session.get(url) as response:
            if response.status != 200:
                err = HTTPResponseError(
                    f"Error in function {inspect.stack()[1][3]}(): failed to retrieve image from URL {url}. HTTP status {response.status}."
                )
                logger.error(err)
                raise err

            response_buffer = await response.read()
            image_bytes = io.BytesIO(response_buffer)

    if not image_bytes:
        err = Exception("Unknown problem occurred trying to fetch image.")
        logger.error(err)
        raise err

    return image_bytes.read()


def hash_image(image: bytes) -> str:
    """Return a string with the MD5 hash of an image.

    Parameters
    ----------
    image : bytes
        The image bytes object.

    Returns
    -------
    str
    """
    return md5(image).hexdigest()


async def resolve_message_reference(
    message_reference: discord.MessageReference | None,
) -> discord.Message | None:
    """Attempt to fetch and return the message a :class:`~discord.MessageReference` is a reference to.

    Parameters
    ----------
    message_reference : :class:`~discord.MessageReference`
        The message reference to fetch the original of.

    Returns
    -------
    :class:`~discord.Message` | None
    """
    if message_reference is None:
        return None

    logger.debug(
        "Trying to resolve the message referenced by reference %s.",
        message_reference,
    )

    if isinstance(
        resolved_message_reference := message_reference.resolved,
        discord.Message,
    ):
        # Original message is cached and I can just fetch it
        logger.debug(
            "Resolved reference to cached message with ID %s.",
            resolved_message_reference.id,
        )
        return resolved_message_reference

    if (message_reference_id := message_reference.message_id) is None:
        logger.debug(
            "Message being referenced was not cached and its ID was not included in the message reference."
        )
        return None

    logger.debug(
        "Message being referenced was not cached but had ID %s. Trying to fetch it.",
        message_reference_id,
    )
    try:
        message_reference_channel = validate_channels(
            message_reference_channel=await get_channel_from_id(
                message_reference.channel_id
            ),
            log_error=False,
        )["message_reference_channel"]
    except ChannelTypeError:
        logger.debug(
            "The channel referenced message with ID %s was from was not a text channel or a thread off one.",
            message_reference_id,
        )
        return None

    # I have access to the channel of the original message being forwarded
    try:
        # Try to find the original message
        fetched_message = await message_reference_channel.fetch_message(
            message_reference_id
        )
        logger.debug(
            "Successfully fetched referenced message with ID %s.",
            fetched_message.id,
        )
        return fetched_message
    except Exception as e:
        logger.error(
            "An error occurred while trying to fetch referenced message with ID %s: %s",
            message_reference_id,
            e,
        )
        return None


def truncate(msg: str, length: int) -> str:
    """Return `msg` truncated to `length` plus a "…" character at the end.

    Parameters
    ----------
    msg : str
        The message to truncate.
    length : int
        Its maximum length.

    Returns
    -------
    str
        _description_
    """
    return msg if (len(msg) < length) else msg[: length - 1] + "…"


async def wait_until_ready(
    *,
    time_to_wait: float | int = 100,
    polling_rate: float | int = 1,
) -> bool:
    """Wait until the bot is ready and return True when that happens, or return False if it times out.

    Parameters
    ----------
    time_to_wait : float | int, optional
        The amount of time in seconds to wait for the bot to get ready. Values less than 0 will be treated as 0. Defaults to 100.
    polling_rate : float | int, optional
        The amount of time in seconds to wait between checks for the variable. Values less than 1 will be treated as 1. Defaults to 1.

    Returns
    -------
    bool
    """
    global is_ready
    if is_ready:
        return True

    time_to_wait = max(time_to_wait, 0.0)
    polling_rate = max(polling_rate, 1.0)
    time_waited = 0.0
    while (not is_ready) and (time_waited < time_to_wait):
        await asyncio.sleep(polling_rate)
        time_waited += polling_rate

    if time_waited >= time_to_wait:
        logger.warning("Taking forever to get ready.")
        return False
    return True
