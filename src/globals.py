from __future__ import annotations

import asyncio
import inspect
import io
import json
from hashlib import md5
from typing import (
    Any,
    Callable,
    Literal,
    SupportsInt,
    TypedDict,
    TypeVar,
    cast,
    overload,
)

import aiohttp
import discord
from aiolimiter import AsyncLimiter
from beartype import beartype
from typing_extensions import NotRequired

from validations import ArgumentError, HTTPResponseError, logger, validate_channels

# discord.guild.GuildChannel isn't working in commands.py for some reason
GuildChannel = (
    discord.VoiceChannel
    | discord.StageChannel
    | discord.ForumChannel
    | discord.TextChannel
    | discord.CategoryChannel
)


class Settings(TypedDict):
    """
    An Typed Dictionary with the bot's settings. The `settings.json` file must contain a `"context"` entry whose value is another key in the file with the attributes below. For example:

    .. code-block:: json
        {
            "context": "production",
            "production": {
                "app_token": "...",
                "db_dialect": "...",
                ...
            },
            "testing": {
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
    db_driver : Literal['pymysql'] | Literal['psycopg2'] | Literal['pysqlite']
        The database driver.
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
    db_driver: Literal["pymysql", "psycopg2", "pysqlite"]
    db_host: str
    db_port: int
    db_user: str
    db_pwd: str
    db_name: str
    emoji_server_id: NotRequired[SupportsInt | str]
    whitelisted_apps: NotRequired[list[SupportsInt | str]]


settings_root: dict[str, str | Settings] = json.load(open("settings.json"))
assert isinstance(settings_root["context"], str)
context = settings_root["context"]
settings: Settings = cast(Settings, settings_root[context])

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

# Channels which will automatically create threads in bridged channels
auto_bridge_thread_channels: set[int] = set()

# Server which can be used to store unknown emoji for mirroring reactions
emoji_server: discord.Guild | None = None

# Dictionary listing all apps whitelisted per channel
per_channel_whitelist: dict[int, set[int]] = {}

# Helper to prevent us from being rate limited
rate_limiter = AsyncLimiter(1, 10)

# Variable to keep track of messages that are still being bridged/edited before they can be edited/deleted
message_lock: dict[int, asyncio.Lock] = {}

# Variable to keep track of channels that are being sent messages to to try to preserve ordering
channel_lock: dict[int, asyncio.Lock] = {}

# Type wildcard
T = TypeVar("T", bound=Any)


@overload
async def get_channel_from_id(
    channel_or_id: (
        GuildChannel
        | discord.Thread
        | discord.DMChannel
        | discord.PartialMessageable
        | discord.abc.PrivateChannel
        | int
    ),
) -> (
    GuildChannel
    | discord.Thread
    | discord.abc.PrivateChannel
    | discord.PartialMessageable
    | discord.DMChannel
    | None
): ...


@overload
async def get_channel_from_id(
    channel_or_id: (
        GuildChannel
        | discord.Thread
        | discord.DMChannel
        | discord.PartialMessageable
        | discord.abc.PrivateChannel
        | int
    ),
    *,
    assert_text_or_thread: Literal[False],
) -> (
    GuildChannel
    | discord.Thread
    | discord.abc.PrivateChannel
    | discord.PartialMessageable
    | discord.DMChannel
    | None
): ...


@overload
async def get_channel_from_id(
    channel_or_id: (
        GuildChannel
        | discord.Thread
        | discord.DMChannel
        | discord.PartialMessageable
        | discord.abc.PrivateChannel
        | int
    ),
    *,
    assert_text_or_thread: Literal[True],
) -> discord.TextChannel | discord.Thread: ...


@beartype
async def get_channel_from_id(
    channel_or_id: (
        GuildChannel
        | discord.Thread
        | discord.DMChannel
        | discord.PartialMessageable
        | discord.abc.PrivateChannel
        | int
    ),
    *,
    assert_text_or_thread: bool = False,
) -> (
    GuildChannel
    | discord.Thread
    | discord.abc.PrivateChannel
    | discord.PartialMessageable
    | discord.DMChannel
    | None
):
    """Ensure that this function's argument is a valid Discord channel, when it may instead be a channel ID.

    #### Args:
        - `channel_or_id`: Either a Discord channel or an ID of same.
        - `assert_text_or_thread`: Whether to assert that the channel is either a TextChannel or a Thread before returning. Defaults to False.

    #### Returns:
        - If the argument is a channel, returns it unchanged; otherwise, returns a channel with the ID passed, or None if it couldn't be found.
    """
    if isinstance(channel_or_id, int):
        channel = client.get_channel(channel_or_id)
        if not channel:
            try:
                channel = await client.fetch_channel(channel_or_id)
            except Exception:
                channel = None
    else:
        channel = channel_or_id

    if assert_text_or_thread:
        assert isinstance(channel, discord.TextChannel | discord.Thread)

    return channel


@beartype
def get_id_from_channel(
    channel_or_id: GuildChannel | discord.Thread | discord.abc.PrivateChannel | int,
) -> int:
    """Return the ID of the channel passed as argument, or the argument itself if it is already an ID.

    #### Args:
        - `channel_or_id`: A Discord channel or its ID.

    #### Returns:
        - `int`: The ID of the channel passed as argument.
    """
    if isinstance(channel_or_id, int):
        return channel_or_id

    if channel_or_id.id:
        return channel_or_id.id

    err = ValueError(
        f"Error in function {inspect.stack()[1][3]}(): argument passed to function get_id_from_channel() was not a valid channel nor an ID."
    )
    logger.error(err)
    raise err


@beartype
async def get_channel_parent(
    channel_or_id: (
        GuildChannel
        | discord.Thread
        | discord.DMChannel
        | discord.PartialMessageable
        | discord.GroupChannel
        | int
    ),
) -> discord.TextChannel:
    """Return the parent channel of its argument, or the argument itself if it does not have a parent. Errors if the channel passed as argument is not a `discord.TextChannel`, a `discord.Thread`, or the ID of one of those.

    #### Args:
        - `channel_or_id`: A Discord channel or its ID.

    #### Raises:
        - `ChannelTypeError`: The channel is not a text channel nor a thread off a text channel.

    #### Returns:
        - `discord.TextChannel`: The parent of the channel passed as argument, or the channel itself in case it is not a thread.
    """
    channel = validate_channels(channel_or_id=await get_channel_from_id(channel_or_id))[
        "channel_or_id"
    ]

    if isinstance(channel, discord.TextChannel):
        return channel

    return cast(discord.TextChannel, channel.parent)


@beartype
async def get_channel_member(
    channel: GuildChannel | discord.Thread,
    member_id: int,
) -> discord.Member | None:
    """Return a channel's member by their ID, or None if they can't be found.

    #### Args:
        - `channel`: The channel to look for a member in.
        - `member_id`: Their ID.
    """
    channel_member = channel.guild.get_member(member_id)
    if not channel_member:
        try:
            channel_member = await channel.guild.fetch_member(member_id)
        except Exception:
            channel_member = None

    return channel_member


@beartype
async def get_image_from_URL(url: str) -> bytes:
    """Return an image stored in a URL.

    #### Args:
        - `url`: The URL of the image to get.

    #### Raises:
        - `HTTPResponseError`: HTTP request to fetch image returned a status other than 200.
        - `InvalidURL`: Argument was not a valid URL.
        - `RuntimeError`: Session connection failed.
        - `ServerTimeoutError`: Connection to server timed out.
    """
    image_bytes: io.BytesIO | None = None
    async with aiohttp.ClientSession(
        headers={"User-Agent": "Discord Channel Bridge Bot/1.0"}
    ) as session:
        async with session.get(url) as response:
            if response.status != 200:
                err = HTTPResponseError(
                    f"Error in function {inspect.stack()[1][3]}(): failed to retrieve image from URL. HTTP status {response.status}."
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


@beartype
def get_emoji_information(
    emoji: discord.PartialEmoji | discord.Emoji | None = None,
    emoji_id: int | str | None = None,
    emoji_name: str | None = None,
) -> tuple[int, str, bool, str]:
    """Return a tuple with emoji ID, emoji name, whether the emoji is animated, and the URL for its image.

    #### Args:
        - `emoji`: A Discord emoji. Defaults to None, in which case the values below will be used instead.
        - `emoji_id`: The ID of an emoji. Will only be used if `emoji` is None. Defaults to None.
        - `emoji_name`: The name of the emoji. Defaults to None, but must be included if `emoji_id` is. If it starts with `"a:"` the emoji will be marked as animated.

    #### Raises:
        - `ArgumentError`: Neither `emoji` nor `emoji_id` were passed, or `emoji_id` was passed but not `emoji_name`.
        - `ValueError`: `emoji` argument was passed and had type `PartialEmoji` but it was not a custom emoji, or `emoji_id` argument was passed and had type `str` but it was not a valid numerical ID.
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
        emoji_url = emoji.url
    else:
        if not emoji_id:
            err = ArgumentError(
                f"Error in function {inspect.stack()[1][3]}(): either emoji or both emoji_id and emoji_name must be passed as argument to get_emoji_information()."
            )
            logger.error(err)
            raise err
        elif not emoji_name:
            err = ArgumentError(
                f"Error in function {inspect.stack()[1][3]}(): if emoji_id is passed as argument to get_emoji_information(), emoji_name must also be."
            )
            logger.error(err)
            raise err

        if emoji_animated := emoji_name.startswith("a:"):
            emoji_name = emoji_name[2:]
        elif emoji_name.startswith(":"):
            emoji_name = emoji_name[1:]

        if emoji_animated:
            ext = "gif"
        else:
            ext = "png"
        emoji_url = f"https://cdn.discordapp.com/emojis/{emoji_id}.{ext}?v=1"

    try:
        emoji_id_int = int(emoji_id)
    except ValueError:
        err = ValueError(
            f"Error in function {inspect.stack()[1][3]}(): emoji_id was passed as an argument to get_emoji_information() and had type str but was not convertible to an ID."
        )
        logger.error(err)
        raise err

    return (emoji_id_int, emoji_name, emoji_animated, emoji_url)


@beartype
def hash_image(image: bytes) -> str:
    """Return a string with a hash of an image.

    #### Args:
        - `image`: The image bytes object.
    """
    return md5(image).hexdigest()


@beartype
def truncate(msg: str, length: int) -> str:
    """Truncate a message to a certain length.

    #### Args:
        - `msg`: The message to truncate.
        - `length`: Its maximum length.

    #### Returns:
        `str`: The truncated message.
    """
    return msg if len(msg) < length else msg[: length - 1] + "…"


@beartype
async def wait_until_ready(
    *,
    time_to_wait: float | int = 100,
    polling_rate: float | int = 1,
) -> bool:
    """Return True when the bot is ready or False if it times out.

    #### Args:
        - `time_to_wait`: The amount of time in seconds to wait for the bot to get ready. Values less than 0 will be treated as 0. Defaults to 100.
        - `polling_rate`: The amount of time in seconds to wait between checks for the variable. Values less than 0 will be treated as 0. Defaults to 1.
    """
    global is_ready
    if is_ready:
        return True

    time_to_wait = max(time_to_wait, 0.0)
    polling_rate = max(polling_rate, 0.0)
    time_waited = 0.0
    while not is_ready and time_waited < time_to_wait:
        await asyncio.sleep(polling_rate)
        time_waited += polling_rate

    if time_waited >= time_to_wait:
        logger.warning("Taking forever to get ready.")
        return False
    return True


@beartype
async def run_retries(
    fun: Callable[..., T],
    num_retries: int,
    time_to_wait: float | int = 5,
    exceptions_to_catch: type | tuple[type] | None = None,
) -> T:
    """Run a function and retry it every time an exception occurs up to a certain maximum number of tries. If it succeeds, return its result; otherwise, raise the error.

    #### Args:
        - `fun`: The function to run.
        - `num_retries`: The number of times to try the function again. If set to 0 or less, will be set to 1.
        - `time_to_wait`: Time in seconds to wait between retries; only used if `num_retries` is greater than 1. If set to 0 or less, will set `num_retries` to 1. Defaults to 5.
        - `exceptions_to_catch`: An exception type or a list of exception types to catch. Defaults to None, in which case all types will be caught.

    #### Returns:
        - `T`: The result of calling `fun()`.
    """
    if num_retries < 1:
        num_retries = 1
    elif num_retries > 1 and time_to_wait <= 0:
        num_retries = 1

    for retry in range(num_retries):
        try:
            return fun()
        except Exception as e:
            if retry < num_retries - 1 and (
                not exceptions_to_catch or isinstance(e, exceptions_to_catch)
            ):
                await asyncio.sleep(time_to_wait)
            else:
                raise e

    err = ValueError(
        f"Error in function {inspect.stack()[1][3]}(): couldn't run function {fun.__name__}() in {num_retries} retries."
    )
    logger.error(err)
    raise err
