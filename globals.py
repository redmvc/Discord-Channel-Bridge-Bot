from __future__ import annotations

import asyncio
import io
import json
from hashlib import md5
from logging import warn
from typing import Any, Callable, Literal, SupportsInt, TypedDict, TypeVar, cast

import aiohttp
import discord
from aiolimiter import AsyncLimiter
from typing_extensions import NotRequired

from validations import ArgumentError, HTTPResponseError, validate_types

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
context: str = cast(str, settings_root["context"])
settings: Settings = cast(Settings, settings_root[context])

# Variables for connection to the Discord client
intents = discord.Intents(
    emojis_and_stickers=True,
    guilds=True,
    members=True,
    message_content=True,
    messages=True,
    reactions=True,
    typing=True,
    webhooks=True,
)
client = discord.Client(intents=intents)
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

# Type wildcard
_T = TypeVar("_T", bound=Any)


async def get_channel_from_id(
    channel_or_id: GuildChannel | discord.Thread | discord.abc.PrivateChannel | int,
) -> GuildChannel | discord.Thread | discord.abc.PrivateChannel | None:
    """Ensure that this function's argument is a valid Discord channel, when it may instead be a channel ID.

    #### Args:
        - `channel_or_id`: Either a Discord channel or an ID of same.

    #### Returns:
        - If the argument is a channel, returns it unchanged; otherwise, returns a channel with the ID passed.
    """
    validate_types(
        channel_or_id=(
            channel_or_id,
            (
                int,
                discord.TextChannel,
                discord.Thread,
                discord.VoiceChannel,
                discord.StageChannel,
                discord.ForumChannel,
                discord.CategoryChannel,
                discord.abc.PrivateChannel,
            ),
        )
    )

    if isinstance(channel_or_id, int):
        channel = client.get_channel(channel_or_id)
        if not channel:
            try:
                channel = await client.fetch_channel(channel_or_id)
            except Exception:
                channel = None
    else:
        channel = channel_or_id

    return channel


def get_id_from_channel(
    channel_or_id: GuildChannel | discord.Thread | discord.abc.PrivateChannel | int,
) -> int:
    """Returns the ID of the channel passed as argument, or the argument itself if it is already an ID.

    #### Args:
        - `channel_or_id`: A Discord channel or its ID.

    #### Returns:
        - `int`: The ID of the channel passed as argument.
    """
    validate_types(
        channel_or_id=(
            channel_or_id,
            (
                int,
                discord.TextChannel,
                discord.Thread,
                discord.VoiceChannel,
                discord.StageChannel,
                discord.ForumChannel,
                discord.CategoryChannel,
                discord.abc.PrivateChannel,
            ),
        )
    )

    if isinstance(channel_or_id, int):
        return channel_or_id
    else:
        return channel_or_id.id


async def get_channel_member(
    channel: GuildChannel | discord.Thread, member_id: int
) -> discord.Member | None:
    """Return a channel's member by their ID, or None if they can't be found.

    #### Args:
        - `channel`: The channel to look for a member in.
        - `member_id`: Their ID.
    """
    validate_types(
        channel=(
            channel,
            (
                discord.VoiceChannel,
                discord.StageChannel,
                discord.ForumChannel,
                discord.TextChannel,
                discord.CategoryChannel,
                discord.Thread,
            ),
        ),
        member_id=(member_id, int),
    )

    channel_member = channel.guild.get_member(member_id)
    if not channel_member:
        try:
            channel_member = await channel.guild.fetch_member(member_id)
        except Exception:
            channel_member = None

    return channel_member


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
        headers={"User-Agent": "Discord Channel Bridge Bot/0.1"}
    ) as session:
        async with session.get(url) as response:
            if response.status != 200:
                raise HTTPResponseError(
                    f"Failed to retrieve image from URL: HTTP status {response.status}."
                )

            response_buffer = await response.read()
            image_bytes = io.BytesIO(response_buffer)

    if not image_bytes:
        raise Exception("Unknown problem occurred trying to fetch image.")

    return image_bytes.read()


def get_emoji_information(
    emoji: discord.PartialEmoji | discord.Emoji | None = None,
    emoji_id: int | str | None = None,
    emoji_name: str | None = None,
) -> tuple[int, str, bool, str]:
    """Return a tuple with emoji ID, emoji name, whether the emoji is animated, and the URL for its image.

    #### Args:
        - `emoji`: A Discord emoji. Defaults to None, in which case the values below will be used instead.
        - `emoji_id`: The ID of an emoji. Defaults to None, in which case the value above will be used instead.
        - `emoji_name`: The name of the emoji. Defaults to None, but must be included if `emoji_id` is. If it starts with `"a:"` the emoji will be marked as animated.

    #### Raises:
        - `ArgumentError`: The number of arguments passed is incorrect.
        - `ValueError`: `emoji` argument was passed and had type `PartialEmoji` but it was not a custom emoji, or `emoji_id` argument was passed and had type `str` but it was not a valid numerical ID.
    """
    types_to_validate: dict[str, tuple] = {}
    if emoji:
        types_to_validate = {"emoji": (emoji, (discord.PartialEmoji, discord.Emoji))}
    elif emoji_id:
        if emoji_name:
            types_to_validate = {
                "emoji_id": (emoji_id, (int, str)),
                "emoji_name": (emoji_name, str),
            }
        else:
            raise ArgumentError(
                "If emoji_id is passed as argument, emoji_name must also be."
            )
    else:
        raise ArgumentError(
            "At least one of emoji or emoji_id must be passed as argument."
        )
    validate_types(**types_to_validate)

    if emoji:
        if not emoji.id:
            raise ValueError("PartialEmoji passed as argument is not a custom emoji.")

        emoji_id = emoji.id
        emoji_name = emoji.name
        emoji_animated = emoji.animated
        emoji_url = emoji.url
    else:
        emoji_name = cast(str, emoji_name)

        emoji_animated = emoji_name.startswith("a:")
        if emoji_animated:
            emoji_name = emoji_name[2:]
        elif emoji_name.startswith(":"):
            emoji_name = emoji_name[1:]

        if emoji_animated:
            ext = "gif"
        else:
            ext = "png"
        emoji_url = f"https://cdn.discordapp.com/emojis/{emoji_id}.{ext}?v=1"

    try:
        emoji_int = int(cast(int | str, emoji_id))
    except ValueError:
        raise ValueError(
            "emoji_int was passed as an argument and had type str but was not convertible to an ID."
        )

    return (emoji_int, emoji_name, emoji_animated, emoji_url)


def hash_image(image: bytes) -> str:
    """Return a string with a hash of an image.

    #### Args:
        - `image`: The image bytes object.
    """
    return md5(image).hexdigest()


async def wait_until_ready() -> bool:
    """Returns True when the bot is ready or False if it times out."""
    if is_ready:
        return True

    time_waited = 0
    while not is_ready and time_waited < 100:
        await asyncio.sleep(1)
        time_waited += 1

    if time_waited >= 100:
        # somethin' real funky going on here
        # I don't have error handling yet though
        print("Taking forever to get ready.")
        return False
    return True


async def run_retries(
    fun: Callable[..., _T],
    num_retries: int,
    time_to_wait: float = 5,
    exceptions_to_catch: type | tuple[type] | None = None,
) -> _T:
    """Run a function and retry it every time an exception occurs up to a certain maximum number of tries. If it succeeds, return its result; otherwise, raise the error.

    #### Args:
        - `fun`: The function to run.
        - `num_retries`: The number of times to try the function again.
        - `time_to_wait`: How long to wait between retries.
        - `exceptions_to_catch`: An exception type or a list of exception types to catch. Defaults to None, in which case all types will be caught.

    #### Returns:
        - `_T`: The result of calling `fun()`.
    """
    validate_exceptions_to_catch: dict[str, tuple] = {}
    if exceptions_to_catch:
        if isinstance(exceptions_to_catch, type):
            exceptions_to_catch = (exceptions_to_catch,)
        else:
            validate_exceptions_to_catch["exceptions_to_catch"] = (
                exceptions_to_catch,
                tuple,
            )
    validate_types(
        num_retries=(num_retries, int),
        time_to_wait=(time_to_wait, (float, int)),
        **validate_exceptions_to_catch,
    )

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

    raise Exception("Couldn't run the function in number of retries.")
