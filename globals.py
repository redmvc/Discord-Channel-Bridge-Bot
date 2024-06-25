from __future__ import annotations

import asyncio
import io
import json

import aiohttp
import discord

from validations import validate_types, HTTPResponseError

"""
The format of this variable is
{
    "app_token": "the app token for the Discord bot",
    "db_dialect": "database dialect",
    "db_driver": "database driver",
    "db_host": "database host",
    "db_port": database port,
    "db_user": "database username",
    "db_pwd": "database password",
    "db_name": "database name",
    "emoji_server_id": "id of the server for storing emoji"
}
"""
settings: dict[str, str | int] = json.load(open("settings.json"))

# Variables for connection to the Discord client
intents = discord.Intents()
intents.emojis_and_stickers = True
intents.guilds = True
intents.members = True
intents.message_content = True
intents.messages = True
intents.reactions = True
intents.typing = True
intents.webhooks = True
client = discord.Client(intents=intents)
command_tree = discord.app_commands.CommandTree(client)

# This one is set to True once the bot has been initialised in main.py
is_ready: bool = False

# Channels which will automatically create threads in bridged channels
auto_bridge_thread_channels: set[int] = set()

# Server which can be used to store unknown emoji for mirroring reactions
emoji_server: discord.Guild | None = None


async def mention_to_channel(
    link_or_mention: str,
) -> discord.guild.GuildChannel | discord.Thread | discord.abc.PrivateChannel | None:
    """Return the channel referenced by a channel mention or a Discord link to a channel.

    #### Args:
        - `link_or_mention`: Either a mention of a Discord channel (`<#channel_id>`) or a Discord link to it (`https://discord.com/channels/server_id/channel_id`).

    #### Returns:
        - The channel whose ID is given by `channel_id`.
    """
    validate_types({"link_or_mention": (link_or_mention, str)})

    if link_or_mention.startswith("https://discord.com/channels"):
        try:
            while link_or_mention.endswith("/"):
                link_or_mention = link_or_mention[:-1]

            channel_id = int(link_or_mention.rsplit("/")[-1])
        except ValueError:
            return None
    else:
        try:
            channel_id = int(
                link_or_mention.replace("<", "").replace(">", "").replace("#", "")
            )
        except ValueError:
            return None

    return await get_channel_from_id(channel_id)


async def get_channel_from_id(
    channel_or_id: (
        discord.guild.GuildChannel | discord.Thread | discord.abc.PrivateChannel | int
    ),
) -> discord.guild.GuildChannel | discord.Thread | discord.abc.PrivateChannel | None:
    """Ensure that this function's argument is a valid Discord channel, when it may instead be a channel ID.

    #### Args:
        - `channel_or_id`: Either a Discord channel or an ID of same.

    #### Returns:
        - If the argument is a channel, returns it unchanged; otherwise, returns a channel with the ID passed.
    """
    validate_types(
        {
            "channel_or_id": (
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
        }
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
    channel_or_id: (
        discord.guild.GuildChannel | discord.Thread | discord.abc.PrivateChannel | int
    ),
) -> int:
    """Returns the ID of the channel passed as argument, or the argument itself if it is already an ID.

    #### Args:
        - `channel_or_id`: A Discord channel or its ID.

    #### Returns:
        - `int`: The ID of the channel passed as argument.
    """
    validate_types(
        {
            "channel_or_id": (
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
        }
    )

    if isinstance(channel_or_id, int):
        return channel_or_id
    else:
        return channel_or_id.id


async def get_channel_member(
    channel: discord.abc.GuildChannel | discord.Thread, member_id: int
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


async def get_image_from_URL(url: str) -> bytes:
    """Return an image stored in a URL.

    #### Args:
        - `url`: The URL of the image to get.

    #### Raises:
        - `AssertionError`: Something went wrong fetching the image.
        - `HTTPResponseError`: HTTP request to fetch image returned a status other than 200.
        - `InvalidURL`: Argument was not a valid URL.
        - `RuntimeError`: Session connection failed.
        - `ServerTimeoutError`: Connection to server timed out.
    """
    image_bytes: io.BytesIO | None = None
    async with aiohttp.ClientSession(
        headers={"User-Agent": "Discord Channel Bridge Bot/0.1"}
    ) as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                raise HTTPResponseError(
                    f"Failed to retrieve image from URL: HTTP status {resp.status}."
                )

            resp_buffer = await resp.read()
            image_bytes = io.BytesIO(resp_buffer)
            assert isinstance(image_bytes, io.BytesIO)
            assert image_bytes is not None

    if not image_bytes:
        raise Exception("Unknown problem occurred trying to fetch image.")

    return image_bytes.read()


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
