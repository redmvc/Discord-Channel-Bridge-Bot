from __future__ import annotations

import json

import discord
import mysql.connector
import mysql.connector.abstracts

"""
The format of this variable is
{
    "app_token": "the app token for the Discord bot",
    "db_host": "database host",
    "db_port": database port,
    "db_user": "database username",
    "db_pwd": "database password",
    "db_name": "database name"
}
"""
credentials: dict[str, str | int]

# The connection to the database
conn: (
    mysql.connector.pooling.PooledMySQLConnection
    | mysql.connector.abstracts.MySQLConnectionAbstract
    | None
) = None

# This variable will be set to True at the end of init() to make sure nothing that relies on the globals uses them before they are ready
globals_are_initialised: bool = False

# Variables for connection to the Discord client
intents: discord.Intents
client: discord.Client
command_tree: discord.app_commands.CommandTree
is_ready: bool = (
    False  # This one is set to True once the bot has been initialised in main.py
)


def init():
    """Initialise all global variables."""
    global intents, client, command_tree, credentials, globals_are_initialised

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

    credentials = json.load(open("credentials.json"))

    globals_are_initialised = True


def mention_to_channel(
    link_or_mention: str,
) -> discord.guild.GuildChannel | discord.Thread | discord.abc.PrivateChannel | None:
    """Return the channel referenced by a channel mention or a Discord link to a channel.

    #### Args:
        - `link_or_mention`: Either a mention of a Discord channel (`<#channel_id>`) or a Discord link to it (`https://discord.com/channels/server_id/channel_id`).

    #### Returns:
        - The channel whose ID is given by `channel_id`.
    """
    global client
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
    return get_channel_from_id(channel_id)


def get_channel_from_id(
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
    global client
    if isinstance(channel_or_id, int):
        channel = client.get_channel(channel_or_id)
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

    if isinstance(channel_or_id, int):
        return channel_or_id
    return channel_or_id.id
