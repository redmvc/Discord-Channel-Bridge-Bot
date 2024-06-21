import json

import discord
import mysql.connector
import mysql.connector.abstracts

from bridge import Bridges

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

# The lists of bridges that have been created
outbound_bridges: dict[int, Bridges] = {}
inbound_bridges: dict[int, dict[int, Bridges]] = {}

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
    global client
    if link_or_mention.startswith("https://discord.com/channels"):
        try:
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
    global client
    if isinstance(channel_or_id, int):
        channel = client.get_channel(channel_or_id)
    else:
        channel = channel_or_id

    return channel
