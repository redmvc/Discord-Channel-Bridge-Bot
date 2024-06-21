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
