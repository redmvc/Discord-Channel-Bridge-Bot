import asyncio
import json
from typing import TYPE_CHECKING, cast

import discord
from beartype import beartype
from test_runner import logger

if TYPE_CHECKING:
    from typing import Any, Coroutine, SupportsInt, TypedDict

    class Settings(TypedDict):
        """A TypedDict with the bot's settings. The `settings.json` file must contain a `"tests"` entry with the attributes below. For example:

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
        testing_server_id: "SupportsInt | str"


settings_root: "dict[str, str | Settings]" = json.load(open("settings.json"))
settings: "Settings" = cast("Settings", settings_root["tests"])

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

# This one is set to True once the bot has been initialised
is_ready: bool = False

# Server in which testing will be run
testing_server: discord.Guild


@client.event
async def on_ready():
    """This function is called when the client is done preparing the data received from Discord. Usually after login is successful and the Client.guilds and co. are filled up.

    Raises
    ------
    ChannelTypeError
        The source or target channels of some existing Bridge are not text channels nor threads off a text channel.
    WebhookChannelError
        Webhook of some existing Bridge is not attached to Bridge's target channel.
    :class:`~discord.HTTPException`
        Deleting an existing webhook or creating a new one failed.
    :class:`~discord.Forbidden`
        You do not have permissions to create or delete webhooks for some of the channels in existing Bridges.
    """
    global is_ready
    if is_ready:
        return

    logger.info("Client successfully connected. Running initial loading procedures...")

    # -----
    logger.info("Loading testing server...")
    testing_server_id_str = settings.get("testing_server_id")
    try:
        if testing_server_id_str:
            testing_server_id = int(testing_server_id_str)
        else:
            testing_server_id = None
    except Exception:
        testing_server_id = None
    if not testing_server_id or testing_server_id <= 0:
        raise ValueError(
            "Testing server ID not set in settings file or it does not resolve to a valid integer."
        )

    if not (potential_testing_server := client.get_guild(testing_server_id)):
        try:
            potential_testing_server = await client.fetch_guild(testing_server_id)
        except Exception:
            potential_testing_server = None

    if not potential_testing_server:
        raise ValueError(
            "Testing server ID does not match a server the testing bot has access to."
        )
    elif not (
        potential_testing_server.me.guild_permissions.change_nickname
        and potential_testing_server.me.guild_permissions.view_channel
        and potential_testing_server.me.guild_permissions.use_external_apps
        and potential_testing_server.me.guild_permissions.send_messages
        and potential_testing_server.me.guild_permissions.send_messages_in_threads
        and potential_testing_server.me.guild_permissions.create_public_threads
        and potential_testing_server.me.guild_permissions.create_private_threads
        and potential_testing_server.me.guild_permissions.attach_files
        and potential_testing_server.me.guild_permissions.read_message_history
        and potential_testing_server.me.guild_permissions.read_messages
        and potential_testing_server.me.guild_permissions.add_reactions
        and potential_testing_server.me.guild_permissions.use_external_emojis
        and potential_testing_server.me.guild_permissions.use_application_commands
    ):
        raise ValueError(
            "Testing bot does not have the necessary permissions in the testing server to run tests."
        )

    global testing_server
    testing_server = potential_testing_server
    logger.info("Testing server loaded.")

    # -----
    logger.info("Syncing command tree...")
    await asyncio.gather(command_tree.sync(), command_tree.sync(guild=testing_server))
    logger.info("Command tree synced.")

    # -----
    is_ready = True
    logger.info("Bot is ready.")


@beartype
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
    while not is_ready and time_waited < time_to_wait:
        await asyncio.sleep(polling_rate)
        time_waited += polling_rate

    if time_waited >= time_to_wait:
        logger.warning("Taking forever to get ready.")
        return False
    return True


def start_client() -> "Coroutine[Any, Any, None]":
    """Return a Coroutine that can be awaited or passed to an asyncio event loop which starts the bot client and connects to Discord without blocking execution.

    Returns
    -------
    Coroutine[Any, Any, None]
    """
    return client.start(settings.get("app_token"), reconnect=True)
