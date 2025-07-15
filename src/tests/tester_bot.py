import asyncio
import inspect
import json
import logging
import sys
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Sequence, Union, cast

import discord
from beartype import beartype

sys.path.append(str(Path(__file__).parent.parent))
import globals
from validations import setup_logger

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


logger = setup_logger("test_logger", "test_logs.log", "INFO")

MISSING = discord.utils.MISSING

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

# Messages sent by the bridge bot via the interaction
received_messages: dict[int, list[discord.Message]] = defaultdict(lambda: [])

# Threads created in each channel with each name
created_threads: dict[int, dict[str, discord.Thread]] = defaultdict(lambda: {})


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


@client.event
async def on_message(message: discord.Message):
    if (bridge_bot_user := globals.client.user) and (
        (message.author.id == bridge_bot_user.id)
        or (message.application_id == bridge_bot_user.id)
    ):
        received_messages[message.channel.id].append(message)


@client.event
async def on_thread_create(thread: discord.Thread):
    created_threads[thread.parent_id][thread.name] = thread


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


class InteractionTester:
    """A class to fake Discord interactions for command testing."""

    def __init__(
        self,
        message: "discord.Message | FakeMessage",
        tester_bot_user: discord.User | discord.Member,
    ):
        self.id = 0
        self.user = tester_bot_user
        self.message = message
        self.channel = message.channel
        self.channel_id = message.channel.id
        self.guild = message.guild
        self.response = InteractionResponseTester(self)
        self.response_edits: list["discord.Message | FakeMessage"] = []
        self.followup = WebhookTester(self)

    async def edit_original_response(
        self,
        *,
        content: Optional[str] = MISSING,
        embed: Optional[discord.Embed] = MISSING,
        embeds: Sequence[discord.Embed] = MISSING,
        attachments: (Sequence[Union[discord.Attachment, discord.File]]) = MISSING,
        view: Optional[discord.ui.View] = MISSING,
        allowed_mentions: Optional[discord.AllowedMentions] = None,
        poll: discord.Poll = MISSING,
    ) -> "discord.Message | FakeMessage | None":
        message_to_edit = (
            self.response_edits[-1]
            if self.response_edits
            else self.response.message or self.message
        )

        # -----
        if content is MISSING:
            content = ""

        # -----
        if embed is not MISSING and embed:
            embeds = [embed]
        if embeds is MISSING:
            embeds = []
        embeds = list(embeds)
        embeds.insert(
            0,
            discord.Embed.from_dict(
                {
                    "type": "rich",
                    "description": "-# This message is an edit to the previous response.",
                }
            ),
        )

        # -----
        arguments: dict[str, Any] = {"embeds": embeds}
        if attachments is not MISSING:
            files = []
            for att in attachments:
                if isinstance(att, discord.File):
                    files.append(att)
                    continue

                file = await att.to_file()
                files.append(file)

            arguments["files"] = files

        if view is not MISSING:
            arguments["view"] = view

        if allowed_mentions is not MISSING:
            arguments["allowed_mentions"] = allowed_mentions

        if poll is not MISSING:
            arguments["poll"] = poll

        # -----
        reply = await message_to_edit.reply(content, **arguments)
        self.response_edits.append(reply)
        return reply


class InteractionResponseTester:
    """A class to fake Discord interaction responses for command testing."""

    def __init__(self, interaction: InteractionTester):
        self.interaction = interaction
        self.message = None
        self.content = None
        self.args = None
        self.deferred_response = None

    async def send_message(
        self,
        content: Optional["Any"] = None,
        *,
        embed: discord.Embed = MISSING,
        embeds: Sequence[discord.Embed] = MISSING,
        file: discord.File = MISSING,
        files: Sequence[discord.File] = MISSING,
        view: discord.ui.View = MISSING,
        tts: bool = False,
        ephemeral: bool = False,
        allowed_mentions: discord.AllowedMentions = MISSING,
        suppress_embeds: bool = False,
        silent: bool = False,
        delete_after: Optional[float] = None,
        poll: discord.Poll = MISSING,
    ) -> "discord.Message | FakeMessage | None":
        logger.debug("Sending interaction response...")
        arguments = {}

        if suppress_embeds:
            embed = embeds = MISSING
        elif embed is not MISSING:
            embeds = [embed]
        if embeds is MISSING:
            embeds = []
        embeds = list(embeds)

        if ephemeral:
            embeds.insert(
                0,
                discord.Embed.from_dict(
                    {
                        "type": "rich",
                        "description": "-# This interaction response was ephemeral.",
                    }
                ),
            )

        if delete_after is not None:
            embeds.insert(
                0,
                discord.Embed.from_dict(
                    {
                        "type": "rich",
                        "description": f"-# This interaction was marked as needing to be deleted after {delete_after} seconds.",
                    }
                ),
            )

        if embeds is not MISSING:
            arguments["embeds"] = embeds

        if file is not MISSING:
            files = [file]
        if files is not MISSING:
            arguments["files"] = files

        if view is not MISSING:
            arguments["view"] = view

        if tts is not MISSING:
            arguments["tts"] = tts

        if allowed_mentions is not MISSING:
            arguments["allowed_mentions"] = allowed_mentions

        if silent is not MISSING:
            arguments["silent"] = silent

        if poll is not MISSING:
            arguments["poll"] = poll

        self.message = await self.interaction.message.reply(content, **arguments)
        logger.debug("Sent.")

        self.content = content
        self.args = arguments

        return self.message

    async def defer(
        self,
        *,
        ephemeral: bool = False,
        thinking: bool = False,
    ):
        self.deferred_response = InteractionResponseTester(self.interaction)
        await self.deferred_response.send_message(
            f"Interaction was deferred with with thinking = {thinking}.",
            ephemeral=ephemeral,
        )


class WebhookTester(InteractionResponseTester):
    def __init__(self, interaction: InteractionTester):
        super().__init__(interaction)

    async def send(
        self,
        content: str = MISSING,
        *,
        username: str = MISSING,
        avatar_url: "Any" = MISSING,
        tts: bool = False,
        ephemeral: bool = False,
        file: discord.File = MISSING,
        files: Sequence[discord.File] = MISSING,
        embed: discord.Embed = MISSING,
        embeds: Sequence[discord.Embed] = MISSING,
        allowed_mentions: discord.AllowedMentions = MISSING,
        view: discord.ui.View = MISSING,
        thread: discord.abc.Snowflake = MISSING,
        thread_name: str = MISSING,
        wait: bool = False,
        suppress_embeds: bool = False,
        silent: bool = False,
        applied_tags: List[discord.ForumTag] = MISSING,
        poll: discord.Poll = MISSING,
    ) -> "discord.Message | FakeMessage | None":
        return await self.send_message(
            content,
            embed=embed,
            embeds=embeds,
            file=file,
            files=files,
            view=view,
            tts=tts,
            ephemeral=ephemeral,
            allowed_mentions=allowed_mentions,
            suppress_embeds=suppress_embeds,
            silent=silent,
            poll=poll,
        )


class FakeMessage:
    """A class to fake messages sent by the client that shouldn't be expected."""

    def __init__(self, content: str, channel: "discord.abc.MessageableChannel"):
        self.content = content
        self.channel = channel
        self.guild = channel.guild

    async def reply(self, *args, **kwargs) -> "FakeMessage":
        return FakeMessage(
            (
                content_kw
                if isinstance(content_kw := kwargs.get("content"), str)
                else (args[0] if args and isinstance(args[0], str) else "")
            ),
            self.channel,
        )


@beartype
async def process_tester_bot_command(
    message: discord.Message | FakeMessage,
    tester_bot_user: discord.User,
) -> bool:
    """Running unit tests and the tester bot sent a command. This function returns True if a command was found and processed and False otherwise.

    Parameters
    ----------
    message : :class:`~discord.Message`
        The message the tester bot sent. Its content must start with a forward slash ("/").
    tester_bot_user : :class:`~discord.User`
        The user object of the tester bot.

    Returns
    -------
    bool
    """
    command_and_args = message.content.strip()[1:].split()

    # -----
    logger.debug("Tester bot sent a slash command: %s", command_and_args[0])
    if not (command := globals.command_tree.get_command(command_and_args[0])):
        command = globals.command_tree.get_command(
            command_and_args[0],
            guild=message.guild,
        )
    if not isinstance(command, discord.app_commands.Command):
        logger.debug("Invalid command; treating message as regular message.")
        return False

    # -----
    args = [
        (
            False
            if arg.strip().lower() == "false"
            else (True if arg.strip().lower() == "true" else arg)
        )
        for arg in command_and_args[1:]
    ]
    logger.debug(
        "Callback function: %s%s",
        command.callback.__name__,
        inspect.signature(command.callback),
    )
    logger.debug("Arguments passed: %s", args)

    # -----
    logger.debug("Fetching server member...")
    if message_guild := message.guild:
        if not (member := message_guild.get_member(tester_bot_user.id)):
            try:
                member = await message_guild.fetch_member(tester_bot_user.id)
            except Exception:
                member = tester_bot_user

        if logger.level <= logging.DEBUG:
            if isinstance(member, discord.Member):
                logger.debug("Found server member.")
            else:
                logger.debug("Did not find server member; member is bot's user.")
    else:
        logger.debug("Interaction was not sent from a server; member is bot's user.")
        member = tester_bot_user

    # -----
    logger.debug("Executing command...")
    try:
        await command.callback(
            cast(
                discord.Interaction,
                InteractionTester(message, member),
            ),
            *args,  # type: ignore
        )
    except Exception as e:
        logger.error(e)
        raise
    logger.debug("Command executed.")

    return True
