from __future__ import annotations

import asyncio
import inspect
import re
from copy import deepcopy
from typing import (
    Any,
    Coroutine,
    Literal,
    NamedTuple,
    NotRequired,
    TypedDict,
    overload,
)

import discord
from beartype import beartype
from sqlalchemy import Delete as SQLDelete
from sqlalchemy import ScalarResult
from sqlalchemy import Select as SQLSelect
from sqlalchemy import and_ as sql_and
from sqlalchemy import or_ as sql_or
from sqlalchemy.exc import StatementError as SQLError
from sqlalchemy.orm import Session as SQLSession
from sqlalchemy.sql import func

import commands
import emoji_hash_map
import globals
from bridge import Bridge, bridges
from database import (
    DBAppWhitelist,
    DBAutoBridgeThreadChannels,
    DBMessageMap,
    DBReactionMap,
    Session,
    sql_command,
    sql_retry,
)
from validations import ChannelTypeError, logger


class ThreadSplat(TypedDict, total=False):
    """Helper class to perform bridge operations on threads."""

    thread: discord.Thread


@globals.client.event
async def on_ready():
    """Load the data registered in the database into memory.

    This function is called when the client is done preparing the data received from Discord. Usually after login is successful and the Client.guilds and co. are filled up.

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
    if globals.is_ready:
        return

    logger.info("Client successfully connected. Running initial loading procedures...")

    try:
        logger.info("Loading bridges from database...")
        await bridges.load_from_database()
        logger.info("Bridges loaded.")

        logger.info("Loading emoji hash map from database...")
        emoji_hash_map.map = emoji_hash_map.EmojiHashMap()
        logger.info("Emoji hash map loaded.")

        with Session.begin() as session:
            # Try to find all apps whitelisted per channel
            logger.info("Loading whitelisted apps...")
            select_whitelisted_apps: SQLSelect[tuple[DBAppWhitelist]] = SQLSelect(
                DBAppWhitelist
            )
            whitelisted_apps_query_result: ScalarResult[DBAppWhitelist] = (
                session.scalars(select_whitelisted_apps)
            )
            accessible_channels: set[int] = set()
            inaccessible_channels: set[int] = set()
            for whitelisted_app in whitelisted_apps_query_result:
                channel_id = int(whitelisted_app.channel)
                if channel_id in inaccessible_channels:
                    continue

                logger.debug(
                    "Adding app with ID %s to whitelist associated with channel with ID %s.",
                    whitelisted_app.application,
                    channel_id,
                )

                if channel_id not in accessible_channels:
                    channel = await globals.get_channel_from_id(channel_id)

                    if channel:
                        accessible_channels.add(channel_id)
                    else:
                        logger.debug(
                            "Channel with ID %s not found when loading list of whitelisted apps.",
                            channel_id,
                        )
                        inaccessible_channels.add(channel_id)
                        continue

                if not globals.per_channel_whitelist.get(channel_id):
                    globals.per_channel_whitelist[channel_id] = set()

                globals.per_channel_whitelist[channel_id].add(
                    int(whitelisted_app.application)
                )

            if len(inaccessible_channels) > 0:
                delete_inaccessible_channels = SQLDelete(DBAppWhitelist).where(
                    DBAppWhitelist.channel.in_(inaccessible_channels)
                )
                session.execute(delete_inaccessible_channels)

            logger.info("Whitelists loaded.")

            # Identify all automatically-thread-bridging channels
            logger.info("Loading automatically-thread-bridging channels...")
            select_auto_bridge_thread_channels: SQLSelect[
                tuple[DBAutoBridgeThreadChannels]
            ] = SQLSelect(DBAutoBridgeThreadChannels)
            auto_thread_query_result: ScalarResult[DBAutoBridgeThreadChannels] = (
                session.scalars(select_auto_bridge_thread_channels)
            )
            globals.auto_bridge_thread_channels = (
                globals.auto_bridge_thread_channels.union(
                    {
                        int(auto_bridge_thread_channel.channel)
                        for auto_bridge_thread_channel in auto_thread_query_result
                    }
                )
            )
            logger.info("Auto-thread-bridging channels loaded.")
    except Exception as e:
        await globals.client.close()
        logger.error("An error occurred when performing bot startup procedures: %s", e)
        raise

    # Finally I'll check whether I have a registered emoji server and save it if so
    logger.info("Loading emoji server...")
    emoji_server_id_str = globals.settings.get("emoji_server_id")
    try:
        if emoji_server_id_str:
            emoji_server_id = int(emoji_server_id_str)
        else:
            emoji_server_id = None
    except Exception:
        logger.warning(
            "Emoji server ID stored in settings.json file does not resolve to a valid integer."
        )
        emoji_server_id = None

    if emoji_server_id:
        emoji_server = globals.client.get_guild(emoji_server_id)
        if not emoji_server:
            try:
                emoji_server = await globals.client.fetch_guild(emoji_server_id)
            except Exception:
                emoji_server = None

        if not emoji_server:
            logger.warning(
                "Couldn't find emoji server with ID registered in settings.json."
            )
        elif (
            not emoji_server.me.guild_permissions.manage_expressions
            or not emoji_server.me.guild_permissions.create_expressions
        ):
            logger.warning(
                "I don't have Create Expressions and Manage Expressions permissions in the emoji server."
            )
        else:
            globals.emoji_server = emoji_server

        logger.info("Emoji server loaded.")
    else:
        logger.info("Emoji server ID not set.")

    logger.info("Syncing command tree...")
    sync_command_tree = [globals.command_tree.sync()]
    if globals.emoji_server:
        sync_command_tree.append(globals.command_tree.sync(guild=globals.emoji_server))
    await asyncio.gather(*sync_command_tree)
    logger.info("Command tree synced.")

    if (connected_servers := globals.client.guilds) and len(connected_servers) > 0:
        print(f"{globals.client.user} is connected to the following servers:\n")
        connected_servers_listed: list[str] = []
        for server in globals.client.guilds:
            server_id_str = f"{server.name}(id: {server.id})"
            print(server_id_str)
            connected_servers_listed.append(server_id_str)

        logger.info(
            "Connected to the following servers:\n- %s",
            "\n- ".join(connected_servers_listed),
        )
    else:
        print("Bot is not connected to any servers.")
        logger.info("Bot is not connected to any servers.")

    globals.is_connected = True
    globals.is_ready = True
    logger.info("Bot is ready.")


@globals.client.event
async def on_typing(
    channel: discord.abc.Messageable,
    user: discord.User | discord.Member,
    _,
):
    """Make the bot start typing across bridges when a user on the source end of a bridge does so.

    Parameters
    ----------
    channel : :class:`~discord.abc.Messageable`
        The a user is typing in.
    user : :class:`~discord.User` | :class:`~discord.Member`
        The user that is typing in the channel.
    """
    if not (
        globals.is_ready
        and globals.is_connected
        and globals.rate_limiter.has_capacity()
        and isinstance(channel, (discord.TextChannel, discord.Thread))
        and globals.client.user
        and globals.client.user.id != user.id
    ):
        return

    outbound_bridges = bridges.get_outbound_bridges(channel.id)
    if not outbound_bridges:
        return

    async def type_through_bridge(bridge: Bridge):
        try:
            target_channel = await bridge.target_channel
            await target_channel.typing()
        except Exception:
            pass

    async with globals.rate_limiter:
        channels_typing: list[Coroutine[Any, Any, None]] = []
        for _, bridge in outbound_bridges.items():
            channels_typing.append(type_through_bridge(bridge))

        await asyncio.gather(*channels_typing)


@globals.client.event
async def on_message(message: discord.Message):
    """This function is called when a Message is created and sent. Requires :class:`~discord.Intents.messages` to be enabled.

    Parameters
    ----------
    message : :class:`~discord.Message`
        The message to bridge.

    Raises
    ------
    :class:`~discord.HTTPException`
        Sending a message failed.
    :class:`~discord.NotFound`
        One of the webhooks was not found.
    :class:`~discord.Forbidden`
        The authorization token for one of the webhooks is incorrect.
    ValueError
        The length of embeds was invalid, there was no token associated with one of the webhooks or ephemeral was passed with the improper webhook type or there was no state attached with one of the webhooks when giving it a view.
    """
    message_id = message.id

    lock = asyncio.Lock()
    globals.message_lock[message_id] = lock
    async with lock:
        if not isinstance(message.channel, (discord.TextChannel, discord.Thread)):
            del globals.message_lock[message_id]
            return

        if message.type not in {discord.MessageType.default, discord.MessageType.reply}:
            # Only bridge contentful messages
            del globals.message_lock[message_id]
            return

        if (
            (application_id := (message.application_id or message.author.id))
            == globals.client.application_id
        ) or (
            (
                not (
                    local_whitelist := globals.per_channel_whitelist.get(
                        message.channel.id
                    )
                )
                or (application_id not in local_whitelist)
            )
            and (
                not (global_whitelist := globals.settings.get("whitelisted_apps"))
                or (application_id not in [int(app_id) for app_id in global_whitelist])
            )
        ):
            # Don't bridge messages from non-whitelisted applications or from self
            del globals.message_lock[message_id]
            return

        if not await globals.wait_until_ready():
            del globals.message_lock[message_id]
            return

        await bridge_message_helper(message)


@beartype
async def bridge_message_helper(message: discord.Message):
    """Mirror a message to all of its outbound bridge targets.

    This function is called when a Message is created and sent. Requires :class:`~discord.Intents.messages` to be enabled.

    Parameters
    ----------
    message : :class:`~discord.Message`
        The message to bridge.

    Raises
    ------
    :class:`~discord.HTTPException`
        Sending a message failed.
    :class:`~discord.NotFound`
        One of the webhooks was not found.
    :class:`~discord.Forbidden`
        The authorization token for one of the webhooks is incorrect.
    ValueError
        The length of embeds was invalid, there was no token associated with one of the webhooks or ephemeral was passed with the improper webhook type or there was no state attached with one of the webhooks when giving it a view.
    """
    message_channel_id = message.channel.id
    outbound_bridges = bridges.get_outbound_bridges(message_channel_id)
    if not outbound_bridges:
        return

    logger.debug(
        "Bridging message with ID %s from channel with ID %s.",
        message.id,
        message_channel_id,
    )

    # Get all channels reachable from this one via an unbroken sequence of outbound bridges as well as their webhooks
    reachable_channels = await bridges.get_reachable_channels(
        message_channel_id,
        "outbound",
        include_webhooks=True,
    )
    if len(reachable_channels) == 0:
        logger.debug(
            "No channels are reachable from channel with ID %s.",
            message.id,
            message_channel_id,
        )
        return

    try:
        with Session.begin() as session:
            # Check whether this message is a reference to another message, i.e. if it's a reply or a forward
            message_reference = message.reference
            original_message = message
            original_message_channel = message.channel
            if message_reference:
                resolved_message_reference = message_reference.resolved
                message_reference_id = message_reference.message_id

                if isinstance(resolved_message_reference, discord.Message):
                    # Original message is cached and I can just fetch it
                    original_message = resolved_message_reference
                    original_message_channel = original_message.channel
                elif message_reference_id:
                    # Try to find the original message, if it's not resolved
                    original_message_channel = await globals.get_channel_from_id(
                        message_reference.channel_id
                    )
                    if isinstance(
                        original_message_channel,
                        (discord.TextChannel, discord.Thread),
                    ):
                        # I have access to the channel of the original message being forwarded
                        try:
                            # Try to find the original message
                            original_message = (
                                await original_message_channel.fetch_message(
                                    message_reference_id
                                )
                            )
                        except Exception:
                            pass
                    else:
                        original_message_channel = message.channel
            else:
                resolved_message_reference = None
                message_reference_id = None

            if (
                not message.message_snapshots
                or len(message.message_snapshots) == 0
                or not message_reference
            ):
                # Regular message with content (probably)
                logger.debug(
                    "Message with ID %s doesn't have snapshots, is probably not forwarded.",
                    message.id,
                )

                forwarded_message = None
                forwarded_message_channel_is_nsfw = False

                message_content = await replace_missing_emoji(message.content, session)
                message_attachments = message.attachments
                message_embeds = message.embeds
            else:
                # There is a message snapshot, so this message was forwarded
                logger.debug(
                    "Message with ID %s has snapshots, is forwarded.", message.id
                )

                forwarded_message = original_message
                original_message_channel_parent = original_message_channel
                if isinstance(original_message_channel, discord.Thread) and (
                    possible_parent := original_message_channel.parent
                ):
                    original_message_channel_parent = possible_parent
                forwarded_message_channel_is_nsfw = (
                    isinstance(
                        original_message_channel_parent,
                        discord.TextChannel
                        | discord.VoiceChannel
                        | discord.StageChannel
                        | discord.ForumChannel
                        | discord.CategoryChannel,
                    )
                    and original_message_channel_parent.nsfw
                )

                message_content = ""
                message_attachments = []
                message_embeds = []

            bridged_reply_to: dict[int, int] = {}
            replied_to_author = None
            replied_to_content = None
            reply_has_ping = False
            if message.type == discord.MessageType.reply:
                # This message is a reply to another message, so we should try to link to its match on the other side of bridges
                # bridged_reply_to will be a dict whose keys are channel IDs and whose values are the IDs of messages matching the
                # message I'm replying to in those channels
                message_is_reply = True

                if original_message.id != message.id:
                    replied_to_message = original_message
                else:
                    replied_to_message = resolved_message_reference
                if isinstance(replied_to_message, discord.Message):
                    replied_to_content = await replace_missing_emoji(
                        globals.truncate(
                            discord.utils.remove_markdown(
                                replied_to_message.clean_content
                            ),
                            50,
                        ),
                        session,
                    )
                    replied_to_author = replied_to_message.author

                    # identify if this reply "pinged" the target, to know whether to add the @ symbol UI
                    reply_has_ping = any(
                        x.id == replied_to_author.id for x in message.mentions
                    )

                # First, check whether the message replied to was itself bridged from a different channel
                select_message_map: SQLSelect[tuple[DBMessageMap]] = SQLSelect(
                    DBMessageMap
                ).where(DBMessageMap.target_message == str(message_reference_id))
                local_replied_to_message_map: DBMessageMap | None = await sql_retry(
                    lambda: session.scalars(select_message_map).first()
                )
                if isinstance(local_replied_to_message_map, DBMessageMap):
                    # So the message replied to was bridged from elsewhere
                    reply_source_channel_id = int(
                        local_replied_to_message_map.source_channel
                    )
                    source_replied_to_id = int(
                        local_replied_to_message_map.source_message
                    )
                    bridged_reply_to[reply_source_channel_id] = source_replied_to_id

                    try:
                        # Try to find the author of the original message
                        reply_source_channel = await globals.get_channel_from_id(
                            reply_source_channel_id,
                            ensure_text_or_thread=True,
                        )
                        source_replied_to = await reply_source_channel.fetch_message(
                            source_replied_to_id
                        )

                        replied_to_author = source_replied_to.author
                    except Exception:
                        pass
                else:
                    source_replied_to_id = message_reference_id

                # Now find all other bridged versions of the message we're replying to
                select_bridged_reply_to: SQLSelect[tuple[DBMessageMap]] = SQLSelect(
                    DBMessageMap
                ).where(DBMessageMap.source_message == str(source_replied_to_id))
                query_result: ScalarResult[DBMessageMap] = await sql_retry(
                    lambda: session.scalars(select_bridged_reply_to)
                )
                for message_map in query_result:
                    bridged_reply_to[int(message_map.target_channel)] = int(
                        message_map.target_message
                    )
            else:
                message_is_reply = False

            # Check who, if anyone, is pinged in the message
            people_to_ping = {m.id for m in message.mentions}
            # Remove everyone who was already successfully pinged in the message in the original channel
            message_channel = await globals.get_channel_parent(message.channel)
            people_to_ping.difference_update(
                {member.id for member in message_channel.members}
            )

            # Send a message out to each target webhook
            async_bridged_messages: list[
                Coroutine[Any, Any, list[BridgedMessage] | None]
            ] = []
            for target_id, webhook in reachable_channels.items():
                if not webhook:
                    continue

                webhook_channel = webhook.channel
                if not isinstance(webhook_channel, discord.TextChannel):
                    continue

                target_channel = await globals.get_channel_from_id(
                    target_id,
                    ensure_text_or_thread=True,
                )

                thread_splat: ThreadSplat = {}
                if target_id != webhook_channel.id:
                    # The target channel is not the same as the webhook's channel, so it should be a thread
                    if not isinstance(target_channel, discord.Thread):
                        logger.warning(
                            "Target channel for bridging (ID %s) does not match its associated webhook (ID %s).",
                            target_id,
                            webhook_channel.id,
                        )
                        continue
                    thread_splat = {"thread": target_channel}

                # Create an async version of bridging this message to gather at the end
                async_bridged_messages.append(
                    bridge_message_to_target_channel(
                        message,
                        message_content,
                        message_attachments,
                        deepcopy(message_embeds),
                        deepcopy(people_to_ping),
                        target_channel,
                        webhook,
                        webhook_channel,
                        message_is_reply,
                        replied_to_author,
                        replied_to_content,
                        bridged_reply_to.get(target_id),
                        reply_has_ping,
                        forwarded_message,
                        forwarded_message_channel_is_nsfw,
                        thread_splat,
                        session,
                    )
                )
                people_to_ping.difference_update(
                    {member.id for member in webhook_channel.members}
                )

            if len(async_bridged_messages) == 0:
                return

            # Insert references to the linked messages into the message_mappings table
            bridged_messages: list[BridgedMessage] = [
                bridged_message
                for bridged_message_list in (
                    await asyncio.gather(*async_bridged_messages)
                )
                if bridged_message_list
                for bridged_message in bridged_message_list
            ]
            source_message_id_str = str(message.id)
            source_channel_id_str = str(message_channel_id)
            await sql_retry(
                lambda: session.add_all(
                    [
                        DBMessageMap(
                            source_message=source_message_id_str,
                            source_channel=source_channel_id_str,
                            target_message=bridged_message.id,
                            target_channel=bridged_message.channel_id,
                            forward_header_message=bridged_message.forwarded_header_id,
                            target_message_order=bridged_message.message_order,
                            webhook=bridged_message.webhook_id,
                        )
                        for bridged_message in bridged_messages
                    ]
                )
            )
    except Exception as e:
        if isinstance(e, SQLError):
            logger.warning(
                "Ran into an SQL error while trying to bridge a message: %s", e
            )
        else:
            logger.error(
                "Ran into an unknown error while trying to bridge a message: %s", e
            )

        raise

    logger.debug("Message with ID %s successfully bridged.", message.id)


class BridgedMessage(NamedTuple):
    id: int
    message_order: int
    channel_id: int
    webhook_id: int | None
    forwarded_header_id: int | None


class ReplyEmbedDict(TypedDict, total=False):
    type: Literal["rich"]
    description: str
    url: NotRequired[str]
    thumbnail: NotRequired["ReplyEmbedThumbnailDict"]


class ReplyEmbedThumbnailDict(TypedDict):
    url: str
    height: int
    width: int


@beartype
async def bridge_message_to_target_channel(
    sent_message: discord.Message,
    message_content: str,
    message_attachments: list[discord.Attachment],
    message_embeds: list[discord.Embed],
    people_to_ping: set[int],
    target_channel: discord.TextChannel | discord.Thread,
    webhook: discord.Webhook,
    webhook_channel: discord.TextChannel,
    message_is_reply: bool,
    replied_to_author: discord.User | discord.Member | None,
    replied_to_content: str | None,
    bridged_reply_to: int | None,
    reply_has_ping: bool,
    forwarded_message: discord.Message | None,
    forwarded_message_channel_is_nsfw: bool,
    thread_splat: ThreadSplat,
    session: SQLSession,
) -> list[BridgedMessage] | None:
    """Bridge a message to a channel and return a list of dictionaries with information about the message bridged. Multiple dictionaries are returned in case a message has to be split into multiple ones due to message length.

    Parameters
    ----------
    sent_message : :class:`~discord.Message`
        The message being bridged.
    message_content : str
        Its contents.
    message_attachments : list[:class:`~discord.Attachment`]
        Its attachments.
    message_embeds : list[:class:`~discord.Embed`]
        Its embeds.
    people_to_ping : set[int]
        A set of IDs of people that were @-mentioned in the original message and who haven't already been pinged.
    target_channel : :class:`~discord.TextChannel` | :class:`~discord.Thread`
        The channel or thread the message is being bridged to.
    webhook : :class:`~discord.Webhook`
        The webhook that will send the message.
    webhook_channel : :class:`~discord.TextChannel`
        The channel the webhook is attached to.
    message_is_reply : bool
        Whether the message being bridged is replying to another message.
    replied_to_author : :class:`~discord.User` | :class:`~discord.Member` | None
        The author of the message the message being bridged is replying to, if it is a reply.
    replied_to_content : str | None
        The content of the message the message being bridged is replying to. Defaults to None, in which case it will be fetched from the matching message in the target channel if it can.
    bridged_reply_to : int | None
        The ID of a message the message being bridged is replying to in the target channel.
    reply_has_ping : bool
        Whether the reply is pinging the author of the original message
    forwarded_message : :class:`~discord.Message` | None
        A message being forwarded, in case it is a forward.
    forwarded_message_channel_is_nsfw : bool
        Whether the origin channel of the message being forwarded from is NSFW.
    thread_splat : ThreadSplat
        A splat with the thread this message is being bridged to, if any.
    session : :class:`~sqlalchemy.orm.Session`
        An SQLAlchemy ORM Session connecting to the database.

    Returns
    -------
    list[BridgedMessage] | None
    """
    logger.debug(
        "Bridging message with ID %s to channel with ID %s.",
        sent_message.id,
        target_channel.id,
    )

    # Lock the channel to preserve message ordering (particularly when doing message forwards)
    target_channel_id = target_channel.id
    lock = globals.channel_lock.get(target_channel_id)
    if not lock:
        lock = globals.channel_lock[target_channel_id] = asyncio.Lock()

    async with lock:
        return await _bridge_message_to_target_channel(
            sent_message,
            message_content,
            message_attachments,
            message_embeds,
            people_to_ping,
            target_channel,
            webhook,
            webhook_channel,
            message_is_reply,
            replied_to_author,
            replied_to_content,
            bridged_reply_to,
            reply_has_ping,
            forwarded_message,
            forwarded_message_channel_is_nsfw,
            thread_splat,
            session,
        )


async def _bridge_message_to_target_channel(
    sent_message: discord.Message,
    message_content: str,
    message_attachments: list[discord.Attachment],
    message_embeds: list[discord.Embed],
    people_to_ping: set[int],
    target_channel: discord.TextChannel | discord.Thread,
    webhook: discord.Webhook,
    webhook_channel: discord.TextChannel,
    message_is_reply: bool,
    replied_to_author: discord.User | discord.Member | None,
    replied_to_content: str | None,
    bridged_reply_to: int | None,
    reply_has_ping: bool,
    forwarded_message: discord.Message | None,
    forwarded_message_channel_is_nsfw: bool,
    thread_splat: ThreadSplat,
    session: SQLSession,
) -> list[BridgedMessage] | None:
    """Helper function to bridge a message to a channel."""
    # Replace Discord links in the message and embed text
    message_content = await replace_discord_links(
        message_content,
        target_channel,
        session,
    )
    for embed in message_embeds:
        embed.description = await replace_discord_links(
            embed.description,
            target_channel,
            session,
        )
        embed.title = await replace_discord_links(
            embed.title,
            target_channel,
            session,
        )

    # Try to find whether the user who sent this message is on the other side of the bridge and if so what their name and avatar would be
    bridged_member = await globals.get_channel_member(
        webhook_channel,
        sent_message.author.id,
    )
    if bridged_member:
        bridged_member_name = bridged_member.display_name
        bridged_avatar_url = bridged_member.display_avatar
        bridged_member_id = bridged_member.id
    else:
        bridged_member_name = sent_message.author.display_name
        bridged_avatar_url = sent_message.author.display_avatar
        bridged_member_id = sent_message.author.id

    if message_is_reply:
        # This message is a reply to another message
        replied_to_author_avatar = None
        replied_to_author_name = ""
        jump_url = None
        reply_error_msg = ""
        if bridged_reply_to:
            # The message being replied to is also bridged to this channel, so I'll create an embed to represent this
            try:
                message_replied_to = await target_channel.fetch_message(
                    bridged_reply_to
                )
                jump_url = message_replied_to.jump_url

                # Use the author's display name and avatar if they're in this server
                replied_to_author_name = discord.utils.escape_markdown(
                    message_replied_to.author.display_name
                )
                replied_to_author_avatar = message_replied_to.author.display_avatar

                # Try to fetch the replied to content if it's not available
                if not replied_to_content:
                    replied_to_content = await replace_missing_emoji(
                        globals.truncate(
                            discord.utils.remove_markdown(
                                message_replied_to.clean_content
                            ),
                            50,
                        ),
                        session,
                    )
            except discord.HTTPException:
                reply_error_msg = "The message being replied to could not be loaded."
        else:
            reply_error_msg = (
                "The message being replied to has not been bridged or has been deleted."
            )

        if replied_to_author is not None:
            if not replied_to_author_name:
                replied_to_author_avatar = replied_to_author.display_avatar

                replied_to_author_name = discord.utils.escape_markdown(
                    replied_to_author.name
                )
        elif replied_to_content is None:
            reply_error_msg = "This message is a reply but the message being replied to could not be loaded."

        reply_embed_dict: ReplyEmbedDict = {"type": "rich"}

        if replied_to_author_avatar:
            reply_embed_dict["thumbnail"] = {
                "url": replied_to_author_avatar.replace(size=16).url,
                "height": 18,
                "width": 18,
            }

        if replied_to_author_name:
            if reply_has_ping:
                # Discord represents ping "ON" vs "OFF" replies with an @ symbol before the reply author name
                # copy this behavior here
                replied_to_author_name = f"@{replied_to_author_name}"
            replied_to_author_name = " " + replied_to_author_name

        if jump_url:
            reply_embed_dict["url"] = jump_url
            reply_symbol = f"[↪]({jump_url})"
        else:
            reply_symbol = "↪"

        if replied_to_content:
            replied_to_content = "  " + replied_to_content
        else:
            replied_to_content = ""
            # TODO: deal with the fact that forwarded messages don't have contents
            if jump_url:
                reply_error_msg = (
                    "Couldn't load contents of the message this message is replying to."
                )

        if reply_error_msg:
            reply_error_msg = f"\n\n-# {reply_error_msg}"

        reply_embed_dict["description"] = (
            f"**{reply_symbol}{replied_to_author_name}**{replied_to_content}{reply_error_msg}"
        )

        reply_embed = [discord.Embed.from_dict(reply_embed_dict)]
    else:
        reply_embed = []

    attachments = await asyncio.gather(
        *[
            attachment.to_file(spoiler=attachment.is_spoiler())
            for attachment in message_attachments
        ]
    )

    target_channel_id = target_channel.id
    webhook_id = webhook.id

    try:
        if not forwarded_message:
            # Message is not a forward
            sent_message_ids: list[int] = []
            sending_initial_message = True
            while len(message_content) > 0 or sending_initial_message:
                # Message could be too long, split it up
                # TODO: handle split emoji/words/channels/mentions/etc
                sending_initial_message = False
                truncated_message = message_content[:2000]
                message_content = message_content[2000:]
                sent_message = await webhook.send(
                    content=truncated_message,
                    allowed_mentions=discord.AllowedMentions(
                        users=[discord.Object(id=id) for id in people_to_ping],
                        roles=False,
                        everyone=False,
                    ),
                    avatar_url=bridged_avatar_url,
                    username=bridged_member_name,
                    embeds=(
                        list(message_embeds + reply_embed)
                        if len(message_content) == 0
                        else []  # Only attach embeds on the last message
                    ),
                    files=(
                        attachments
                        if len(message_content) == 0
                        else []  # Only attach files on the last message
                    ),  # TODO: might throw HHTPException if too large?
                    wait=True,
                    **thread_splat,
                )
                sent_message_ids.append(sent_message.id)
                people_to_ping = set()
            return [
                BridgedMessage(
                    id=message_id,
                    message_order=idx,
                    channel_id=target_channel_id,
                    webhook_id=webhook_id,
                    forwarded_header_id=None,
                )
                for idx, message_id in enumerate(sent_message_ids)
            ]

        # Message is a forward so I'll send a short message saying who sent it then forward it myself
        target_channel_parent = await globals.get_channel_parent(target_channel)
        if not target_channel_parent.nsfw and forwarded_message_channel_is_nsfw:
            # Messages can't be forwarded from NSFW channels to SFW channels
            sent_message = await target_channel.send(
                allowed_mentions=discord.AllowedMentions(
                    users=False,
                    roles=False,
                    everyone=False,
                ),
                content=f"> -# <@{bridged_member_id}> forwarded a message from an NSFW channel across the bridge but this channel is SFW; forwarding failed.",
            )

            return [
                BridgedMessage(
                    id=sent_message.id,
                    message_order=0,
                    channel_id=sent_message.channel.id,
                    webhook_id=sent_message.webhook_id,
                    forwarded_header_id=None,
                )
            ]

        # Either the target channel is NSFW or the source isn't, so the forwarding can work fine
        async def bridge_forwarded_message():
            forward_header = await target_channel.send(
                allowed_mentions=discord.AllowedMentions(
                    users=False,
                    roles=False,
                    everyone=False,
                ),
                content=f"> -# The following message was originally forwarded by <@{bridged_member_id}>.",
            )
            bridged_forward = await forwarded_message.forward(target_channel)
            return BridgedMessage(
                id=bridged_forward.id,
                message_order=0,
                channel_id=bridged_forward.channel.id,
                webhook_id=bridged_forward.webhook_id,
                forwarded_header_id=forward_header.id,
            )

        return [await bridge_forwarded_message()]
    except discord.NotFound:
        # Webhook is gone, delete this bridge
        logger.warning(
            "Webhook in %s:%s (ID: %s) not found, demolishing bridges to this channel and its threads.",
            target_channel.guild.name,
            target_channel.name,
            target_channel.id,
        )

        try:
            await bridges.demolish_bridges(
                target_channel=target_channel,
                session=session,
            )
        except Exception as e:
            logger.error(
                "Exception occurred when trying to demolish an invalid bridge after bridging a message: %s",
                e,
            )
            raise
        return None


@globals.client.event
async def on_raw_message_edit(payload: discord.RawMessageUpdateEvent):
    """This function is called when a message is edited. Unlike `on_message_edit()`, this is called regardless of the state of the internal message cache.

    Parameters
    ----------
    payload : :class:`~discord.RawMessageUpdateEvent`
        The raw event payload data.

    Raises
    ------
    :class:`~discord.HTTPException`
        Editing a message failed.
    :class:`~discord.Forbidden`
        Tried to edit a message that is not yours.
    ValueError
        The length of embeds was invalid, there was no token associated with a webhook or a webhook had no state.
    """
    message_id = payload.message_id

    lock = globals.message_lock.get(message_id)
    if not lock:
        lock = asyncio.Lock()
        globals.message_lock[message_id] = lock

    async with lock:
        if not globals.message_lock.get(message_id):
            # Message has been deleted before the edit receipt somehow
            return

        if not (updated_message_content := payload.data.get("content")):
            # Not a content edit
            return

        if not await globals.wait_until_ready():
            return

        channel_id = payload.channel_id
        if not bridges.get_outbound_bridges(channel_id):
            return

        await edit_message_helper(
            message_content=updated_message_content,
            embeds=[
                discord.Embed.from_dict(embed) for embed in payload.data.get("embeds")
            ],
            message_id=message_id,
            channel_id=channel_id,
            message_is_reply=(
                int(payload.data.get("type")) == discord.MessageType.reply.value
            ),
        )


@beartype
async def edit_message_helper(
    *,
    message_content: str,
    embeds: list[discord.Embed],
    message_id: int,
    channel_id: int,
    message_is_reply: bool,
):
    """Edit bridged versions of a message, if possible.

    Parameters
    ----------
    message_content : str
        The updated contents of the message.
    embeds : list[:class:`~discord.Embed`]
        The updated embeds of the message.
    message_id : int
        The message ID.
    channel_id : int
        The ID of the channel the message being edited is in.
    message_is_reply : bool
        Whether the message being edited is a reply.

    Raises
    ------
    :class:`~discord.HTTPException`
        Editing a message failed.
    :class:`~discord.Forbidden`
        Tried to edit a message that is not the Bridge's.
    ValueError
        The length of embeds was invalid, there was no token associated with a webhook or a webhook had no state.
    """
    logger.debug("Bridging edit to message with ID %s.", message_id)

    # Get all channels reachable from this one via an unbroken sequence of outbound bridges as well as their webhooks
    reachable_channels = await bridges.get_reachable_channels(
        channel_id,
        "outbound",
        include_webhooks=True,
    )

    # Find all messages matching this one
    try:
        async_message_edits: list[Coroutine[Any, Any, None]] = []
        with Session() as session:
            # Ensure that the message has emoji I have access to
            message_content = await replace_missing_emoji(message_content, session)

            for bridged_channel_id, webhook in reachable_channels.items():
                # Iterate through the target channels and edit the bridged messages
                bridged_channel = await globals.get_channel_from_id(bridged_channel_id)
                if not isinstance(
                    bridged_channel,
                    (discord.TextChannel, discord.Thread),
                ):
                    continue

                thread_splat: ThreadSplat = {}
                if isinstance(bridged_channel, discord.Thread):
                    if not isinstance(bridged_channel.parent, discord.TextChannel):
                        continue
                    thread_splat = {"thread": bridged_channel}

                target_channel = webhook.channel
                channel_specific_message_content = message_content
                channel_specific_embeds = deepcopy(embeds)
                if isinstance(target_channel, discord.TextChannel):
                    # Replace Discord links in the message and embed text
                    channel_specific_message_content = await replace_discord_links(
                        channel_specific_message_content,
                        target_channel,
                        session,
                    )
                    for embed in channel_specific_embeds:
                        embed.description = await replace_discord_links(
                            embed.description,
                            target_channel,
                            session,
                        )
                        embed.title = await replace_discord_links(
                            embed.title,
                            target_channel,
                            session,
                        )

                # Find all bridged messages associated with this one (there might be multiple if the original message was split due to length)
                select_message_map: SQLSelect[tuple[DBMessageMap]] = (
                    SQLSelect(DBMessageMap)
                    .where(
                        sql_and(
                            DBMessageMap.source_message == message_id,
                            DBMessageMap.target_channel == str(bridged_channel_id),
                        )
                    )
                    .order_by(DBMessageMap.target_message_order)
                )
                bridged_messages: ScalarResult[DBMessageMap] = await sql_retry(
                    lambda: session.scalars(select_message_map)
                )

                for message_row in bridged_messages:
                    if not message_row.webhook:
                        break

                    # The webhook returned by the call to get_reachable_channels() may not be the same as the one used to post the message
                    if (message_webhook_id := int(message_row.webhook)) != webhook.id:
                        try:
                            webhook = await globals.client.fetch_webhook(
                                message_webhook_id
                            )
                        except Exception:
                            break

                    if len(channel_specific_message_content) > 0:
                        truncated_content = channel_specific_message_content[:2000]
                        channel_specific_message_content = (
                            channel_specific_message_content[2000:]
                        )
                    else:
                        truncated_content = "-# (The original message was longer than 2000 characters but has been edited to be shorter.)"

                    try:

                        async def edit_message(
                            message_row: DBMessageMap,
                            channel_specific_embeds: list[discord.Embed],
                            bridged_channel: discord.TextChannel | discord.Thread,
                            content: str,
                            webhook: discord.Webhook,
                            thread_splat: ThreadSplat,
                            attach_embeds: bool,
                        ):
                            try:
                                bridged_message_id = int(message_row.target_message)
                                if message_is_reply and attach_embeds:
                                    # The message being edited is a reply, I need to keep its embed
                                    bridged_message_embeds = (
                                        await bridged_channel.fetch_message(
                                            bridged_message_id
                                        )
                                    ).embeds
                                    if len(bridged_message_embeds) > 0:
                                        reply_embed = bridged_message_embeds[-1]
                                        channel_specific_embeds += [reply_embed]
                                await webhook.edit_message(
                                    message_id=bridged_message_id,
                                    content=content,
                                    embeds=(
                                        channel_specific_embeds
                                        if attach_embeds
                                        else []  # Only attach embeds on the last message
                                    ),
                                    **thread_splat,
                                )
                            except discord.NotFound:
                                # Webhook is gone, delete this bridge
                                logger.warning(
                                    "Webhook in %s:%s (ID: %s) not found, demolishing bridges to this channel and its threads.",
                                    bridged_channel.guild.name,
                                    bridged_channel.name,
                                    bridged_channel.id,
                                )
                                try:
                                    await bridges.demolish_bridges(
                                        target_channel=bridged_channel,
                                        session=session,
                                    )
                                except Exception as e:
                                    logger.error(
                                        "Exception occurred when trying to demolish an invalid bridge after bridging a message edit: %s",
                                        e,
                                    )

                        async_message_edits.append(
                            edit_message(
                                message_row,
                                channel_specific_embeds,
                                bridged_channel,
                                truncated_content,
                                webhook,
                                thread_splat,
                                len(channel_specific_message_content) == 0,
                            )
                        )
                    except discord.HTTPException as e:
                        logger.warning(
                            "Ran into a Discord exception while trying to edit a message across a bridge:\n"
                            + str(e)
                        )

            await asyncio.gather(*async_message_edits)
    except Exception as e:
        if isinstance(e, SQLError):
            logger.warning(
                "Ran into an SQL error while trying to edit a message: %s", e
            )
        else:
            logger.error(
                "Ran into an unknown error while trying to edit a message: %s", e
            )

        raise

    logger.debug("Successfully bridged edit to message with ID %s.", message_id)


@overload
async def replace_missing_emoji(
    message_content: str,
    session: SQLSession,
) -> str: ...


@overload
async def replace_missing_emoji(
    message_content: str,
    session: SQLSession | None = None,
) -> str: ...


@sql_command
@beartype
async def replace_missing_emoji(
    message_content: str,
    session: SQLSession,
) -> str:
    """Return a version of the contents of a message that replaces any instances of an emoji that the bot can't find with matching ones, if possible.

    Parameters
    ----------
    message_content : str
        The content of the message to process.
    session : :class:`~sqlalchemy.orm.Session` | None, optional
        An SQLAlchemy ORM Session connecting to the database. Defaults to None, in which case a new one will be created.

    Returns
    -------
    str

    Raises
    ------
    :class:`~discord.HTTPResponseError`
        HTTP request to fetch image returned a status other than 200.
    :class:`~discord.InvalidURL`
        URL generated from emoji was not valid.
    :class:`~discord.RuntimeError`
        Session connection failed.
    :class:`~discord.ServerTimeoutError`
        Connection to server timed out.
    """
    if not globals.emoji_server:
        # If we don't have an emoji server to store our own versions of emoji in then there's nothing we can do
        return message_content

    message_emoji: set[tuple[str, str]] = set(
        re.findall(r"<(a?:[^:]+):(\d+)>", message_content)
    )
    if len(message_emoji) == 0:
        # Message has no emoji
        return message_content

    emoji_to_replace: dict[str, str] = {}
    for emoji_name, emoji_id_str in message_emoji:
        emoji_id = int(emoji_id_str)
        emoji = globals.client.get_emoji(emoji_id)
        if emoji and emoji.is_usable():
            # I already have access to this emoji so it's fine
            continue

        try:
            await emoji_hash_map.map.ensure_hash_map(
                emoji_id=emoji_id,
                emoji_name=emoji_name,
                session=session,
            )
        except Exception as e:
            logger.error(
                "An error occurred when calling ensure_hash_map() from replace_missing_emoji(): %s",
                e,
            )
            continue

        if emoji := emoji_hash_map.map.get_accessible_emoji(emoji_id, skip_self=True):
            # I don't have access to this emoji but I have a matching one in my emoji mappings
            emoji_to_replace[f"<{emoji_name}:{emoji_id_str}>"] = str(emoji)
            continue

        try:
            emoji = await emoji_hash_map.map.copy_emoji_into_server(
                emoji_to_copy_id=emoji_id_str,
                emoji_to_copy_name=emoji_name,
                session=session,
            )
            if emoji:
                emoji_to_replace[f"<{emoji_name}:{emoji_id_str}>"] = str(emoji)
        except Exception:
            pass

    for missing_emoji_str, new_emoji_str in emoji_to_replace.items():
        message_content = message_content.replace(missing_emoji_str, new_emoji_str)
    return message_content


@overload
async def replace_discord_links(
    content: None,
    channel: discord.TextChannel | discord.Thread,
    session: SQLSession,
) -> None: ...


@overload
async def replace_discord_links(
    content: str,
    channel: discord.TextChannel | discord.Thread,
    session: SQLSession,
) -> str: ...


@beartype
async def replace_discord_links(
    content: str | None,
    channel: discord.TextChannel | discord.Thread,
    session: SQLSession,
) -> str | None:
    """Return a version of the contents of a string that replaces any links to other messages within the same channel or parent channel to appropriately bridged ones.

    Parameters
    ----------
    content : str | None
        The string to process. If set to None, this function returns None.
    channel : :class:`~discord.TextChannel` | :class:`~discord.Thread`
        The channel this message is being processed for.
    session : :class:`~sqlalchemy.orm.Session`
        An SQLAlchemy ORM Session connecting to the database.

    Returns
    -------
    str | None

    Raises
    ------
    :class:`~discord.RuntimeError`
        Session connection failed.
    :class:`~discord.ServerTimeoutError`
        Connection to server timed out.
    """
    if content is None:
        return None

    message_links: set[tuple[str, str, str, str]] = set(
        re.findall(r"discord(app)?.com/channels/(\d+|@me)/(\d+)/(\d+)", content)
    )
    if len(message_links) == 0:
        # No links to replace
        return content

    logger.debug(
        "Replacing discord links when bridging message into channel with ID %s.",
        channel.id,
    )
    guild_id = channel.guild.id

    # Get all reachable channel IDs from the current channel
    channel_id = channel.id
    channel_ids_to_check = {str(channel_id)}
    bridged_channel_ids: set[int] = set().union(
        await bridges.get_reachable_channels(channel_id, "outbound"),
        await bridges.get_reachable_channels(channel_id, "inbound"),
    )

    # If the current channel is actually a thread, get reachable channel IDs from its parent
    parent_channel = await globals.get_channel_parent(channel)
    if isinstance(channel, discord.Thread):
        parent_channel_id = parent_channel.id
        channel_ids_to_check.add(str(parent_channel_id))
        bridged_channel_ids = bridged_channel_ids.union(
            await bridges.get_reachable_channels(parent_channel_id, "outbound"),
            await bridges.get_reachable_channels(parent_channel_id, "inbound"),
        )

    # Get the channel IDs of all threads of the channel
    for thread in parent_channel.threads:
        thread_id = thread.id
        if thread_id == channel_id:
            continue

        channel_ids_to_check.add(str(thread_id))
        bridged_channel_ids = bridged_channel_ids.union(
            await bridges.get_reachable_channels(thread_id, "outbound"),
            await bridges.get_reachable_channels(thread_id, "inbound"),
        )

    # Now try to find equivalent links if the messages being linked to are bridged
    for _, link_guild_id, link_channel_id, link_message_id in message_links:
        if int(link_channel_id) not in bridged_channel_ids:
            continue

        # The message being linked is from a channel that is bridged to the current channel
        select_message_map: SQLSelect[tuple[DBMessageMap]] = SQLSelect(
            DBMessageMap
        ).where(
            sql_or(
                sql_and(
                    DBMessageMap.source_message == link_message_id,
                    DBMessageMap.target_channel.in_(channel_ids_to_check),
                ),
                sql_and(
                    DBMessageMap.target_message == link_message_id,
                    DBMessageMap.source_channel.in_(channel_ids_to_check),
                ),
            )
        )
        bridged_messages: ScalarResult[DBMessageMap] = await sql_retry(
            lambda: session.scalars(select_message_map)
        )
        for message_row in bridged_messages:
            if message_row.source_message == link_message_id:
                content = content.replace(
                    f"{link_guild_id}/{link_channel_id}/{link_message_id}",
                    f"{guild_id}/{message_row.target_channel}/{message_row.target_message}",
                )
            else:
                content = content.replace(
                    f"{link_guild_id}/{link_channel_id}/{link_message_id}",
                    f"{guild_id}/{message_row.source_channel}/{message_row.source_message}",
                )

            break

    return content


@globals.client.event
async def on_raw_message_delete(payload: discord.RawMessageDeleteEvent):
    """This function is called when a message is deleted. Unlike `on_message_delete()`, this is called regardless of the message being in the internal message cache or not.

    Parameters
    ----------
    payload : :class:`~discord.RawMessageDeleteEvent`
        The raw event payload data.

    Raises
    ------
    :class:`~discord.HTTPException`
        Deleting a message failed.
    :class:`~discord.Forbidden`
        Tried to delete a message that is not yours.
    ValueError
        A webhook does not have a token associated with it.
    """
    message_id = payload.message_id

    lock = globals.message_lock.get(message_id)
    if not lock:
        lock = asyncio.Lock()
        globals.message_lock[message_id] = lock

    async with lock:
        if not globals.message_lock.get(message_id):
            # Message has been deleted already (two deletion receipts?)
            return

        if not await globals.wait_until_ready():
            return

        channel_id = payload.channel_id
        if not bridges.get_outbound_bridges(channel_id):
            return

        await delete_message_helper(message_id, channel_id)

        del globals.message_lock[message_id]


@beartype
async def delete_message_helper(message_id: int, channel_id: int):
    """Delete bridged versions of a message, if possible.

    Parameters
    ----------
    message_id : int
        The message ID.
    channel_id : int
        The ID of the channel the message being deleted is in.

    Raises
    ------
    :class:`~discord.HTTPException`
        Deleting a message failed.
    :class:`~discord.Forbidden`
        Tried to delete a message that is not yours.
    ValueError
        A webhook does not have a token associated with it.
    """
    logger.debug("Bridging deletion of message with ID %s.", message_id)

    # Get all channels reachable from this one via an unbroken sequence of outbound bridges as well as their webhooks
    reachable_channels = await bridges.get_reachable_channels(
        channel_id,
        "outbound",
        include_webhooks=True,
    )

    # Find all messages matching this one
    try:
        async_message_deletes: list[Coroutine[Any, Any, None]] = []
        with Session.begin() as session:
            select_message_map: SQLSelect[tuple[DBMessageMap]] = SQLSelect(
                DBMessageMap
            ).where(DBMessageMap.source_message == message_id)
            bridged_messages: ScalarResult[DBMessageMap] = await sql_retry(
                lambda: session.scalars(select_message_map)
            )
            for message_row in bridged_messages:
                target_channel_id = int(message_row.target_channel)
                if target_channel_id not in reachable_channels:
                    continue

                bridged_channel = await globals.get_channel_from_id(target_channel_id)
                if not isinstance(
                    bridged_channel,
                    (discord.TextChannel, discord.Thread),
                ):
                    continue

                thread_splat: ThreadSplat = {}
                if isinstance(bridged_channel, discord.Thread):
                    if not isinstance(bridged_channel.parent, discord.TextChannel):
                        continue
                    thread_splat = {"thread": bridged_channel}

                try:

                    async def delete_message(
                        message_row: DBMessageMap,
                        target_channel_id: int,
                        thread_splat: ThreadSplat,
                        bridged_channel: discord.TextChannel | discord.Thread,
                    ):
                        if message_row.webhook:
                            # The webhook returned by the call to get_reachable_channels() may not be the same as the one used to post the message
                            message_webhook_id = int(message_row.webhook)
                            if (
                                message_webhook_id
                                == reachable_channels[target_channel_id].id
                            ):
                                webhook = reachable_channels[target_channel_id]
                            else:
                                try:
                                    webhook = await globals.client.fetch_webhook(
                                        message_webhook_id
                                    )
                                except Exception:
                                    return

                            try:
                                await webhook.delete_message(
                                    int(message_row.target_message),
                                    **thread_splat,
                                )
                            except discord.NotFound:
                                # Webhook is gone, delete this bridge
                                logger.warning(
                                    "Webhook in %s:%s (ID: %s) not found, demolishing bridges to this channel and its threads.",
                                    bridged_channel.guild.name,
                                    bridged_channel.name,
                                    bridged_channel.id,
                                )
                                try:
                                    await bridges.demolish_bridges(
                                        target_channel=bridged_channel,
                                        session=session,
                                    )
                                except Exception as e:
                                    if not isinstance(e, discord.HTTPException):
                                        logger.error(
                                            "Exception occurred when trying to demolish an invalid bridge after bridging a message deletion: %s",
                                            e,
                                        )
                                    raise
                        elif message_row.forward_header_message:
                            # If the message doesn't have a webhook, it's forwarded
                            partial_target_channel = (
                                globals.client.get_partial_messageable(
                                    target_channel_id
                                )
                            )
                            await partial_target_channel.get_partial_message(
                                int(message_row.target_message)
                            ).delete()
                            await partial_target_channel.get_partial_message(
                                int(message_row.forward_header_message)
                            ).delete()
                        else:
                            # This should never happen
                            return

                    async_message_deletes.append(
                        delete_message(
                            message_row,
                            target_channel_id,
                            thread_splat,
                            bridged_channel,
                        )
                    )
                except discord.HTTPException as e:
                    logger.warning(
                        "Ran into a Discord exception while trying to delete a message across a bridge: %s",
                        e,
                    )

            # If the message was bridged, delete its row
            # If it was a source of bridged messages, delete all rows of its bridged versions
            await sql_retry(
                lambda: session.execute(
                    SQLDelete(DBMessageMap).where(
                        sql_or(
                            DBMessageMap.source_message == str(message_id),
                            DBMessageMap.target_message == str(message_id),
                        )
                    )
                )
            )
    except Exception as e:
        if isinstance(e, SQLError):
            logger.warning(
                "Ran into an SQL error while trying to delete a message: %s", e
            )
        else:
            logger.error(
                "Ran into an unknown error while trying to delete a message: %s", e
            )

        raise

    await asyncio.gather(*async_message_deletes)

    logger.debug("Successfully bridged deletion of message with ID %s.", message_id)


@globals.client.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    """This function is called when a message has a reaction added. Unlike `on_reaction_add()`, this is called regardless of the state of the internal message cache.

    Parameters
    ----------
    payload : :class:`~discord.RawReactionActionEvent`
        The raw event payload data.

    Raises
    ------
    :class:`~discord.HTTPResponseError`
        HTTP request to fetch image returned a status other than 200.
    :class:`~discord.InvalidURL`
        URL generated from emoji was not valid.
    :class:`~discord.RuntimeError`
        Session connection failed.
    :class:`~discord.ServerTimeoutError`
        Connection to server timed out.
    """
    if not await globals.wait_until_ready():
        return

    if not globals.client.user or payload.user_id == globals.client.user.id:
        # Don't bridge my own reaction
        return

    channel_id = payload.channel_id
    if not bridges.get_outbound_bridges(channel_id):
        # Only bridge reactions across outbound bridges
        return

    await bridge_reaction_add(
        message_id=payload.message_id,
        channel_id=channel_id,
        emoji=payload.emoji,
    )


async def bridge_reaction_add(
    *,
    message_id: int,
    channel_id: int,
    emoji: discord.PartialEmoji,
):
    """Bridge reactions added to a message, if possible.

    Parameters
    ----------
    message_id : int
        The ID of the message being reacted to.
    channel_id : int
        The ID of the channel the message is in.
    emoji : :class:`~discord.PartialEmoji`
        The emoji being added to the message.

    Raises
    ------
    :class:`~discord.HTTPResponseError`
        HTTP request to fetch image returned a status other than 200.
    :class:`~discord.InvalidURL`
        URL generated from emoji was not valid.
    :class:`~discord.RuntimeError`
        Session connection failed.
    :class:`~discord.ServerTimeoutError`
        Connection to server timed out.
    """
    logger.debug(
        "Bridging reaction add of %s to message with ID %s.",
        emoji,
        message_id,
    )

    # Choose a "fallback emoji" to use in case I don't have access to the one being reacted and the message across the bridge doesn't already have it
    fallback_emoji: discord.Emoji | str | None
    if emoji.is_custom_emoji() and (emoji_id := emoji.id):
        # is_custom_emoji() guarantees that emoji.id is not None
        # Custom emoji, I need to check whether it exists and is available to me
        # I'll add this to my hash map if it's not there already
        try:
            await emoji_hash_map.map.ensure_hash_map(emoji=emoji)
        except Exception as e:
            logger.error(
                "An error occurred when calling ensure_hash_map() from on_raw_reaction_add(): %s",
                e,
            )
            raise

        emoji_id_str = str(emoji_id)

        fallback_emoji = globals.client.get_emoji(emoji_id)
        if not fallback_emoji or not fallback_emoji.is_usable():
            # Couldn't find the reactji, will try to see if I've got it mapped locally
            fallback_emoji = emoji_hash_map.map.get_accessible_emoji(
                emoji_id, skip_self=True
            )

        if not fallback_emoji:
            # I don't have the emoji mapped locally, I'll add it to my server and update my map
            try:
                fallback_emoji = await emoji_hash_map.map.copy_emoji_into_server(
                    emoji_to_copy=emoji
                )
            except Exception:
                fallback_emoji = None
    else:
        # It's a standard emoji, it's fine
        fallback_emoji = emoji.name
        emoji_id_str = fallback_emoji

    # Get the IDs of all emoji that match the current one
    equivalent_emoji_ids = emoji_hash_map.map.get_matches(emoji, return_str=True)
    if not equivalent_emoji_ids:
        equivalent_emoji_ids = frozenset(str(emoji.id))

    # Now find the list of channels that can validly be reached via outbound chains from this channel
    reachable_channel_ids = await bridges.get_reachable_channels(
        channel_id,
        "outbound",
    )

    # Find and react to all messages matching this one
    try:
        # Create a function to add reactions to messages asynchronously and gather them all at the end
        source_message_id_str = str(message_id)
        source_channel_id_str = str(channel_id)
        async_add_reactions: list[Coroutine[Any, Any, DBReactionMap | None]] = []

        async def add_reaction_helper(
            bridged_channel: discord.TextChannel | discord.Thread,
            target_message_id: int,
        ):
            bridged_message = await bridged_channel.fetch_message(target_message_id)

            # I'll try to check whether there are already reactions in the target message matching mine
            existing_matching_emoji = None
            if fallback_emoji:
                existing_matching_emoji = next(
                    (
                        reaction.emoji
                        for reaction in bridged_message.reactions
                        if reaction.emoji == fallback_emoji
                    ),
                    None,
                )
            if not existing_matching_emoji:
                existing_matching_emoji = next(
                    (
                        emoji
                        for reaction in bridged_message.reactions
                        if (
                            (emoji := reaction.emoji)
                            and (
                                (
                                    not (is_str := isinstance(emoji, str))
                                    and (
                                        emoji.name in equivalent_emoji_ids
                                        or str(emoji.id) in equivalent_emoji_ids
                                    )
                                )
                                or (is_str and emoji in equivalent_emoji_ids)
                            )
                        )
                    ),
                    None,
                )

            if existing_matching_emoji:
                bridged_emoji = existing_matching_emoji
            elif fallback_emoji:
                bridged_emoji = fallback_emoji
            else:
                return None

            await bridged_message.add_reaction(bridged_emoji)

            # I'll return a reaction map to insert into the reaction map table
            if isinstance(bridged_emoji, str):
                bridged_emoji_id = None
                bridged_emoji_name = bridged_emoji
            else:
                bridged_emoji_id = str(bridged_emoji.id) if bridged_emoji.id else None
                bridged_emoji_name = bridged_emoji.name
                if bridged_emoji.animated:
                    bridged_emoji_name = f"a:{bridged_emoji_name}"

            return DBReactionMap(
                source_emoji=emoji_id_str,
                source_message=source_message_id_str,
                source_channel=source_channel_id_str,
                target_message=str(target_message_id),
                target_channel=str(bridged_channel.id),
                target_emoji_id=bridged_emoji_id,
                target_emoji_name=bridged_emoji_name,
            )

        with Session.begin() as session:
            # Let me check whether I've already reacted to bridged messages in some of these channels
            select_reaction_map: SQLSelect[tuple[DBReactionMap]] = SQLSelect(
                DBReactionMap
            ).where(
                DBReactionMap.source_message == source_message_id_str,
                DBReactionMap.source_emoji == emoji_id_str,
            )
            already_bridged_reactions: ScalarResult[DBReactionMap] = await sql_retry(
                lambda: session.scalars(select_reaction_map)
            )
            already_bridged_reaction_channels = {
                int(bridged_reaction.target_channel)
                for bridged_reaction in already_bridged_reactions
            }

            reachable_channel_ids = (
                reachable_channel_ids - already_bridged_reaction_channels
            )
            if len(reachable_channel_ids) == 0:
                # I've already bridged this reaction to all reachable channels
                return

            # First, check whether this message is bridged, in which case I need to find its source
            select_message_map: SQLSelect[tuple[DBMessageMap]] = SQLSelect(
                DBMessageMap
            ).where(
                DBMessageMap.target_message == source_message_id_str,
            )
            source_message_map: DBMessageMap | None = await sql_retry(
                lambda: session.scalars(select_message_map).first()
            )
            if isinstance(source_message_map, DBMessageMap):
                # This message was bridged, so find the original one, react to it, and then find any other bridged messages from it
                try:
                    source_channel = await globals.get_channel_from_id(
                        int(source_message_map.source_channel),
                        ensure_text_or_thread=True,
                    )
                except ChannelTypeError:
                    return

                source_channel_id = source_channel.id
                source_message_id = int(source_message_map.source_message)
                if source_channel_id in reachable_channel_ids:
                    try:
                        async_add_reactions.append(
                            add_reaction_helper(source_channel, source_message_id)
                        )
                    except discord.HTTPException as e:
                        logger.warning(
                            "Ran into a Discord exception while trying to add a reaction across a bridge: %s",
                            e,
                        )
                    except Exception as e:
                        logger.error(
                            "Ran into an unknown error while trying to add a reaction across a bridge: %s",
                            e,
                        )
                        raise
            else:
                # This message is (or might be) the source
                source_message_id = message_id
                source_channel_id = channel_id

            if not bridges.get_outbound_bridges(source_channel_id):
                if len(async_add_reactions) > 0:
                    reaction_added = await async_add_reactions[0]
                    await sql_retry(lambda: session.add(reaction_added))
                return

            # Bridge reactions to the last bridged message of a group of split bridged messages
            max_message_subq = (
                SQLSelect(
                    DBMessageMap.target_channel,
                    DBMessageMap.source_message,
                    func.max(DBMessageMap.target_message_order).label("max_order"),
                )
                .where(DBMessageMap.source_message == str(source_message_id))
                .group_by(DBMessageMap.target_channel, DBMessageMap.source_message)
                .subquery()
            )
            select_message_map: SQLSelect[tuple[DBMessageMap]] = (
                SQLSelect(DBMessageMap)
                .where(DBMessageMap.source_message == str(source_message_id))
                .join(
                    max_message_subq,
                    sql_and(
                        DBMessageMap.target_channel
                        == max_message_subq.c.target_channel,
                        DBMessageMap.target_message_order
                        == max_message_subq.c.max_order,
                        DBMessageMap.source_message
                        == max_message_subq.c.source_message,
                    ),
                )
            )
            bridged_messages_query_result: ScalarResult[DBMessageMap] = await sql_retry(
                lambda: session.scalars(select_message_map)
            )
            for message_row in bridged_messages_query_result:
                target_channel_id = int(message_row.target_channel)
                if target_channel_id not in reachable_channel_ids:
                    continue

                bridged_channel = await globals.get_channel_from_id(target_channel_id)
                if not isinstance(
                    bridged_channel,
                    (discord.TextChannel, discord.Thread),
                ):
                    continue

                try:
                    async_add_reactions.append(
                        add_reaction_helper(
                            bridged_channel, int(message_row.target_message)
                        )
                    )
                except discord.HTTPException as e:
                    logger.warning(
                        "Ran into a Discord exception while trying to add a reaction across a bridge: %s",
                        e,
                    )
                except Exception as e:
                    logger.error(
                        "Ran into an unknown error while trying to add a reaction across a bridge: %s",
                        e,
                    )
                    return

            reactions_added = await asyncio.gather(*async_add_reactions)
            await sql_retry(lambda: session.add_all([r for r in reactions_added if r]))
    except Exception as e:
        if isinstance(e, SQLError):
            logger.warning(
                "Ran into an SQL error while trying to add a reaction to a message: %s",
                e,
            )
        else:
            logger.error(
                "Ran into an unknown error while trying to add a reaction to a message: %s",
                e,
            )

        return

    logger.debug("Reaction bridged.")


@globals.client.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    """This function is called when a message has a reaction removed. Unlike `on_reaction_remove()`, this is called regardless of the state of the internal message cache.

    Parameters
    ----------
    payload : :class:`~discord.RawReactionActionEvent`
        The raw event payload data.
    """
    if not await globals.wait_until_ready():
        return

    if not globals.client.user:
        return

    if (client_user_id := globals.client.user.id) == payload.user_id:
        # Don't bridge my own reactions
        return

    if not bridges.get_outbound_bridges(channel_id := payload.channel_id):
        # Only remove reactions across outbound bridges
        return

    try:
        channel = await globals.get_channel_from_id(
            channel_id,
            ensure_text_or_thread=True,
        )
    except ChannelTypeError:
        # This really shouldn't happen
        logger.error(
            "Channel with ID %s is registered to bot's bridged channels but isn't a text channel nor a thread off one.",
            channel_id,
        )
        return

    # I will try to see if this emoji still has other reactions in it and, if so, stop doing this as I don't care anymore
    message_id = payload.message_id
    message = await channel.fetch_message(message_id)

    emoji = payload.emoji
    reactions_with_emoji = {
        reaction
        for reaction in message.reactions
        if reaction.emoji == emoji.name or reaction.emoji == emoji
    }
    for reaction in reactions_with_emoji:
        async for user in reaction.users():
            if user.id != client_user_id:
                # There is at least one user who reacted to this message other than me, so I don't need to do anything
                return

    await bridge_reaction_remove(
        message_id=message_id,
        emoji=emoji,
    )


async def bridge_reaction_remove(
    *,
    message_id: int,
    emoji: discord.PartialEmoji,
):
    """Bridge reaction removal.

    Parameters
    ----------
    message_id : int
        The ID of the message a reaction is being removed from.
    emoji : :class:`~discord.PartialEmoji`
        The emoji being removed.
    """
    logger.debug(
        "Bridging reaction removal of %s from message with ID %s.",
        emoji,
        message_id,
    )

    # If I'm here, there are no remaining reactions of this kind on this message except perhaps for my own
    await unreact(message_id=message_id, emoji_to_remove=emoji)

    logger.debug(
        "Successfully bridged reaction removal of %s from message with ID %s.",
        emoji,
        message_id,
    )


@globals.client.event
async def on_raw_reaction_clear_emoji(payload: discord.RawReactionClearEmojiEvent):
    """This function is called when a message has a specific reaction removed it. Unlike `on_reaction_clear_emoji()`, this is called regardless of the state of the internal message cache.

    Parameters
    ----------
    payload : :class:`~discord.RawReactionClearEmojiEvent`
        The raw event payload data.
    """
    if not await globals.wait_until_ready():
        return

    if not bridges.get_outbound_bridges(payload.channel_id):
        # Only remove reactions across outbound bridges
        return

    await bridge_reaction_clear_emoji(
        message_id=payload.message_id, emoji=payload.emoji
    )


async def bridge_reaction_clear_emoji(
    *,
    message_id: int,
    emoji: discord.PartialEmoji,
):
    """Bridge reaction removal.

    Parameters
    ----------
    message_id : int
        The ID of the message a reaction is being removed from.
    emoji : :class:`~discord.PartialEmoji`
        The emoji being removed.
    """
    logger.debug(
        "Bridging reaction clear of %s from message with ID %s.",
        emoji,
        message_id,
    )

    await unreact(message_id=message_id, emoji_to_remove=emoji)

    logger.debug(
        "Successfully bridged clear removal of %s from message with ID %s.",
        emoji,
        message_id,
    )


@globals.client.event
async def on_raw_reaction_clear(payload: discord.RawReactionClearEvent):
    """Bridge reaction removal, if necessary.

    This function is called when a message has all its reactions removed. Unlike `on_reaction_clear()`, this is called regardless of the state of the internal message cache.

    Parameters
    ----------
    payload : :class:`~discord.RawReactionClearEvent`
        The raw event payload data.
    """
    if not await globals.wait_until_ready():
        return

    if not bridges.get_outbound_bridges(payload.channel_id):
        # Only remove reactions across outbound bridges
        return

    message_id = payload.message_id

    logger.debug("Bridging reaction clear from message with ID %s.", message_id)

    await unreact(message_id=message_id)

    logger.debug(
        "Successfully bridged clear removal from message with ID %s.",
        message_id,
    )


@overload
async def unreact(*, message_id: int, session: SQLSession | None = None):
    """Remove all reactions by the bot from messages bridged from a given message (but not from the message itself).

    Parameters
    ----------
    message_id : int
        The ID of the message a reaction is being removed from.
    """
    ...


@overload
async def unreact(
    *,
    message_id: int,
    emoji_to_remove: discord.PartialEmoji,
    session: SQLSession | None = None,
):
    """Remove all reactions by the bot using a given emoji from messages bridged from a given message (but not from the message itself).

    Parameters
    ----------
    message_id : int
        The ID of the message a reaction is being removed from.
    emoji : :class:`~discord.PartialEmoji`
        The emoji being removed.
    """
    ...


@sql_command
@beartype
async def unreact(
    *,
    message_id: int,
    emoji_to_remove: discord.PartialEmoji | None = None,
    session: SQLSession,
):
    """Remove all reactions by the bot using a given emoji (or all emoji) from messages bridged from a given message (but not from the message itself).

    Parameters
    ----------
    message_id : int
        The ID of the message a reaction is being removed from.
    emoji : :class:`~discord.PartialEmoji` | None, optional
        The emoji being removed. Defaults to None, in which case all of them will be.
    session : :class:`~sqlalchemy.orm.Session` | None, optional
        An SQLAlchemy ORM Session connecting to the database. Defaults to None, in which case a new one will be created.
    """
    if emoji_to_remove:
        removed_emoji_id = (
            str(emoji_to_remove.id) if emoji_to_remove.id else emoji_to_remove.name
        )
        equivalent_emoji_ids = emoji_hash_map.map.get_matches(
            emoji_to_remove,
            return_str=True,
        )
    else:
        removed_emoji_id = None
        equivalent_emoji_ids = None

    try:
        # First I find all of the messages that got this reaction bridged to them
        conditions = [DBReactionMap.source_message == str(message_id)]
        if removed_emoji_id:
            conditions.append(DBReactionMap.source_emoji == removed_emoji_id)

        select_bridged_reactions: SQLSelect[tuple[DBReactionMap]] = SQLSelect(
            DBReactionMap
        ).where(*conditions)
        bridged_reactions: ScalarResult[DBReactionMap] = await sql_retry(
            lambda: session.scalars(select_bridged_reactions)
        )
        bridged_messages = {
            (
                map.target_message,
                map.target_channel,
                map.target_emoji_id,
                map.target_emoji_name,
                (
                    equivalent_emoji_ids
                    or emoji_hash_map.map.get_matches(
                        map.source_emoji,
                        return_str=True,
                    )
                ),
            )
            for map in bridged_reactions
        }

        if len(bridged_messages) == 0:
            return

        # Then I remove them from the database
        await sql_retry(
            lambda: session.execute(SQLDelete(DBReactionMap).where(*conditions))
        )

        # Next I find the messages that still have reactions of this type in them even after I removed the ones above
        conditions = [
            DBReactionMap.target_message.in_(
                [message_id for message_id, _, _, _, _ in bridged_messages]
            )
        ]
        if equivalent_emoji_ids:
            conditions.append(DBReactionMap.source_emoji.in_(equivalent_emoji_ids))
        select_bridged_reactions: SQLSelect[tuple[DBReactionMap]] = SQLSelect(
            DBReactionMap
        ).where(*conditions)
        remaining_reactions: ScalarResult[DBReactionMap] = await sql_retry(
            lambda: session.scalars(select_bridged_reactions)
        )

        # And I get rid of my reactions from the messages that aren't on that list
        messages_to_remove_reaction_from = bridged_messages - {
            (
                map.target_message,
                map.target_channel,
                map.target_emoji_id,
                map.target_emoji_name,
                (
                    equivalent_emoji_ids
                    or emoji_hash_map.map.get_matches(
                        map.source_emoji,
                        return_str=True,
                    )
                ),
            )
            for map in remaining_reactions
        }

        if len(messages_to_remove_reaction_from) == 0:
            # I don't have to remove my reaction from any bridged messages
            return

        # There is at least one reaction in one target message that should no longer be there
        def get_emoji_or_name(
            target_emoji_id: str | None,
            target_emoji_name: str | None,
        ):
            if target_emoji_name:
                if target_emoji_id:
                    return f"{target_emoji_name}:{target_emoji_id}"
                else:
                    return target_emoji_name
            elif target_emoji_id:
                try:
                    return globals.client.get_emoji(int(target_emoji_id))
                except ValueError:
                    return None
            else:
                return None

        async def remove_reactions_with_emoji(
            target_channel_id: str,
            target_message_id: str,
            target_emoji_id: str | None,
            target_emoji_name: str | None,
        ):
            target_channel = await globals.get_channel_from_id(int(target_channel_id))
            if not isinstance(
                target_channel,
                (discord.TextChannel, discord.Thread),
            ):
                return

            target_message = await target_channel.fetch_message(int(target_message_id))

            emoji_to_remove = get_emoji_or_name(target_emoji_id, target_emoji_name)
            if emoji_to_remove:
                try:
                    await target_message.remove_reaction(
                        emoji_to_remove, target_channel.guild.me
                    )
                except Exception:
                    pass

        compacted_messages_to_remove_reaction_from = {
            (
                target_message_id,
                target_channel_id,
                target_emoji_id,
                target_emoji_name,
            )
            for target_message_id, target_channel_id, target_emoji_id, target_emoji_name, _ in messages_to_remove_reaction_from
        }
        await asyncio.gather(
            *[
                remove_reactions_with_emoji(
                    target_channel_id,
                    target_message_id,
                    target_emoji_id,
                    target_emoji_name,
                )
                for target_message_id, target_channel_id, target_emoji_id, target_emoji_name in compacted_messages_to_remove_reaction_from
            ]
        )
    except Exception as e:
        if isinstance(e, SQLError):
            logger.warning(
                "Ran into an SQL error while running %s(): %s", inspect.stack()[1][3], e
            )
        else:
            logger.error(
                "Ran into an unknown error while running %s(): %s",
                inspect.stack()[1][3],
                e,
            )

        raise


@globals.client.event
async def on_thread_create(thread: discord.Thread):
    """This function is called whenever a thread is created.

    Parameters
    ----------
    thread : :class:`~discord.Thread`
        The thread that was created.
    """
    # Bridge a thread from a channel that has auto_bridge_threads enabled
    if not isinstance(thread.parent, discord.TextChannel):
        return

    try:
        await thread.join()
    except Exception as e:
        logger.error("An error occurred while trying to join a thread: %s", e)
        raise

    if not await globals.wait_until_ready():
        return

    parent_channel = thread.parent
    if parent_channel.id not in globals.auto_bridge_thread_channels:
        return

    assert globals.client.user
    if thread.owner_id and thread.owner_id == globals.client.user.id:
        return

    if not thread.permissions_for(thread.guild.me).manage_webhooks:
        return

    await auto_bridge_thread(thread)


async def auto_bridge_thread(thread: discord.Thread):
    """Create matching threads across a bridge if the created thread's parent channel has auto-bridge-threads enabled.

    Parameters
    ----------
    thread : :class:`~discord.Thread`
        The thread that was created.
    """
    logger.debug("Automatically bridging thread with ID %s.", thread.id)

    try:
        await commands.bridge_thread_helper(thread, thread.owner_id)
    except Exception as e:
        logger.error("An error occurred while trying to bridge a thread: %s", e)
        raise

    # The message that was used to create the thread will need to be bridged, as the bridge didn't exist at the time
    last_message = thread.last_message
    if not last_message or last_message.content == "":
        refreshed_thread = await globals.get_channel_from_id(thread.id)
        if isinstance(refreshed_thread, discord.Thread):
            last_message = refreshed_thread.last_message
    if last_message and last_message.content != "":
        await bridge_message_helper(last_message)

    logger.debug("Thread with ID %s successfully bridged.", thread.id)


@globals.client.event
async def on_guild_join(server: discord.Guild):
    joined_server_msg = f"Just joined server '{server.name}'."
    logger.info(f"{joined_server_msg} Hashing emoji...")
    print(joined_server_msg)
    try:
        await emoji_hash_map.map.load_server_emoji(server.id)
        logger.info("Emoji hashed!")
    except Exception as e:
        logger.error(
            "An unknown error occurred when trying to hash emoji on joining a new server: %s",
            e,
        )
        raise


@globals.client.event
async def on_guild_remove(server: discord.Guild):
    left_server_msg = f"Just left server '{server.name}'."
    logger.info(left_server_msg)
    print(left_server_msg)


@globals.client.event
async def on_disconnect():
    """Mark the bot as disconnected."""
    globals.is_connected = False


@globals.client.event
async def on_connect():
    await reconnect()


@globals.client.event
async def on_resumed():
    await reconnect()


async def reconnect():
    """Mark the bot as connected and try to check for new messages that haven't been bridged."""
    if (not globals.is_ready) or globals.is_connected:
        return

    globals.is_connected = True

    # Find all channels that have outbound bridges.
    if len(bridges.get_channels_with_outbound_bridges()) == 0:
        return

    await bridge_unbridged_messages()


async def bridge_unbridged_messages():
    with Session() as session:
        # Find the latest bridged messages from each channel
        subquery = (
            session.query(
                DBMessageMap.source_channel,
                func.max(DBMessageMap.id).label("max_id"),
            )
            .group_by(DBMessageMap.source_channel)
            .subquery()
        )
        select_latest_bridged_messages: SQLSelect[tuple[DBMessageMap]] = SQLSelect(
            DBMessageMap
        ).join(
            subquery,
            (DBMessageMap.source_channel == subquery.c.source_channel)
            & (DBMessageMap.id == subquery.c.max_id),
        )

        bridged_messages_query_result: ScalarResult[DBMessageMap] = session.scalars(
            select_latest_bridged_messages
        )

        for bridged_message_row in bridged_messages_query_result:
            # Check whether the ID of the latest bridged message for a channel is also the ID of that channel's latest message
            channel_id = int(bridged_message_row.source_channel)
            message_id = int(bridged_message_row.source_message)

            try:
                channel = await globals.get_channel_from_id(
                    channel_id,
                    ensure_text_or_thread=True,
                )
            except ChannelTypeError:
                continue

            if channel.last_message_id and channel.last_message_id == message_id:
                # Most recent message in the channel has been bridged
                continue

            # There might be unbridged messages, try to find them
            try:
                latest_bridged_message = channel.get_partial_message(message_id)

                messages_after_latest_bridged = [
                    message
                    async for message in channel.history(
                        after=latest_bridged_message.created_at,
                        oldest_first=True,
                    )
                ]
            except discord.Forbidden as e:
                logger.warning(
                    "An error occurred when attempting to fetch unbridged messages after a disconnection:\n%s",
                    e,
                )
                continue

            # Try to bridge them
            for message_to_bridge in messages_after_latest_bridged:
                if message_to_bridge.id == message_id:
                    continue

                try:
                    await on_message(message_to_bridge)
                except Exception:
                    break


app_token = globals.settings.get("app_token")
assert isinstance(app_token, str)
logger.info("Connecting client...")
globals.client.run(app_token, reconnect=True)
