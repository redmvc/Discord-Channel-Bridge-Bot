from __future__ import annotations

import asyncio
import inspect
import re
from typing import Any, Coroutine, TypedDict, cast

import discord
from beartype import beartype
from sqlalchemy import Delete as SQLDelete
from sqlalchemy import ScalarResult
from sqlalchemy import Select as SQLSelect
from sqlalchemy import or_ as sql_or
from sqlalchemy.exc import StatementError as SQLError
from sqlalchemy.orm import Session as SQLSession

import commands
import emoji_hash_map
import globals
from bridge import Bridge, bridges
from database import (
    DBAppWhitelist,
    DBAutoBridgeThreadChannels,
    DBMessageMap,
    DBReactionMap,
    engine,
    sql_retry,
)
from validations import logger


class ThreadSplat(TypedDict, total=False):
    """Helper class to perform bridge operations on threads."""

    thread: discord.Thread


@globals.client.event
async def on_ready():
    """Load the data registered in the database into memory.

    This function is called when the client is done preparing the data received from Discord. Usually after login is successful and the Client.guilds and co. are filled up.

    #### Raises:
        - `ChannelTypeError`: The source or target channels of some existing Bridge are not text channels nor threads off a text channel.
        - `WebhookChannelError`: Webhook of some existing Bridge is not attached to Bridge's target channel.
        - `HTTPException`: Deleting an existing webhook or creating a new one failed.
        - `Forbidden`: You do not have permissions to create or delete webhooks for some of the channels in existing Bridges.
    """
    if globals.is_ready:
        return

    logger.info("Client successfully connected. Running initial loading procedures...")

    session = None
    try:
        with SQLSession(engine) as session:
            await bridges.load_from_database(session)

            # Try to identify hashed emoji
            emoji_hash_map.map = emoji_hash_map.EmojiHashMap(session)

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
            session.commit()

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
        if session:
            session.rollback()
            session.close()

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
            await emoji_hash_map.map.load_forwarded_message_emoji()

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

    globals.is_ready = True
    logger.info("Bot is ready.")


@globals.client.event
async def on_typing(
    channel: discord.abc.Messageable, user: discord.User | discord.Member, _
):
    """Make the bot start typing across bridges when a user on the source end of a bridge does so.

    #### Args:
        - `channel`: The a user is typing in.
        - `user`: The user that is typing in the channel.
    """
    if not (
        globals.is_ready
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
    """Mirror a message across bridges, if possible.

    This function is called when a Message is created and sent. Requires Intents.messages to be enabled.

    #### Raises:
        - `HTTPException`: Sending a message failed.
        - `NotFound`: One of the webhooks was not found.
        - `Forbidden`: The authorization token for one of the webhooks is incorrect.
        - `ValueError`: The length of embeds was invalid, there was no token associated with one of the webhooks or ephemeral was passed with the improper webhook type or there was no state attached with one of the webhooks when giving it a view.
    """
    if not isinstance(message.channel, (discord.TextChannel, discord.Thread)):
        return

    if message.type not in {discord.MessageType.default, discord.MessageType.reply}:
        # Only bridge contentful messages
        return

    if message.application_id and (
        message.application_id == globals.client.application_id
        or (
            (
                not (
                    local_whitelist := globals.per_channel_whitelist.get(
                        message.channel.id
                    )
                )
                or message.application_id not in local_whitelist
            )
            and (
                not (global_whitelist := globals.settings.get("whitelisted_apps"))
                or message.application_id
                not in [int(app_id) for app_id in global_whitelist]
            )
        )
    ):
        # Don't bridge messages from non-whitelisted applications or from self
        return

    if not await globals.wait_until_ready():
        return

    await bridge_message_helper(message)


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
async def bridge_message_helper(message: discord.Message):
    """Mirror a message to any of its outbound bridge targets.

    #### Args:
        - `message`: The message to bridge.

    #### Raises:
        - `HTTPException`: Sending a message failed.
        - `NotFound`: One of the webhooks was not found.
        - `Forbidden`: The authorization token for one of the webhooks is incorrect.
        - `ValueError`: The length of embeds was invalid, there was no token associated with one of the webhooks or ephemeral was passed with the improper webhook type or there was no state attached with one of the webhooks when giving it a view.
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

    session = None
    try:
        with SQLSession(engine) as session:
            if (
                not message.message_snapshots
                or len(message.message_snapshots) == 0
                or not message.reference
            ):
                # Regular message with content (probably)
                logger.debug(
                    "Message with ID %s doesn't have snapshots, is probably not forwarded.",
                    message.id,
                )

                is_forwarded = False
                message_content = await replace_missing_emoji(
                    non_forwarded_message_start(message.content)
                )
                message_attachments = message.attachments
                message_embeds = message.embeds
            else:
                # There is a message snapshot, so this message was forwarded
                logger.debug(
                    "Message with ID %s has snapshots, is forwarded.", message.id
                )

                is_forwarded = True
                snapshot = message.message_snapshots[0]
                message_content = ""
                message_attachments = snapshot.attachments

                # If the original message being forwarded was created by me and it was, itself,
                # a bridge of another forwarded message, I need to treat it differently
                forwarded_message_id = message.reference.message_id
                select_forwarded_message: SQLSelect[tuple[DBMessageMap]] = SQLSelect(
                    DBMessageMap
                ).where(
                    DBMessageMap.target_message == str(forwarded_message_id),
                    DBMessageMap.forwarded,
                )
                query_result: ScalarResult[DBMessageMap] = await sql_retry(
                    lambda: session.scalars(select_forwarded_message)
                )
                if not query_result.first():
                    # The forwarded message was not a bridged forwarded message
                    # so I'll just add an embed with its contents
                    forwarded_message_header = (
                        "-# "
                        + (
                            f"<:forwarded_message:{emoji_hash_map.map.forward_message_emoji_id}> "
                            if emoji_hash_map.map.forward_message_emoji_id
                            else ""
                        )
                        + "***Forwarded***"
                    )

                    message_embeds = [
                        discord.Embed(
                            colour=discord.Colour.from_str("#414348"),
                            description=f"{forwarded_message_header}\n{snapshot.content}",
                        )
                    ] + snapshot.embeds
                else:
                    # The forwarded message was originally bridged
                    # That means that its only content is the embeds, which were originally generated
                    # by the code above
                    message_embeds = snapshot.embeds

            message_is_reply = not not (
                not is_forwarded and message.reference and message.reference.message_id
            )
            bridged_reply_to: dict[int, int] = {}
            replied_author = None
            replied_content = None
            reply_has_ping = False
            if message_is_reply:
                # This message is a reply to another message, so we should try to link to its match on the other side of bridges
                # bridged_reply_to will be a dict whose keys are channel IDs and whose values are the IDs of messages matching the
                # message I'm replying to in those channels
                replied_to_message = message.reference.resolved
                replied_content = await replace_missing_emoji(
                    truncate(
                        discord.utils.remove_markdown(replied_to_message.clean_content),
                        50,
                    )
                )
                replied_author = replied_to_message.author

                # identify if this reply "pinged" the target, to know whether to add the @ symbol UI
                reply_has_ping = isinstance(
                    replied_to_message, discord.Message
                ) and any(
                    x.id == replied_to_message.author.id for x in message.mentions
                )

                # First, check whether the message replied to was itself bridged from a different channel
                replied_to_id = message.reference.message_id
                select_message_map: SQLSelect[tuple[DBMessageMap]] = SQLSelect(
                    DBMessageMap
                ).where(DBMessageMap.target_message == str(replied_to_id))
                local_replied_to_message_map: DBMessageMap | None = await sql_retry(
                    lambda: session.scalars(select_message_map).first()
                )
                if isinstance(local_replied_to_message_map, DBMessageMap):
                    # So the message replied to was bridged from elsewhere
                    source_replied_to_id = int(
                        local_replied_to_message_map.source_message
                    )
                    reply_source_channel_id = int(
                        local_replied_to_message_map.source_channel
                    )
                    bridged_reply_to[reply_source_channel_id] = source_replied_to_id
                else:
                    source_replied_to_id = replied_to_id

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

            # Send a message out to each target webhook
            async_bridged_messages: list[
                Coroutine[Any, Any, discord.WebhookMessage | None]
            ] = []
            for target_id, webhook in reachable_channels.items():
                if not webhook:
                    continue

                webhook_channel = webhook.channel
                if not isinstance(webhook_channel, discord.TextChannel):
                    continue

                target_channel = await globals.get_channel_from_id(target_id)
                assert isinstance(target_channel, discord.TextChannel | discord.Thread)

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
                        message_embeds,
                        target_channel,
                        webhook,
                        webhook_channel,
                        message_is_reply,
                        replied_author,
                        replied_content,
                        bridged_reply_to.get(target_id),
                        reply_has_ping,
                        thread_splat,
                        session,
                    )
                )

            if len(async_bridged_messages) == 0:
                return

            # Insert references to the linked messages into the message_mappings table
            bridged_messages: list[discord.WebhookMessage] = [
                bridged_message
                for bridged_message in (await asyncio.gather(*async_bridged_messages))
                if bridged_message
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
                            target_channel=bridged_message.channel.id,
                            forwarded=is_forwarded,
                            webhook=bridged_message.webhook_id,
                        )
                        for bridged_message in bridged_messages
                    ]
                )
            )
            session.commit()
    except Exception as e:
        if session:
            session.rollback()
            session.close()

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


@beartype
async def bridge_message_to_target_channel(
    message: discord.Message,
    message_content: str,
    message_attachments: list[discord.Attachment],
    message_embeds: list[discord.Embed],
    target_channel: discord.TextChannel | discord.Thread,
    webhook: discord.Webhook,
    webhook_channel: discord.TextChannel,
    message_is_reply: bool,
    replied_author: discord.User | discord.Member | None,
    replied_content: str | None,
    bridged_reply_to: int | None,
    reply_has_ping: bool,
    thread_splat: ThreadSplat,
    session: SQLSession,
) -> discord.WebhookMessage | None:
    """Bridge a message to a channel and returns the message bridged.

    #### Args:
        - `message`: The message being bridged.
        - `message_content`: Its contents.
        - `message_attachments`: Its attachments.
        - `message_embneds`: Its embeds.
        - `target_channel`: The channel the message is being bridged to.
        - `webhook`: The webhook that will send the message.
        - `webhook_channel`: The parent channel the webhook is attached to.
        - `message_is_reply`: Whether the message being bridged is replying to another message.
        - `replied_author`: The author of the message the message being bridged is replying to.
        - `replied_content`: The content of the message the message being bridged is replying to.
        - `bridged_reply_to`: The ID of a message the message being bridged is replying to on the target channel.
        - `reply_has_ping`: Whether the reply is pinging the original message.
        - `thread_splat`: A splat with the thread this message is being bridged to, if any.
        - `session`: A connection to the database.

    #### Returns:
        - `discord.WebhookMessage`: The message bridged.
    """
    logger.debug(
        "Bridging message with ID %s to channel with ID %s.",
        message.id,
        target_channel.id,
    )

    # Try to find whether the user who sent this message is on the other side of the bridge and if so what their name and avatar would be
    bridged_member = await globals.get_channel_member(
        webhook_channel, message.author.id
    )
    if bridged_member:
        bridged_member_name = bridged_member.display_name
        bridged_avatar_url = bridged_member.display_avatar
    else:
        bridged_member_name = message.author.display_name
        bridged_avatar_url = message.author.display_avatar

    if message_is_reply:
        # This message is a reply to another message
        def create_reply_embed_dict(
            replied_to_author_avatar: discord.Asset,
            replied_to_author_name: str,
            replied_content: str,
            *,
            jump_url: str | None = None,
            error_msg: str | None = None,
        ):
            reply_embed_dict = {
                "type": "rich",
                "url": jump_url,
                "thumbnail": {
                    "url": replied_to_author_avatar.replace(size=16).url,
                    "height": 18,
                    "width": 18,
                },
                "description": f"**[↪]({jump_url}) {replied_to_author_name}**  {replied_content}",
            }

            if jump_url:
                reply_embed_dict["url"] = jump_url
                reply_embed_dict["description"] = (
                    f"**[↪]({jump_url}) {replied_to_author_name}**  {replied_content}"
                )
            elif error_msg:
                reply_embed_dict["description"] = (
                    f"**↪ {replied_to_author_name}**  {replied_content}\n\n#- {error_msg}"
                )

            return reply_embed_dict

        if bridged_reply_to:
            # The message being replied to is also bridged to this channel, so I'll create an embed to represent this
            try:
                message_replied_to = await target_channel.fetch_message(
                    bridged_reply_to
                )

                # Use the author's display name if they're in this server
                display_name = discord.utils.escape_markdown(
                    message_replied_to.author.display_name
                )
                # Discord represents ping "ON" vs "OFF" replies with an @ symbol before the reply author name
                # copy this behavior here
                if reply_has_ping:
                    display_name = "@" + display_name

                if not replied_content:
                    replied_content = await replace_missing_emoji(
                        truncate(
                            discord.utils.remove_markdown(
                                message_replied_to.clean_content
                            ),
                            50,
                        )
                    )
                reply_embed_dict = create_reply_embed_dict(
                    message_replied_to.author.display_avatar,
                    display_name,
                    replied_content,
                    jump_url=message_replied_to.jump_url,
                )
                reply_embed = [discord.Embed.from_dict(reply_embed_dict)]
            except discord.HTTPException:
                if replied_content and replied_author:
                    replied_author_name = discord.utils.escape_markdown(
                        replied_author.name
                    )
                    if reply_has_ping:
                        replied_author_name = "@" + replied_author_name

                    reply_embed = [
                        discord.Embed.from_dict(
                            create_reply_embed_dict(
                                replied_author.display_avatar,
                                replied_author_name,
                                replied_content,
                                error_msg="The message being replied to could not be loaded.",
                            )
                        )
                    ]
                else:
                    reply_embed = [
                        discord.Embed.from_dict(
                            {
                                "type": "rich",
                                "description": f"-# **↪** This message is a reply but the message being replied to could not be loaded.",
                            }
                        )
                    ]
        else:
            if replied_content and replied_author:
                replied_author_name = discord.utils.escape_markdown(replied_author.name)
                if reply_has_ping:
                    replied_author_name = "@" + replied_author_name

                reply_embed = [
                    discord.Embed.from_dict(
                        create_reply_embed_dict(
                            replied_author.display_avatar,
                            replied_author_name,
                            replied_content,
                            error_msg="The message being replied has not been bridged.",
                        )
                    )
                ]
            else:
                reply_embed = [
                    discord.Embed.from_dict(
                        {
                            "type": "rich",
                            "description": f"-# **↪** This message is a reply but the message being replied to could not be loaded.",
                        }
                    )
                ]
    else:
        reply_embed = []

    attachments = await asyncio.gather(
        *[attachment.to_file() for attachment in message_attachments]
    )

    try:
        return await webhook.send(
            content=message_content,
            allowed_mentions=discord.AllowedMentions(
                users=True, roles=False, everyone=False
            ),
            avatar_url=bridged_avatar_url,
            username=bridged_member_name,
            embeds=list(message_embeds + reply_embed),
            files=attachments,  # TODO might throw HHTPException if too large?
            wait=True,
            **thread_splat,
        )
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
                target_channel=target_channel, session=session
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
    """Edit bridged versions of a message, if possible.

    This function is called when a message is edited. Unlike `on_message_edit()`, this is called regardless of the state of the internal message cache.

    #### Args:
        - `payload`: The raw event payload data.

    #### Raises:
        - `HTTPException`: Editing a message failed.
        - `Forbidden`: Tried to edit a message that is not yours.
        - `ValueError`: The length of embeds was invalid, there was no token associated with a webhook or a webhook had no state.
    """
    updated_message_content = payload.data.get("content")
    if not updated_message_content or updated_message_content == "":
        # Not a content edit
        return

    if not await globals.wait_until_ready():
        return

    if not bridges.get_outbound_bridges(payload.channel_id):
        return

    logger.debug("Bridging edit to message with ID %s.", payload.message_id)

    # Ensure that the message has emoji I have access to
    updated_message_content = await replace_missing_emoji(
        non_forwarded_message_start(updated_message_content)
    )

    # Get all channels reachable from this one via an unbroken sequence of outbound bridges as well as their webhooks
    reachable_channels = await bridges.get_reachable_channels(
        payload.channel_id,
        "outbound",
        include_webhooks=True,
    )

    # Find all messages matching this one
    try:
        async_message_edits: list[Coroutine[Any, Any, None]] = []
        with SQLSession(engine) as session:
            select_message_map: SQLSelect[tuple[DBMessageMap]] = SQLSelect(
                DBMessageMap
            ).where(DBMessageMap.source_message == payload.message_id)
            bridged_messages: ScalarResult[DBMessageMap] = await sql_retry(
                lambda: session.scalars(select_message_map)
            )

            for message_row in bridged_messages:
                target_channel_id = int(message_row.target_channel)
                if target_channel_id not in reachable_channels:
                    continue

                bridged_channel = await globals.get_channel_from_id(target_channel_id)
                if not isinstance(
                    bridged_channel, (discord.TextChannel, discord.Thread)
                ):
                    continue

                thread_splat: ThreadSplat = {}
                if isinstance(bridged_channel, discord.Thread):
                    if not isinstance(bridged_channel.parent, discord.TextChannel):
                        continue
                    thread_splat = {"thread": bridged_channel}

                try:

                    async def edit_message(
                        message_row: DBMessageMap,
                        target_channel_id: int,
                        thread_splat: ThreadSplat,
                    ):
                        if not message_row.webhook:
                            return

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
                            await webhook.edit_message(
                                message_id=int(message_row.target_message),
                                content=updated_message_content,
                                **thread_splat,
                            )
                        except discord.NotFound:
                            # Webhook is gone, delete this bridge
                            assert isinstance(
                                bridged_channel, (discord.TextChannel, discord.Thread)
                            )
                            logger.warning(
                                "Webhook in %s:%s (ID: %s) not found, demolishing bridges to this channel and its threads.",
                                bridged_channel.guild.name,
                                bridged_channel.name,
                                bridged_channel.id,
                            )
                            try:
                                await bridges.demolish_bridges(
                                    target_channel=bridged_channel, session=session
                                )
                            except Exception as e:
                                logger.error(
                                    "Exception occurred when trying to demolish an invalid bridge after bridging a message edit: %s",
                                    e,
                                )

                    async_message_edits.append(
                        edit_message(
                            message_row,
                            target_channel_id,
                            thread_splat,
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

    logger.debug("Successfully bridged edit to message with ID %s.", payload.message_id)


@beartype
async def replace_missing_emoji(message_content: str) -> str:
    """Return a version of the contents of a message that replaces any instances of an emoji that the bot can't find with matching ones, if possible.

    #### Args:
        - `message_content`: The content of the message to process.

    #### Raises
        - `HTTPResponseError`: HTTP request to fetch image returned a status other than 200.
        - `InvalidURL`: URL generated from emoji was not valid.
        - `RuntimeError`: Session connection failed.
        - `ServerTimeoutError`: Connection to server timed out.
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
                emoji_id=emoji_id, emoji_name=emoji_name
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
                emoji_to_copy_id=emoji_id_str, emoji_to_copy_name=emoji_name
            )
            if emoji:
                emoji_to_replace[f"<{emoji_name}:{emoji_id_str}>"] = str(emoji)
        except Exception:
            pass

    for missing_emoji_str, new_emoji_str in emoji_to_replace.items():
        message_content = message_content.replace(missing_emoji_str, new_emoji_str)
    return message_content


@beartype
def non_forwarded_message_start(message_content: str):
    """Check whether the start of a message has the forwarded message header and, if it does, add a disclaimer at the top clarifying it is not forwarded. Then, return it.

    #### Args:
        - `message_content`: The message content to validate.
    """
    if (
        message_content.startswith("-# ")
        and (first_line := message_content.split("\n")[0])
        and first_line.endswith(" ***Forwarded***")
        and (first_line_split := first_line.split())
        and (
            len(first_line_split) == 2
            or (
                len(first_line_split) == 3
                and re.match(r"<(a?:[^:]+):(\d+)>", first_line_split[1])
            )
        )
    ):
        return (
            "-# (This message was not actually forwarded; the header was added by the user who wrote it.)\n"
            + message_content
        )

    return message_content


@globals.client.event
async def on_raw_message_delete(payload: discord.RawMessageDeleteEvent):
    """Delete bridged versions of a message, if possible.

    This function is called when a message is deleted. Unlike `on_message_delete()`, this is called regardless of the message being in the internal message cache or not.

    #### Args:
        - `payload`: The raw event payload data.

    #### Raises:
        - `HTTPException`: Deleting a message failed.
        - `Forbidden`: Tried to delete a message that is not yours.
        - `ValueError`: A webhook does not have a token associated with it.
    """
    if not await globals.wait_until_ready():
        return

    if not bridges.get_outbound_bridges(payload.channel_id):
        return

    logger.debug("Bridging deletion of message with ID %s.", payload.message_id)

    # Get all channels reachable from this one via an unbroken sequence of outbound bridges as well as their webhooks
    reachable_channels = await bridges.get_reachable_channels(
        payload.channel_id,
        "outbound",
        include_webhooks=True,
    )

    # Find all messages matching this one
    session = None
    try:
        async_message_deletes: list[Coroutine[Any, Any, None]] = []
        with SQLSession(engine) as session:
            select_message_map: SQLSelect[tuple[DBMessageMap]] = SQLSelect(
                DBMessageMap
            ).where(DBMessageMap.source_message == payload.message_id)
            bridged_messages: ScalarResult[DBMessageMap] = await sql_retry(
                lambda: session.scalars(select_message_map)
            )
            for message_row in bridged_messages:
                target_channel_id = int(message_row.target_channel)
                if target_channel_id not in reachable_channels:
                    continue

                bridged_channel = await globals.get_channel_from_id(target_channel_id)
                if not isinstance(
                    bridged_channel, (discord.TextChannel, discord.Thread)
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
                    ):
                        if not message_row.webhook:
                            return

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
                            assert isinstance(
                                bridged_channel, (discord.TextChannel, discord.Thread)
                            )
                            logger.warning(
                                "Webhook in %s:%s (ID: %s) not found, demolishing bridges to this channel and its threads.",
                                bridged_channel.guild.name,
                                bridged_channel.name,
                                bridged_channel.id,
                            )
                            try:
                                await bridges.demolish_bridges(
                                    target_channel=bridged_channel, session=session
                                )
                            except Exception as e:
                                if not isinstance(e, discord.HTTPException):
                                    logger.error(
                                        "Exception occurred when trying to demolish an invalid bridge after bridging a message deletion: %s",
                                        e,
                                    )
                                raise

                    async_message_deletes.append(
                        delete_message(message_row, target_channel_id, thread_splat)
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
                            DBMessageMap.source_message == str(payload.message_id),
                            DBMessageMap.target_message == str(payload.message_id),
                        )
                    )
                )
            )
            session.commit()
    except Exception as e:
        if session:
            session.rollback()
            session.close()

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

    logger.debug(
        "Successfully bridged deletion of message with ID %s.", payload.message_id
    )


@globals.client.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    """Bridge reactions added to a message, if possible.

    This function is called when a message has a reaction added. Unlike `on_reaction_add()`, this is called regardless of the state of the internal message cache.

    #### Args:
        - `payload`: The raw event payload data.

    #### Raises
        - `HTTPResponseError`: HTTP request to fetch image returned a status other than 200.
        - `InvalidURL`: URL generated from emoji was not valid.
        - `RuntimeError`: Session connection failed.
        - `ServerTimeoutError`: Connection to server timed out.
    """
    if not await globals.wait_until_ready():
        return

    if not globals.client.user or payload.user_id == globals.client.user.id:
        # Don't bridge my own reaction
        return

    if not bridges.get_outbound_bridges(payload.channel_id):
        # Only bridge reactions across outbound bridges
        return

    logger.debug(
        "Bridging reaction add of %s to message with ID %s.",
        payload.emoji,
        payload.message_id,
    )

    # Choose a "fallback emoji" to use in case I don't have access to the one being reacted and the message across the bridge doesn't already have it
    fallback_emoji: discord.Emoji | str | None
    if payload.emoji.is_custom_emoji():
        # Custom emoji, I need to check whether it exists and is available to me
        # I'll add this to my hash map if it's not there already
        try:
            await emoji_hash_map.map.ensure_hash_map(emoji=payload.emoji)
        except Exception as e:
            logger.error(
                "An error occurred when calling ensure_hash_map() from on_raw_reaction_add(): %s",
                e,
            )
            raise

        # is_custom_emoji() guarantees that payload.emoji.id is not None
        emoji_id = cast(int, payload.emoji.id)
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
                    emoji_to_copy=payload.emoji
                )
            except Exception:
                fallback_emoji = None
    else:
        # It's a standard emoji, it's fine
        fallback_emoji = payload.emoji.name
        emoji_id_str = fallback_emoji

    # Get the IDs of all emoji that match the current one
    equivalent_emoji_ids = emoji_hash_map.map.get_matches(
        payload.emoji, return_str=True
    )
    if not equivalent_emoji_ids:
        equivalent_emoji_ids = frozenset(str(payload.emoji.id))

    # Now find the list of channels that can validly be reached via outbound chains from this channel
    reachable_channel_ids = await bridges.get_reachable_channels(
        payload.channel_id, "outbound"
    )

    # Find and react to all messages matching this one
    session = None
    try:
        # Create a function to add reactions to messages asynchronously and gather them all at the end
        source_message_id_str = str(payload.message_id)
        source_channel_id_str = str(payload.channel_id)
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

        with SQLSession(engine) as session:
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
                source_channel = await globals.get_channel_from_id(
                    int(source_message_map.source_channel)
                )
                if not source_channel:
                    # The source channel isn't valid or reachable anymore, so we can't find the other versions of this message
                    return

                assert isinstance(source_channel, (discord.TextChannel, discord.Thread))

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
                source_message_id = payload.message_id
                source_channel_id = payload.channel_id

            if not bridges.get_outbound_bridges(source_channel_id):
                if len(async_add_reactions) > 0:
                    reaction_added = await async_add_reactions[0]
                    await sql_retry(lambda: session.add(reaction_added))
                    session.commit()
                return

            select_message_map: SQLSelect[tuple[DBMessageMap]] = SQLSelect(
                DBMessageMap
            ).where(DBMessageMap.source_message == str(source_message_id))
            bridged_messages_query_result: ScalarResult[DBMessageMap] = await sql_retry(
                lambda: session.scalars(select_message_map)
            )
            for message_row in bridged_messages_query_result:
                target_channel_id = int(message_row.target_channel)
                if target_channel_id not in reachable_channel_ids:
                    continue

                bridged_channel = await globals.get_channel_from_id(target_channel_id)
                if not isinstance(
                    bridged_channel, (discord.TextChannel, discord.Thread)
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
                    raise

        reactions_added = await asyncio.gather(*async_add_reactions)
        await sql_retry(lambda: session.add_all([r for r in reactions_added if r]))
        session.commit()
    except Exception as e:
        if session:
            session.rollback()
            session.close()

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

        raise

    logger.debug("Reaction bridged.")


@globals.client.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    """Bridge reaction removal, if necessary.

    This function is called when a message has a reaction removed. Unlike `on_reaction_remove()`, this is called regardless of the state of the internal message cache.

    #### Args:
        - `payload`: The raw event payload data.
    """
    if not await globals.wait_until_ready():
        return

    if not globals.client.user:
        return
    client_user_id = globals.client.user.id
    if payload.user_id == client_user_id:
        return

    if not bridges.get_outbound_bridges(payload.channel_id):
        # Only remove reactions across outbound bridges
        return

    channel = await globals.get_channel_from_id(payload.channel_id)
    if not isinstance(channel, (discord.TextChannel, discord.Thread)):
        # This really shouldn't happen
        return

    logger.debug(
        "Bridging reaction removal of %s from message with ID %s.",
        payload.emoji,
        payload.message_id,
    )

    # I will try to see if this emoji still has other reactions in it and, if so, stop doing this as I don't care anymore
    message = await channel.fetch_message(payload.message_id)
    reactions_with_emoji = {
        reaction
        for reaction in message.reactions
        if reaction.emoji == payload.emoji.name or reaction.emoji == payload.emoji
    }
    for reaction in reactions_with_emoji:
        async for user in reaction.users():
            if user.id != client_user_id:
                # There is at least one user who reacted to this message other than me, so I don't need to do anything
                return

    # If I'm here, there are no remaining reactions of this kind on this message except perhaps for my own
    await unreact(payload)

    logger.debug(
        "Successfully bridged reaction removal of %s from message with ID %s.",
        payload.emoji,
        payload.message_id,
    )


@globals.client.event
async def on_raw_reaction_clear_emoji(payload: discord.RawReactionClearEmojiEvent):
    """Bridge reaction removal, if necessary.

    This function is called when a message has a specific reaction removed it. Unlike `on_reaction_clear_emoji()`, this is called regardless of the state of the internal message cache.

    #### Args:
        - `payload`: The raw event payload data.
    """
    if not await globals.wait_until_ready():
        return

    if not bridges.get_outbound_bridges(payload.channel_id):
        # Only remove reactions across outbound bridges
        return

    logger.debug(
        "Bridging reaction clear of %s from message with ID %s.",
        payload.emoji,
        payload.message_id,
    )
    await unreact(payload)
    logger.debug(
        "Successfully bridged clear removal of %s from message with ID %s.",
        payload.emoji,
        payload.message_id,
    )


@globals.client.event
async def on_raw_reaction_clear(payload: discord.RawReactionClearEvent):
    """Bridge reaction removal, if necessary.

    This function is called when a message has all its reactions removed. Unlike `on_reaction_clear()`, this is called regardless of the state of the internal message cache.

    #### Args:
        - `payload`: The raw event payload data.
    """
    if not await globals.wait_until_ready():
        return

    if not bridges.get_outbound_bridges(payload.channel_id):
        # Only remove reactions across outbound bridges
        return

    logger.debug("Bridging reaction clear from message with ID %s.", payload.message_id)
    await unreact(payload)
    logger.debug(
        "Successfully bridged clear removal from message with ID %s.",
        payload.message_id,
    )


@beartype
async def unreact(
    payload: (
        discord.RawReactionActionEvent
        | discord.RawReactionClearEmojiEvent
        | discord.RawReactionClearEvent
    ),
):
    """Remove all reactions by the bot using a given emoji (or all emoji) on messages bridged from the current one (but not the current one itself).

    #### Args:
        - `payload`: The argument of the call to `on_raw_reaction_remove()`, `on_raw_reaction_clear_emoji()`, or `on_raw_reaction_clear()`.
    """
    if isinstance(payload, discord.RawReactionClearEvent):
        # Clear all reactions
        emoji_to_remove = None
        removed_emoji_id = None
        equivalent_emoji_ids = None
    else:
        # Remove just one emoji
        emoji_to_remove = payload.emoji
        removed_emoji_id = (
            str(emoji_to_remove.id) if emoji_to_remove.id else emoji_to_remove.name
        )
        equivalent_emoji_ids = emoji_hash_map.map.get_matches(
            emoji_to_remove, return_str=True
        )

    session = None
    try:
        with SQLSession(engine) as session:
            # First I find all of the messages that got this reaction bridged to them
            conditions = [DBReactionMap.source_message == str(payload.message_id)]
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
                    equivalent_emoji_ids
                    or emoji_hash_map.map.get_matches(
                        map.source_emoji, return_str=True
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
                    equivalent_emoji_ids
                    or emoji_hash_map.map.get_matches(
                        map.source_emoji, return_str=True
                    ),
                )
                for map in remaining_reactions
            }

            session.commit()

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
                target_channel = await globals.get_channel_from_id(
                    int(target_channel_id)
                )
                if not isinstance(
                    target_channel, (discord.TextChannel, discord.Thread)
                ):
                    return

                target_message = await target_channel.fetch_message(
                    int(target_message_id)
                )

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
        if session:
            session.rollback()
            session.close()

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
    """Create matching threads across a bridge if the created thread's parent channel has auto-bridge-threads enabled.

    This function is called whenever a thread is created.

    #### Args:
        - `thread`: The thread that was created.
    """
    # Bridge a thread from a channel that has auto_bridge_threads enabled
    if not isinstance(thread.parent, discord.TextChannel):
        return

    try:
        await thread.join()
    except Exception:
        pass

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
        last_message = cast(discord.Thread, refreshed_thread).last_message
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


app_token = globals.settings.get("app_token")
assert isinstance(app_token, str)
logger.info("Connecting client...")
globals.client.run(app_token, reconnect=True)
