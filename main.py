from __future__ import annotations

import asyncio
import random
from typing import Any, Coroutine, TypedDict, cast
from warnings import warn

import discord
from sqlalchemy import Delete as SQLDelete
from sqlalchemy import ScalarResult
from sqlalchemy import Select as SQLSelect
from sqlalchemy import or_ as sql_or
from sqlalchemy.exc import StatementError as SQLError
from sqlalchemy.orm import Session as SQLSession

import commands
import globals
from bridge import Bridge, bridges
from database import (
    DBAppWhitelist,
    DBAutoBridgeThreadChannels,
    DBBridge,
    DBEmojiMap,
    DBMessageMap,
    DBReactionMap,
    engine,
    sql_retry,
)
from validations import validate_types


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

    with SQLSession(engine) as session:
        # I am going to try to identify all existing bridges
        invalid_channels: set[str] = set()
        invalid_webhooks: set[str] = set()
        create_bridges = []

        select_all_bridges: SQLSelect = SQLSelect(DBBridge)
        bridge_query_result: ScalarResult[DBBridge] = session.scalars(
            select_all_bridges
        )
        for bridge in bridge_query_result:
            source_id_str = bridge.source
            target_id_str = bridge.target
            webhook_id_str = bridge.webhook

            if webhook_id_str in invalid_webhooks:
                continue

            source_id = int(source_id_str)
            source_channel = await globals.get_channel_from_id(int(source_id))
            if not source_channel:
                # If I don't have access to the source channel, delete bridges from and to it
                invalid_channels.add(source_id_str)

            target_id = int(target_id_str)
            target_channel = await globals.get_channel_from_id(int(target_id))
            if not target_channel:
                # If I don't have access to the target channel, delete bridges from and to it
                invalid_channels.add(target_id_str)

            try:
                webhook = await globals.client.fetch_webhook(int(webhook_id_str))

                if not source_channel:
                    # I have access to the target webhook but not to the source channel anymore so I'll delete the webhook
                    await webhook.delete(reason="Source channel no longer available.")
                    raise Exception
                elif target_channel:
                    # I have access to both the source and target channels and to the webhook
                    create_bridges.append(
                        commands.create_bridge(source_id, target_id, webhook)
                    )
            except Exception:
                invalid_webhooks.add(webhook_id_str)

                if source_channel and target_channel:
                    # There *should* be a webhook there and I have access to the channels
                    create_bridges.append(
                        commands.create_bridge_and_db(source_id, target_id, session)
                    )
        await asyncio.gather(*create_bridges)

        if len(invalid_channels) > 0:
            delete_invalid_channels = SQLDelete(DBBridge).where(
                sql_or(
                    DBBridge.source.in_(invalid_channels),
                    DBBridge.target.in_(invalid_channels),
                )
            )
            session.execute(delete_invalid_channels)

            delete_invalid_messages = SQLDelete(DBMessageMap).where(
                sql_or(
                    DBMessageMap.source_channel.in_(invalid_channels),
                    DBMessageMap.target_channel.in_(invalid_channels),
                )
            )
            session.execute(delete_invalid_messages)

        if len(invalid_webhooks) > 0:
            delete_invalid_webhooks = SQLDelete(DBBridge).where(
                DBBridge.webhook.in_(invalid_webhooks)
            )
            session.execute(delete_invalid_webhooks)

        # Try to identify mapped emoji
        select_mapped_emoji: SQLSelect = SQLSelect(DBEmojiMap)
        mapped_emoji_query_result: ScalarResult[DBEmojiMap] = session.scalars(
            select_mapped_emoji
        )
        emoji_not_found = set()
        for emoji_map in mapped_emoji_query_result:
            if emoji_map.internal_emoji in emoji_not_found:
                continue

            internal_emoji_id = int(emoji_map.internal_emoji)
            if not globals.client.get_emoji(internal_emoji_id):
                emoji_not_found.add(emoji_map.internal_emoji)
            else:
                # The emoji is registered
                globals.emoji_mappings[int(emoji_map.external_emoji)] = (
                    internal_emoji_id
                )

        if len(emoji_not_found) > 0:
            delete_missing_internal_emoji = SQLDelete(DBEmojiMap).where(
                DBEmojiMap.internal_emoji.in_(emoji_not_found)
            )
            session.execute(delete_missing_internal_emoji)

        # Try to find all apps whitelisted per channel
        select_whitelisted_apps: SQLSelect = SQLSelect(DBAppWhitelist)
        whitelisted_apps_query_result: ScalarResult[DBAppWhitelist] = session.scalars(
            select_whitelisted_apps
        )
        accessible_channels = set()
        inaccessible_channels = set()
        for whitelisted_app in whitelisted_apps_query_result:
            channel_id = int(whitelisted_app.channel)
            if channel_id in inaccessible_channels:
                continue

            if channel_id not in accessible_channels:
                channel = globals.client.get_channel(channel_id)
                if not channel:
                    try:
                        channel = await globals.client.fetch_channel(channel_id)
                    except Exception:
                        channel = None

                if channel:
                    accessible_channels.add(channel_id)
                else:
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

        session.commit()

        # Identify all automatically-thread-bridging channels
        select_auto_bridge_thread_channels: SQLSelect = SQLSelect(
            DBAutoBridgeThreadChannels
        )
        auto_thread_query_result: ScalarResult[DBAutoBridgeThreadChannels] = (
            session.scalars(select_auto_bridge_thread_channels)
        )
        globals.auto_bridge_thread_channels = globals.auto_bridge_thread_channels.union(
            {
                int(auto_bridge_thread_channel.channel)
                for auto_bridge_thread_channel in auto_thread_query_result
            }
        )

    # Finally I'll check whether I have a registered emoji server and save it if so
    emoji_server_id_str = globals.settings.get("emoji_server_id")
    try:
        if emoji_server_id_str:
            emoji_server_id = int(emoji_server_id_str)
        else:
            emoji_server_id = None
    except Exception:
        print(
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
            print("Couldn't find emoji server with ID registered in settings.json.")
        elif (
            not emoji_server.me.guild_permissions.manage_expressions
            or not emoji_server.me.guild_permissions.create_expressions
        ):
            print(
                "I don't have Create Expressions or Manage Expressions permissions in the emoji server."
            )
        else:
            globals.emoji_server = emoji_server

    sync_command_tree = [globals.command_tree.sync()]
    if globals.emoji_server:
        sync_command_tree.append(globals.command_tree.sync(guild=globals.emoji_server))
    await asyncio.gather(*sync_command_tree)

    print(f"{globals.client.user} is connected to the following servers:\n")
    for server in globals.client.guilds:
        print(f"{server.name}(id: {server.id})")

    globals.is_ready = True


@globals.client.event
async def on_raw_typing(payload: discord.RawTypingEvent):
    """Make the bot start typing across bridges when a user on the source end of a bridge does so.

    This function is called when someone begins typing a message. Unlike on_typing() this is called regardless of the channel and user being in the internal cache. Requires Intents.typing to be enabled.

    #### Args:
        - `payload`: The raw event payload data.
    """
    if not globals.is_ready:
        return

    if not globals.client.user or globals.client.user.id == payload.user_id:
        return

    outbound_bridges = bridges.get_outbound_bridges(payload.channel_id)
    if not outbound_bridges:
        return

    async def type_through_bridge(bridge: Bridge):
        target_channel = await bridge.target_channel
        await target_channel.typing()

    channels_typing: list[Coroutine] = []
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
    validate_types({"message": (message, discord.Message)})

    outbound_bridges = bridges.get_outbound_bridges(message.channel.id)
    if not outbound_bridges:
        return

    # Get all channels reachable from this one via an unbroken sequence of outbound bridges as well as their webhooks
    reachable_channels = bridges.get_reachable_channels(
        message.channel.id,
        "outbound",
        include_webhooks=True,
    )

    session = None
    try:
        with SQLSession(engine) as session:
            bridged_reply_to: dict[int, int] = {}
            reply_has_ping = False
            if message.reference and message.reference.message_id:
                # This message is a reply to another message, so we should try to link to its match on the other side of bridges
                # bridged_reply_to will be a dict whose keys are channel IDs and whose values are the IDs of messages matching the message I'm replying to in those channels
                replied_to_id = message.reference.message_id

                # identify if this reply "pinged" the target, to know whether to add the @ symbol UI
                replied_to_message = message.reference.resolved
                reply_has_ping = isinstance(
                    replied_to_message, discord.Message
                ) and any(
                    x.id == replied_to_message.author.id for x in message.mentions
                )

                # First, check whether the message replied to was itself bridged from a different channel
                def get_local_replied_to():
                    return session.scalars(
                        SQLSelect(DBMessageMap).where(
                            DBMessageMap.target_message == str(replied_to_id)
                        )
                    ).first()

                local_replied_to_message_map: DBMessageMap | None = await sql_retry(
                    get_local_replied_to
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
                    reply_source_channel_id = message.channel.id

                # Now find all other bridged versions of the message we're replying to
                def get_bridged_reply_tos():
                    select_bridged_reply_to: SQLSelect = SQLSelect(DBMessageMap).where(
                        DBMessageMap.source_message == str(source_replied_to_id)
                    )
                    return session.scalars(select_bridged_reply_to)

                query_result: ScalarResult[DBMessageMap] = await sql_retry(
                    get_bridged_reply_tos
                )
                for message_map in query_result:
                    bridged_reply_to[int(message_map.target_channel)] = int(
                        message_map.target_message
                    )

            # Send a message out to each target webhook
            async_bridged_messages = []
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
                        continue
                    thread_splat = {"thread": target_channel}

                # Create an async version of bridging this message to gather at the end
                async def bridge_message(
                    webhook_channel: discord.TextChannel,
                    message: discord.Message,
                    target_id: int,
                    bridged_reply_to: dict[int, int],
                    target_channel: discord.TextChannel | discord.Thread,
                    reply_has_ping: bool,
                    thread_splat: ThreadSplat,
                    webhook: discord.Webhook,
                ):
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

                    if bridged_reply_to.get(target_id):
                        # The message being replied to is also bridged to this channel, so I'll create an embed to represent this
                        try:
                            message_replied_to = await target_channel.fetch_message(
                                bridged_reply_to[target_id]
                            )

                            def truncate(msg: str, length: int) -> str:
                                return (
                                    msg
                                    if len(msg) < length
                                    else msg[: length - 1] + "…"
                                )

                            display_name = discord.utils.escape_markdown(
                                message_replied_to.author.display_name
                            )

                            # Discord represents ping "ON" vs "OFF" replies with an @ symbol before the reply author name
                            # copy this behavior here
                            if reply_has_ping:
                                display_name = "@" + display_name

                            replied_content = truncate(
                                discord.utils.remove_markdown(
                                    message_replied_to.clean_content
                                ),
                                50,
                            )
                            reply_embed = [
                                discord.Embed.from_dict(
                                    {
                                        "type": "rich",
                                        "url": message_replied_to.jump_url,
                                        "thumbnail": {
                                            "url": message_replied_to.author.display_avatar.replace(
                                                size=16
                                            ).url,
                                            "height": 18,
                                            "width": 18,
                                        },
                                        "description": f"**[↪]({message_replied_to.jump_url}) {display_name}**  {replied_content}",
                                    }
                                ),
                            ]
                        except discord.HTTPException:
                            reply_embed = []
                    else:
                        reply_embed = []

                    attachments = []
                    for attachment in message.attachments:
                        attachments.append(await attachment.to_file())

                    return await webhook.send(
                        content=message.content,
                        allowed_mentions=discord.AllowedMentions(
                            users=True, roles=False, everyone=False
                        ),
                        avatar_url=bridged_avatar_url,
                        username=bridged_member_name,
                        embeds=list(message.embeds + reply_embed),
                        files=attachments,  # might throw HHTPException if too large?
                        wait=True,
                        **thread_splat,
                    )

                async_bridged_messages.append(
                    bridge_message(
                        webhook_channel,
                        message,
                        target_id,
                        bridged_reply_to,
                        target_channel,
                        reply_has_ping,
                        thread_splat,
                        webhook,
                    )
                )

            if len(async_bridged_messages) == 0:
                return

            # Insert references to the linked messages into the message_mappings table
            bridged_messages: list[discord.WebhookMessage] = await asyncio.gather(
                *async_bridged_messages
            )
            source_message_id_str = str(message.id)
            source_channel_id_str = str(message.channel.id)

            def insert_into_message_map():
                session.add_all(
                    [
                        DBMessageMap(
                            source_message=source_message_id_str,
                            source_channel=source_channel_id_str,
                            target_message=message.id,
                            target_channel=message.channel.id,
                            webhook=message.webhook_id,
                        )
                        for message in bridged_messages
                    ]
                )

            await sql_retry(insert_into_message_map)
            session.commit()
    except SQLError as e:
        if session:
            session.rollback()
            session.close()

        warn("Ran into an SQL error while trying to bridge a message:\n" + str(e))
        return


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

    # Get all channels reachable from this one via an unbroken sequence of outbound bridges as well as their webhooks
    reachable_channels = bridges.get_reachable_channels(
        payload.channel_id,
        "outbound",
        include_webhooks=True,
    )

    # Find all messages matching this one
    try:
        async_message_edits = []
        with SQLSession(engine) as session:

            def get_bridged_messages():
                return session.scalars(
                    SQLSelect(DBMessageMap).where(
                        DBMessageMap.source_message == payload.message_id
                    )
                )

            bridged_messages: ScalarResult[DBMessageMap] = await sql_retry(
                get_bridged_messages
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

                        await webhook.edit_message(
                            message_id=int(message_row.target_message),
                            content=updated_message_content,
                            **thread_splat,
                        )

                    async_message_edits.append(
                        edit_message(
                            message_row,
                            target_channel_id,
                            thread_splat,
                        )
                    )
                except discord.HTTPException as e:
                    warn(
                        "Ran into a Discord exception while trying to edit a message across a bridge:\n"
                        + str(e)
                    )
    except SQLError as e:
        warn("Ran into an SQL error while trying to edit a message:\n" + str(e))

    await asyncio.gather(*async_message_edits)


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

    # Get all channels reachable from this one via an unbroken sequence of outbound bridges as well as their webhooks
    reachable_channels = bridges.get_reachable_channels(
        payload.channel_id,
        "outbound",
        include_webhooks=True,
    )

    # Find all messages matching this one
    session = None
    try:
        async_message_deletes = []
        with SQLSession(engine) as session:

            def get_bridged_messages():
                return session.scalars(
                    SQLSelect(DBMessageMap).where(
                        DBMessageMap.source_message == payload.message_id
                    )
                )

            bridged_messages: ScalarResult[DBMessageMap] = await sql_retry(
                get_bridged_messages
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

                        await webhook.delete_message(
                            int(message_row.target_message),
                            **thread_splat,
                        )

                    async_message_deletes.append(
                        delete_message(message_row, target_channel_id, thread_splat)
                    )
                except discord.HTTPException as e:
                    warn(
                        "Ran into a Discord exception while trying to delete a message across a bridge:\n"
                        + str(e)
                    )

            # If the message was bridged, delete its row
            # If it was a source of bridged messages, delete all rows of its bridged versions
            def delete_bridged_messages():
                session.execute(
                    SQLDelete(DBMessageMap).where(
                        sql_or(
                            DBMessageMap.source_message == str(payload.message_id),
                            DBMessageMap.target_message == str(payload.message_id),
                        )
                    )
                )

            await sql_retry(delete_bridged_messages)
            session.commit()
    except SQLError as e:
        if session:
            session.rollback()
            session.close()

        warn("Ran into an SQL error while trying to delete a message:\n" + str(e))
        return

    await asyncio.gather(*async_message_deletes)


@globals.client.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    """Bridge reactions added to a message, if possible.

    This function is called when a message has a reaction added. Unlike `on_reaction_add()`, this is called regardless of the state of the internal message cache.

    #### Args:
        - `payload`: The raw event payload data.
    """
    if not await globals.wait_until_ready():
        return

    if not globals.client.user or payload.user_id == globals.client.user.id:
        # Don't bridge my own reaction
        return

    if not bridges.get_outbound_bridges(payload.channel_id):
        # Only bridge reactions across outbound bridges
        return

    # Choose a "fallback emoji" to use in case I don't have access to the one being reacted and the message across the bridge doesn't already have it
    fallback_emoji: discord.Emoji | str | None
    if payload.emoji.is_custom_emoji():
        # Custom emoji, I need to check whether it exists and is available to me
        # is_custom_emoji() guarantees that payload.emoji.id is not None
        emoji_id = cast(int, payload.emoji.id)
        emoji_id_str = str(emoji_id)

        fallback_emoji = globals.client.get_emoji(emoji_id)
        if not fallback_emoji or not fallback_emoji.available:
            fallback_emoji = None
            # Couldn't find the reactji, will try to see if I've got it mapped locally
            mapped_emoji_id = globals.emoji_mappings.get(emoji_id)
            if mapped_emoji_id:
                # I already have this Emoji mapped locally
                fallback_emoji = globals.client.get_emoji(mapped_emoji_id)

        if not fallback_emoji:
            # I don't have the emoji mapped locally, I'll add it to my server and update my map
            try:
                fallback_emoji = await copy_emoji_into_server(payload.emoji)
            except Exception:
                fallback_emoji = None
    else:
        # It's a standard emoji, it's fine
        fallback_emoji = payload.emoji.name
        emoji_id_str = fallback_emoji

    # Get the IDs of all emoji that match the current one
    equivalent_emoji_ids = get_equivalent_emoji_ids(payload.emoji)

    # Now find the list of channels that can validly be reached via outbound chains from this channel
    reachable_channel_ids = bridges.get_reachable_channels(
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
                await bridged_message.add_reaction(existing_matching_emoji)
            elif fallback_emoji:
                await bridged_message.add_reaction(fallback_emoji)
            else:
                return None

            # I'll return a reaction map to insert into the reaction map table
            return DBReactionMap(
                emoji=emoji_id_str,
                source_message=source_message_id_str,
                source_channel=source_channel_id_str,
                target_message=str(target_message_id),
                target_channel=str(bridged_channel.id),
            )

        with SQLSession(engine) as session:
            # Let me check whether I've already reacted to bridged messages in some of these channels
            def get_already_bridged_reactions():
                return session.scalars(
                    SQLSelect(DBReactionMap).where(
                        DBReactionMap.source_message == source_message_id_str,
                        DBReactionMap.emoji == emoji_id_str,
                    )
                )

            already_bridged_reactions: ScalarResult[DBReactionMap] = await sql_retry(
                get_already_bridged_reactions
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
            def get_source_message_map():
                return session.scalars(
                    SQLSelect(DBMessageMap).where(
                        DBMessageMap.target_message == source_message_id_str,
                    )
                ).first()

            source_message_map: DBMessageMap | None = await sql_retry(
                get_source_message_map
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
                        warn(
                            "Ran into a Discord exception while trying to add a reaction across a bridge:\n"
                            + str(e)
                        )
            else:
                # This message is (or might be) the source
                source_message_id = payload.message_id
                source_channel_id = payload.channel_id

            if not bridges.get_outbound_bridges(source_channel_id):
                if len(async_add_reactions) > 0:
                    reaction_added = await async_add_reactions[0]

                    def insert_into_reactions_map():
                        session.add(reaction_added)
                        session.commit()

                    await sql_retry(insert_into_reactions_map)
                return

            def get_bridged_messages():
                return session.scalars(
                    SQLSelect(DBMessageMap).where(
                        DBMessageMap.source_message == str(source_message_id)
                    )
                )

            bridged_messages_query_result: ScalarResult[DBMessageMap] = await sql_retry(
                get_bridged_messages
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
                    warn(
                        "Ran into a Discord exception while trying to add a reaction across a bridge:\n"
                        + str(e)
                    )

        reactions_added = await asyncio.gather(*async_add_reactions)

        def insert_into_reactions_map():
            session.add_all([r for r in reactions_added if r])
            session.commit()

        await sql_retry(insert_into_reactions_map)
    except SQLError as e:
        if session:
            session.rollback()
            session.close()

        warn(
            "Ran into an SQL error while trying to add a reaction to a message:\n"
            + str(e)
        )


async def copy_emoji_into_server(
    missing_emoji: discord.PartialEmoji,
) -> discord.Emoji | None:
    """Try to create an emoji in the emoji server and, if successful, return it.

    #### Args:
        - `missing_emoji`: The emoji we are trying to copy into our emoji server.

    #### Raises:
        - `Forbidden`: Emoji server permissions not set correctly.
        - `HTTPResponseError`: HTTP request to fetch emoji image returned a status other than 200.
        - `InvalidURL`: URL generated from emoji ID was not valid.
        - `RuntimeError`: Session connection to the server to fetch image from URL failed.
        - `ServerTimeoutError`: Connection to server to fetch image from URL timed out.
    """
    if not globals.emoji_server:
        return None

    if missing_emoji.animated:
        ext = "gif"
    else:
        ext = "png"
    image = await globals.get_image_from_URL(
        f"https://cdn.discordapp.com/emojis/{missing_emoji.id}.{ext}?v=1"
    )

    delete_existing_emoji_query = None
    try:
        emoji = await globals.emoji_server.create_custom_emoji(
            name=missing_emoji.name, image=image, reason="Bridging reaction."
        )
    except discord.Forbidden as e:
        print("Emoji server permissions not set correctly.")
        raise e
    except discord.HTTPException as e:
        if len(globals.emoji_server.emojis) == 0:
            # Something weird happened, the error was not due to a full server
            raise e

        # Try to delete an emoji from the server and then add this again.
        emoji_to_delete = random.choice(globals.emoji_server.emojis)
        globals.emoji_mappings = {
            external_id: internal_id
            for external_id, internal_id in globals.emoji_mappings.items()
            if internal_id != emoji_to_delete.id
        }
        delete_existing_emoji_query = SQLDelete(DBEmojiMap).where(
            DBEmojiMap.internal_emoji == str(emoji_to_delete.id)
        )
        await emoji_to_delete.delete()

        try:
            emoji = await globals.emoji_server.create_custom_emoji(
                name=missing_emoji.name, image=image, reason="Bridging reaction."
            )
        except discord.Forbidden as e:
            print("Emoji server permissions not set correctly.")
            raise e

    # Copied the emoji, going to update my table
    try:
        with SQLSession(engine) as session:
            if delete_existing_emoji_query is not None:
                await sql_retry(lambda: session.execute(delete_existing_emoji_query))
            await commands.map_emoji_helper(
                external_emoji=missing_emoji, internal_emoji=emoji, session=session
            )
            session.commit()
    except SQLError as e:
        warn("Couldn't add emoji mapping to table.")
        print(e)

    return emoji


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
    assert isinstance(channel, (discord.TextChannel, discord.Thread))

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

    await unreact(payload)


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

    await unreact(payload)


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
        equivalent_emoji_ids = get_equivalent_emoji_ids(emoji_to_remove)

    try:
        with SQLSession(engine) as session:
            # First I find all of the messages that got this reaction bridged to them
            conditions = [DBReactionMap.source_message == str(payload.message_id)]
            if removed_emoji_id:
                conditions.append(DBReactionMap.emoji == removed_emoji_id)

            bridged_reactions: ScalarResult[DBReactionMap] = await sql_retry(
                lambda: session.scalars(SQLSelect(DBReactionMap).where(*conditions))
            )
            bridged_messages = {
                (
                    map.target_message,
                    map.target_channel,
                    equivalent_emoji_ids or get_equivalent_emoji_ids(map.emoji),
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
                    [message_id for message_id, _, _ in bridged_messages]
                )
            ]
            if equivalent_emoji_ids:
                conditions.append(DBReactionMap.emoji.in_(equivalent_emoji_ids))
            remaining_reactions: ScalarResult[DBReactionMap] = await sql_retry(
                lambda: session.scalars(SQLSelect(DBReactionMap).where(*conditions))
            )

            # And I get rid of my reactions from the messages that aren't on that list
            messages_to_remove_reaction_from = bridged_messages - {
                (
                    map.target_message,
                    map.target_channel,
                    equivalent_emoji_ids or get_equivalent_emoji_ids(map.emoji),
                )
                for map in remaining_reactions
            }

            session.commit()

            if len(messages_to_remove_reaction_from) == 0:
                # I don't have to remove my reaction from any bridged messages
                return

            # There is at least one reaction in one target message that should no longer be there
            def get_emoji_or_name(emoji_id: str):
                try:
                    emoji_or_name: discord.Emoji | str | None = (
                        globals.client.get_emoji(int(emoji_id))
                    )

                    if not emoji_or_name and emoji_to_remove:
                        # I can't find the emoji by ID but it might still be an emoji I can unreact if it happens to have the same name as the original one
                        emoji_or_name = f"{emoji_to_remove.name}:{emoji_id}"
                except ValueError:
                    emoji_or_name = emoji_id

                return emoji_or_name

            async def remove_specific_emoji(
                target_message: discord.Message,
                target_channel_member: discord.Member,
                emoji: discord.Emoji | str,
            ):
                try:
                    await target_message.remove_reaction(emoji, target_channel_member)
                except Exception:
                    pass

            async def remove_reactions_with_emoji(
                target_channel_id: str,
                target_message_id: str,
                emoji_ids: frozenset[str],
            ):
                target_channel = await globals.get_channel_from_id(
                    int(target_channel_id)
                )
                assert isinstance(target_channel, (discord.TextChannel, discord.Thread))
                target_channel_member = target_channel.guild.me

                target_message = await target_channel.fetch_message(
                    int(target_message_id)
                )

                await asyncio.gather(
                    *[
                        remove_specific_emoji(
                            target_message, target_channel_member, emoji
                        )
                        for emoji_id in emoji_ids
                        if (emoji := get_emoji_or_name(emoji_id))
                    ]
                )

            await asyncio.gather(
                *[
                    remove_reactions_with_emoji(
                        target_channel_id, target_message_id, emoji_ids
                    )
                    for target_message_id, target_channel_id, emoji_ids in messages_to_remove_reaction_from
                    if emoji_ids
                ]
            )
    except SQLError as e:
        if session:
            session.rollback()
            session.close()

        warn("Ran into an SQL error while trying to remove a reaction:\n" + str(e))
        return


def get_equivalent_emoji_ids(
    emoji: discord.PartialEmoji | int | str,
) -> frozenset[str]:
    """Return a set with the IDs of all emoji that match the argument (due to being mapped to it in the emoji server).

    #### Args:
        - `emoji`: The emoji to find equivalencies for.
    """
    validate_types({"emoji": (emoji, (discord.PartialEmoji, int, str))})

    if isinstance(emoji, discord.PartialEmoji) and not emoji.is_custom_emoji():
        return frozenset({emoji.name})

    if not isinstance(emoji, discord.PartialEmoji):
        # This should be an emoji ID
        try:
            emoji_id = int(emoji)
        except ValueError:
            # For some reason it's not, I'll just return it stringified then
            # This can only happen if emoji is a string
            return frozenset({cast(str, emoji)})
    else:
        # is_custom_emoji() guarantees that emoji.id is not None
        emoji_id = cast(int, emoji.id)

    # I'm going to go through every emoji in globals.emoji_mappings that can be reached from this emoji_id
    emoji_to_check: set[int] = {emoji_id}
    equivalent_emoji_ids: set[str] = set()
    while len(emoji_to_check) > 0:
        checking_emoji = emoji_to_check.pop()
        if str(checking_emoji) in equivalent_emoji_ids:
            continue

        equivalent_emoji_ids.add(str(checking_emoji))

        emoji_to_check = emoji_to_check.union(
            {
                external_emoji
                for external_emoji, internal_emoji in globals.emoji_mappings.items()
                if internal_emoji == checking_emoji
            }
        )
        if globals.emoji_mappings.get(checking_emoji):
            emoji_to_check.add(globals.emoji_mappings[checking_emoji])

    return frozenset(equivalent_emoji_ids)


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

    await commands.bridge_thread_helper(thread, thread.owner_id)

    # The message that was used to create the thread will need to be bridged, as the bridge didn't exist at the time
    last_message = thread.last_message
    if not last_message or last_message.content == "":
        refreshed_thread = await globals.get_channel_from_id(thread.id)
        last_message = cast(discord.Thread, refreshed_thread).last_message
    if last_message and last_message.content != "":
        await bridge_message_helper(last_message)


globals.client.run(cast(str, globals.settings["app_token"]), reconnect=True)
