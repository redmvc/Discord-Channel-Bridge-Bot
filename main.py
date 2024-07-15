from __future__ import annotations

import asyncio
import random
import re
from typing import Any, Coroutine, TypedDict, cast
from warnings import warn

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
    DBBridge,
    DBMessageMap,
    DBReactionMap,
    DBWebhook,
    engine,
    sql_retry,
)


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

    session = None
    try:
        with SQLSession(engine) as session:
            # I am going to try to identify all existing bridges and webhooks and add them to my tracking
            # and also delete the ones that aren't valid or accessible
            invalid_channel_ids: set[str] = set()
            invalid_webhook_ids: set[str] = set()

            select_all_webhooks: SQLSelect[tuple[DBWebhook]] = SQLSelect(DBWebhook)
            webhook_query_result: ScalarResult[DBWebhook] = session.scalars(
                select_all_webhooks
            )
            add_webhook_async: list[Coroutine[Any, Any, discord.Webhook]] = []
            for channel_webhook in webhook_query_result:
                channel_id = int(channel_webhook.channel)
                webhook_id = int(channel_webhook.webhook)

                channel = await globals.get_channel_from_id(channel_id)
                if not channel:
                    # If I don't have access to the channel, delete bridges from and to it
                    invalid_channel_ids.add(channel_webhook.channel)
                    continue

                if channel_webhook.webhook in invalid_webhook_ids:
                    # If I noticed that I can't fetch this webhook I add its channel to the list of invalid channels
                    invalid_channel_ids.add(channel_webhook.channel)
                    continue

                try:
                    webhook = await globals.client.fetch_webhook(webhook_id)
                except Exception:
                    # If I have access to the channel but not the webhook I remove that channel from targets
                    invalid_channel_ids.add(channel_webhook.channel)
                    invalid_webhook_ids.add(channel_webhook.webhook)
                    continue

                # Webhook and channel are valid
                add_webhook_async.append(
                    bridges.webhooks.add_webhook(channel_id, webhook)
                )
            await asyncio.gather(*add_webhook_async)

            # I will make a list of all target channels that have at least one source and delete the ones that don't
            all_target_channels: set[str] = set()
            targets_with_sources: set[str] = set()

            async_create_bridges: list[Coroutine[Any, Any, Bridge]] = []
            select_all_bridges: SQLSelect[tuple[DBBridge]] = SQLSelect(DBBridge)
            bridge_query_result: ScalarResult[DBBridge] = session.scalars(
                select_all_bridges
            )
            for bridge in bridge_query_result:
                target_id_str = bridge.target
                if target_id_str in invalid_channel_ids:
                    continue

                target_id = int(target_id_str)
                target_webhook = await bridges.webhooks.get_webhook(target_id)
                if not target_webhook:
                    # This target channel is not in my list of webhooks fetched from earlier, destroy this bridge
                    invalid_channel_ids.add(target_id_str)
                    continue

                webhook_id_str = str(target_webhook.id)
                if webhook_id_str in invalid_webhook_ids:
                    # This should almost certainly never happen
                    invalid_channel_ids.add(target_id_str)
                    if deleted_webhook_id := await bridges.webhooks.delete_channel(
                        target_id
                    ):
                        # After deleting this channel there were no longer any channels attached to this webhook
                        invalid_webhook_ids.add(str(deleted_webhook_id))
                    continue

                source_id_str = bridge.source
                source_id = int(source_id_str)
                source_channel = await globals.get_channel_from_id(source_id)
                if not source_channel:
                    # If I don't have access to the source channel, delete bridges from and to it
                    invalid_channel_ids.add(source_id_str)
                    if deleted_webhook_id := await bridges.webhooks.delete_channel(
                        source_id
                    ):
                        # After deleting this channel there were no longer any channels attached to this webhook
                        invalid_webhook_ids.add(str(deleted_webhook_id))
                else:
                    # I have access to both the source and target channels and to the webhook
                    # so I can add this channel to my list of Bridges
                    targets_with_sources.add(target_id_str)
                    async_create_bridges.append(
                        bridges.create_bridge(
                            source=source_id,
                            target=target_id,
                            webhook=target_webhook,
                            update_db=False,
                        )
                    )

            # Any target channels that don't have valid source channels attached to them should be deleted
            invalid_channel_ids = invalid_channel_ids.union(
                all_target_channels - targets_with_sources
            )

            # I'm going to delete all webhooks attached to invalid channels or to channels that aren't target channels
            channel_ids_with_webhooks_to_delete = {
                channel_id
                for channel_id_str in invalid_channel_ids
                if (channel_id := int(channel_id_str))
                and (await bridges.webhooks.get_webhook(channel_id))
            }.union(
                {
                    channel_id
                    for channel_id, webhook_id in bridges.webhooks._webhook_by_channel.items()
                    if str(channel_id) not in targets_with_sources
                    or str(webhook_id) in invalid_webhook_ids
                }
            )
            for channel_id in channel_ids_with_webhooks_to_delete:
                await bridges.webhooks.delete_channel(channel_id)

            # Gather bridge creation and webhook deletion
            await asyncio.gather(*async_create_bridges)

            # And update the database with any necessary deletions
            if (
                len(invalid_channel_ids) > 0
                or len(channel_ids_with_webhooks_to_delete) > 0
                or len(invalid_webhook_ids) > 0
            ):
                # First fetch the full list of channel IDs to delete
                channel_ids_to_delete = invalid_channel_ids.union(
                    {
                        str(channel_id)
                        for channel_id in channel_ids_with_webhooks_to_delete
                    }
                )

                if len(channel_ids_to_delete) > 0:
                    delete_invalid_bridges = SQLDelete(DBBridge).where(
                        sql_or(
                            DBBridge.source.in_(channel_ids_to_delete),
                            DBBridge.target.in_(channel_ids_to_delete),
                        )
                    )
                    session.execute(delete_invalid_bridges)

                    delete_invalid_messages = SQLDelete(DBMessageMap).where(
                        sql_or(
                            DBMessageMap.source_channel.in_(channel_ids_to_delete),
                            DBMessageMap.target_channel.in_(channel_ids_to_delete),
                        )
                    )
                    session.execute(delete_invalid_messages)

                delete_invalid_webhooks = SQLDelete(DBWebhook).where(
                    sql_or(
                        DBWebhook.channel.in_(channel_ids_to_delete),
                        DBWebhook.webhook.in_(invalid_webhook_ids),
                    )
                )
                session.execute(delete_invalid_webhooks)

            # Try to identify hashed emoji
            emoji_hash_map.map = emoji_hash_map.EmojiHashMap(session)

            # Try to find all apps whitelisted per channel
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
    except SQLError as e:
        if session:
            session.rollback()
            session.close()

        await globals.client.close()
        raise e

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
    outbound_bridges = bridges.get_outbound_bridges(message.channel.id)
    if not outbound_bridges:
        return

    # Ensure that the message has emoji I have access to
    message_content = await replace_missing_emoji(message.content)

    # Get all channels reachable from this one via an unbroken sequence of outbound bridges as well as their webhooks
    reachable_channels = await bridges.get_reachable_channels(
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
                    select_message_map: SQLSelect[tuple[DBMessageMap]] = SQLSelect(
                        DBMessageMap
                    ).where(DBMessageMap.target_message == str(replied_to_id))
                    return session.scalars(select_message_map).first()

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
                    select_bridged_reply_to: SQLSelect[tuple[DBMessageMap]] = SQLSelect(
                        DBMessageMap
                    ).where(DBMessageMap.source_message == str(source_replied_to_id))
                    return session.scalars(select_bridged_reply_to)

                query_result: ScalarResult[DBMessageMap] = await sql_retry(
                    get_bridged_reply_tos
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
                        continue
                    thread_splat = {"thread": target_channel}

                # Create an async version of bridging this message to gather at the end
                async_bridged_messages.append(
                    bridge_message_to_target_channel(
                        message,
                        message_content,
                        target_channel,
                        webhook,
                        webhook_channel,
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


@beartype
async def bridge_message_to_target_channel(
    message: discord.Message,
    message_content: str,
    target_channel: discord.TextChannel | discord.Thread,
    webhook: discord.Webhook,
    webhook_channel: discord.TextChannel,
    bridged_reply_to: int | None,
    reply_has_ping: bool,
    thread_splat: ThreadSplat,
    session: SQLSession,
) -> discord.WebhookMessage | None:
    """Bridge a message to a channel and returns the message bridged.

    #### Args:
        - `message`: The message being bridged.
        - `message_content`: Its contents
        - `target_channel`: The channel the message is being bridged to.
        - `webhook`: The webhook that will send the message.
        - `webhook_channel`: The parent channel the webhook is attached to.
        - `bridged_reply_to`: The ID of a message the message being bridged is replying to on the target channel.
        - `reply_has_ping`: Whether the reply is pinging the original message.
        - `thread_splat`: A splat with the thread this message is being bridged to, if any.
        - `session`: A connection to the database.

    #### Returns:
        - `discord.WebhookMessage`: The message bridged.
    """
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

    if bridged_reply_to:
        # The message being replied to is also bridged to this channel, so I'll create an embed to represent this
        try:
            message_replied_to = await target_channel.fetch_message(bridged_reply_to)

            def truncate(msg: str, length: int) -> str:
                return msg if len(msg) < length else msg[: length - 1] + "…"

            display_name = discord.utils.escape_markdown(
                message_replied_to.author.display_name
            )

            # Discord represents ping "ON" vs "OFF" replies with an @ symbol before the reply author name
            # copy this behavior here
            if reply_has_ping:
                display_name = "@" + display_name

            replied_content = await replace_missing_emoji(
                truncate(
                    discord.utils.remove_markdown(message_replied_to.clean_content),
                    50,
                )
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

    attachments = await asyncio.gather(
        *[attachment.to_file() for attachment in message.attachments]
    )

    try:
        return await webhook.send(
            content=message_content,
            allowed_mentions=discord.AllowedMentions(
                users=True, roles=False, everyone=False
            ),
            avatar_url=bridged_avatar_url,
            username=bridged_member_name,
            embeds=list(message.embeds + reply_embed),
            files=attachments,  # TODO might throw HHTPException if too large?
            wait=True,
            **thread_splat,
        )
    except discord.NotFound:
        # Webhook is gone, delete this bridge
        warn(
            f"Webhook in {target_channel.guild.name}:{target_channel.name} (ID: {target_channel.id}) not found, demolishing bridges to this channel and its threads."
        )
        await bridges.demolish_bridges(target_channel=target_channel, session=session)
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

    # Ensure that the message has emoji I have access to
    updated_message_content = await replace_missing_emoji(updated_message_content)

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

            def get_bridged_messages():
                select_message_map: SQLSelect[tuple[DBMessageMap]] = SQLSelect(
                    DBMessageMap
                ).where(DBMessageMap.source_message == payload.message_id)
                return session.scalars(select_message_map)

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
                            warn(
                                f"Webhook in {bridged_channel.guild.name}:{bridged_channel.name} (ID: {bridged_channel.id}) not found, demolishing bridges to this channel and its threads."
                            )
                            await bridges.demolish_bridges(
                                target_channel=bridged_channel, session=session
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

        await asyncio.gather(*async_message_edits)
    except SQLError as e:
        warn("Ran into an SQL error while trying to edit a message:\n" + str(e))


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

        await emoji_hash_map.map.ensure_hash_map(
            emoji_id=emoji_id, emoji_name=emoji_name
        )
        if emoji := emoji_hash_map.map.get_accessible_emoji(emoji_id, skip_self=True):
            # I don't have access to this emoji but I have a matching one in my emoji mappings
            emoji_to_replace[f"<{emoji_name}:{emoji_id_str}>"] = str(emoji)
            continue

        try:
            emoji = await copy_emoji_into_server(
                missing_emoji_id=emoji_id_str, missing_emoji_name=emoji_name
            )
            if emoji:
                emoji_to_replace[f"<{emoji_name}:{emoji_id_str}>"] = str(emoji)
        except Exception:
            pass

    for missing_emoji_str, new_emoji_str in emoji_to_replace.items():
        message_content = message_content.replace(missing_emoji_str, new_emoji_str)
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

            def get_bridged_messages():
                select_message_map: SQLSelect[tuple[DBMessageMap]] = SQLSelect(
                    DBMessageMap
                ).where(DBMessageMap.source_message == payload.message_id)
                return session.scalars(select_message_map)

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
                            warn(
                                f"Webhook in {bridged_channel.guild.name}:{bridged_channel.name} (ID: {bridged_channel.id}) not found, demolishing bridges to this channel and its threads."
                            )
                            await bridges.demolish_bridges(
                                target_channel=bridged_channel, session=session
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

    # Choose a "fallback emoji" to use in case I don't have access to the one being reacted and the message across the bridge doesn't already have it
    fallback_emoji: discord.Emoji | str | None
    if payload.emoji.is_custom_emoji():
        # Custom emoji, I need to check whether it exists and is available to me
        # I'll add this to my hash map if it's not there already
        await emoji_hash_map.map.ensure_hash_map(emoji=payload.emoji)

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
                fallback_emoji = await copy_emoji_into_server(
                    missing_emoji=payload.emoji
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
            def get_already_bridged_reactions():
                select_reaction_map: SQLSelect[tuple[DBReactionMap]] = SQLSelect(
                    DBReactionMap
                ).where(
                    DBReactionMap.source_message == source_message_id_str,
                    DBReactionMap.source_emoji == emoji_id_str,
                )
                return session.scalars(select_reaction_map)

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
                select_message_map: SQLSelect[tuple[DBMessageMap]] = SQLSelect(
                    DBMessageMap
                ).where(
                    DBMessageMap.target_message == source_message_id_str,
                )
                return session.scalars(select_message_map).first()

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
                select_message_map: SQLSelect[tuple[DBMessageMap]] = SQLSelect(
                    DBMessageMap
                ).where(DBMessageMap.source_message == str(source_message_id))
                return session.scalars(select_message_map)

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


@beartype
async def copy_emoji_into_server(
    *,
    missing_emoji: discord.PartialEmoji | None = None,
    missing_emoji_id: str | int | None = None,
    missing_emoji_name: str | None = None,
) -> discord.Emoji | None:
    """Try to create an emoji in the emoji server and, if successful, return it.

    #### Args:
        - `missing_emoji`: The emoji we are trying to copy into our emoji server. Defaults to None, in which case `missing_emoji_name` and `missing_emoji_id` are used instead.
        - `missing_emoji_id`: The ID of the missing emoji. Defaults to None, in which case `missing_emoji` is used instead.
        - `missing_emoji_name`: The name of a missing emoji, optionally preceded by an `"a:"` in case it's animated. Defaults to None, but must be included if `missing_emoji_id` is.

    #### Raises:
        - `ArgumentError`: The number of arguments passed is incorrect.
        - `ValueError`: `missing_emoji` argument was passed and had type `PartialEmoji` but it was not a custom emoji, or `missing_emoji_id` argument was passed and had type `str` but it was not a valid numerical ID.
        - `Forbidden`: Emoji server permissions not set correctly.
        - `HTTPResponseError`: HTTP request to fetch emoji image returned a status other than 200.
        - `InvalidURL`: URL generated from emoji ID was not valid.
        - `RuntimeError`: Session connection to the server to fetch image from URL failed.
        - `ServerTimeoutError`: Connection to server to fetch image from URL timed out.
    """
    if not globals.emoji_server:
        return None
    emoji_server_id = globals.emoji_server.id

    missing_emoji_id, missing_emoji_name, _, missing_emoji_url = (
        globals.get_emoji_information(
            missing_emoji, missing_emoji_id, missing_emoji_name
        )
    )

    image = await globals.get_image_from_URL(missing_emoji_url)
    image_hash = globals.hash_image(image)

    emoji_to_delete_id = None
    try:
        emoji = await globals.emoji_server.create_custom_emoji(
            name=missing_emoji_name, image=image, reason="Bridging reaction."
        )
    except discord.Forbidden as e:
        print("Emoji server permissions not set correctly.")
        raise e
    except discord.HTTPException as e:
        if len(globals.emoji_server.emojis) < 50:
            # Something weird happened, the error was not due to a full server
            raise e

        # Try to delete an emoji from the server and then add this again.
        emoji_to_delete = random.choice(globals.emoji_server.emojis)
        emoji_to_delete_id = emoji_to_delete.id
        await emoji_to_delete.delete()

        try:
            emoji = await globals.emoji_server.create_custom_emoji(
                name=missing_emoji_name, image=image, reason="Bridging reaction."
            )
        except discord.Forbidden as e:
            print("Emoji server permissions not set correctly.")
            raise e

    # Copied the emoji, going to update my table
    session = None
    try:
        with SQLSession(engine) as session:
            if emoji_to_delete_id is not None:
                await emoji_hash_map.map.delete_emoji(emoji_to_delete_id, session)

            await emoji_hash_map.map.add_emoji(
                emoji=emoji,
                emoji_server_id=emoji_server_id,
                image_hash=image_hash,
                is_internal=True,
                session=session,
            )

            if missing_emoji:
                await commands.map_emoji_helper(
                    external_emoji=missing_emoji,
                    internal_emoji=emoji,
                    image_hash=image_hash,
                    session=session,
                )
            else:
                await commands.map_emoji_helper(
                    external_emoji_id=missing_emoji_id,
                    external_emoji_name=missing_emoji_name,
                    internal_emoji=emoji,
                    image_hash=image_hash,
                    session=session,
                )

            session.commit()
    except SQLError as e:
        warn("Couldn't add emoji mapping to table.")
        print(e)

        if session:
            session.rollback()
            session.close()

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
    if not isinstance(channel, (discord.TextChannel, discord.Thread)):
        # This really shouldn't happen
        return

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
    except SQLError as e:
        if session:
            session.rollback()
            session.close()

        warn("Ran into an SQL error while trying to remove a reaction:\n" + str(e))
        return


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


@globals.client.event
async def on_guild_join(server: discord.Guild):
    print(f"Just joined server '{server.name}'. Hashing emoji...")
    await emoji_hash_map.map.load_server_emoji(server.id)
    print("Emoji hashed!")


@globals.client.event
async def on_guild_remove(server: discord.Guild):
    print(f"Just left server '{server.name}'.")


app_token = globals.settings.get("app_token")
assert isinstance(app_token, str)
globals.client.run(app_token, reconnect=True)
