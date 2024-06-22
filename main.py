from __future__ import annotations

from typing import TypedDict, cast

import discord
from sqlalchemy import Delete as SQLDelete
from sqlalchemy import ScalarResult
from sqlalchemy import Select as SQLSelect
from sqlalchemy import and_ as sql_and
from sqlalchemy import or_ as sql_or
from sqlalchemy.orm import Session as SQLSession

import commands
import globals
from bridge import bridges
from database import DBBridge, DBMessageMap, engine


class ThreadSplat(TypedDict, total=False):
    thread: discord.Thread


@globals.client.event
async def on_ready():
    """Called when the client is done preparing the data received from Discord. Usually after login is successful and the Client.guilds and co. are filled up."""
    if globals.is_ready:
        return

    # I am going to try to identify all existing bridges
    session = SQLSession(engine)
    registered_bridges: ScalarResult[DBBridge] = session.scalars(SQLSelect(DBBridge))
    invalid_channels: set[str] = set()
    invalid_webhooks: set[str] = set()
    for bridge in registered_bridges:
        source_id_str = bridge.source
        target_id_str = bridge.target
        webhook_id_str = bridge.webhook

        if webhook_id_str in invalid_webhooks:
            continue

        source_id = int(source_id_str)
        source_channel = globals.get_channel_from_id(int(source_id))
        if not source_channel:
            # If I don't have access to the source channel, delete bridges from and to it
            invalid_channels.add(source_id_str)

        target_id = int(target_id_str)
        target_channel = globals.get_channel_from_id(int(target_id))
        if not target_channel:
            # If I don't have access to the source channel, delete bridges from and to it
            invalid_channels.add(target_id_str)

        try:
            webhook = await globals.client.fetch_webhook(int(webhook_id_str))

            if not source_channel:
                # I have access to the target webhook but not to the source channel anymore so I'll delete the webhook
                await webhook.delete(reason="Source channel no longer available.")
                raise Exception
            elif target_channel:
                # I have access to both the source and target channels and to the webhook
                await commands.create_bridge(source_id, target_id, webhook)
        except Exception:
            invalid_webhooks.add(webhook_id_str)

            if source_channel and target_channel:
                # There *should* be a webhook there and I have access to the channels
                await commands.create_bridge_and_db(source_id, target_id, None, session)

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

    session.commit()

    await globals.command_tree.sync()
    print(f"{globals.client.user} is connected to the following servers:\n")
    for server in globals.client.guilds:
        print(f"{server.name}(id: {server.id})")

    globals.is_ready = True


@globals.client.event
async def on_message(message: discord.Message):
    """Called when a Message is created and sent.

    This requires Intents.messages to be enabled."""
    if not isinstance(message.channel, (discord.TextChannel, discord.Thread)):
        return

    if message.content == "":
        # Only bridge contentful messages
        return

    if message.webhook_id:
        # Don't bridge messages from webhooks
        return

    if not await globals.wait_until_ready():
        return

    outbound_bridges = bridges.get_outbound_bridges(message.channel.id)
    if not outbound_bridges:
        return

    session = SQLSession(engine)
    reply_bridges: dict[int, int] = {}
    if message.reference and message.reference.message_id:
        # This message is a reply to another message, so we should try to link to its match on the other side of bridges
        # reply_bridges will be a dict whose keys are channel IDs and whose values are the IDs of messages matching the message I'm replying to in those channels
        reply_id = message.reference.message_id

        # First, check whether the message replied to was itself bridged from somewhere
        reply_source_match = session.scalars(
            SQLSelect(DBMessageMap).where(DBMessageMap.target_message == str(reply_id))
        ).first()
        if isinstance(reply_source_match, DBMessageMap):
            # So the message replied to was bridged from elsewhere
            reply_source_id = int(reply_source_match.source_message)
            reply_source_channel_id = int(reply_source_match.source_channel)
            reply_bridges[reply_source_channel_id] = reply_source_id
        else:
            # The message this is replying to might have been the source of a bridge, not the target
            reply_source_id = reply_id
            reply_source_channel_id = message.channel.id

        # Now find all other bridged versions of the message we're replying to
        reply_bridge_match: ScalarResult[DBMessageMap] = session.scalars(
            SQLSelect(DBMessageMap).where(
                DBMessageMap.source_message == str(reply_source_id)
            )
        )
        for message_match in reply_bridge_match:
            reply_bridges[int(message_match.target_channel)] = int(
                message_match.target_message
            )

    # Send a message out to each target webhook
    bridged_message_ids = []
    bridged_channel_ids = list(outbound_bridges.keys())
    for target_id, bridge in outbound_bridges.items():
        webhook = bridge.webhook
        if not webhook:
            continue

        webhook_channel = webhook.channel
        if not isinstance(webhook_channel, discord.TextChannel):
            continue

        target_channel = globals.get_channel_from_id(target_id)
        target_channel = cast(discord.TextChannel | discord.Thread, target_channel)

        # attachments = []  # TODO
        # should_spoiler = message.channel.is_nsfw() and not webhook_channel.is_nsfw()

        # Try to find whether the user who sent this message is on the other side of the bridge and if so what their name and avatar would be
        tgt_member = webhook_channel.guild.get_member(message.author.id)
        if tgt_member:
            tgt_member_name = tgt_member.display_name
            tgt_avatar_url = tgt_member.display_avatar
        else:
            tgt_member_name = message.author.display_name
            tgt_avatar_url = message.author.display_avatar

        if reply_bridges.get(target_id):
            # This message is replying to a message that is bridged
            replied_message = await target_channel.fetch_message(
                reply_bridges[target_id]
            )
            reply_embed = [
                discord.Embed(
                    description=f"Reply to [{replied_message.author.display_name}]({replied_message.jump_url})"
                )
            ]
        else:
            reply_embed = []

        thread_splat: ThreadSplat = {}
        if target_id != webhook_channel.id:
            if not isinstance(target_channel, discord.Thread):
                continue
            thread_splat = {"thread": target_channel}
        bridged_message = await webhook.send(
            content=message.content,
            allowed_mentions=discord.AllowedMentions(
                users=True, roles=False, everyone=False
            ),
            avatar_url=tgt_avatar_url,
            username=tgt_member_name,
            embeds=list(message.embeds + reply_embed),
            wait=True,
            **thread_splat,
        )

        bridged_message_ids.append(bridged_message.id)

    if len(bridged_message_ids) == 0:
        session.close()
        return

    # Insert references to the linked messages into the message_mappings table
    source_message_id = str(message.id)
    source_channel_id = str(message.channel.id)
    session.add_all(
        [
            DBMessageMap(
                source_message=source_message_id,
                source_channel=source_channel_id,
                target_message=str(bridged_message_id),
                target_channel=str(bridged_channel_id),
            )
            for bridged_message_id, bridged_channel_id in zip(
                bridged_message_ids, bridged_channel_ids
            )
        ]
    )
    session.commit()
    session.close()


@globals.client.event
async def on_raw_message_edit(payload: discord.RawMessageUpdateEvent):
    """Called when a message is edited. Unlike `on_message_edit()`, this is called regardless of the state of the internal message cache.

    #### Args:
        - `payload`: The raw event payload data.
    """
    updated_message_content = payload.data.get("content")
    if not updated_message_content or updated_message_content == "":
        # Not a content edit
        return

    if not await globals.wait_until_ready():
        return

    outbound_bridges = bridges.get_outbound_bridges(payload.channel_id)
    if not outbound_bridges:
        return

    # Find all messages matching this one
    session = SQLSession(engine)
    bridged_messages: ScalarResult[DBMessageMap] = session.scalars(
        SQLSelect(DBMessageMap).where(DBMessageMap.source_message == payload.message_id)
    )
    for message_row in bridged_messages:
        target_channel_id = int(message_row.target_channel)
        bridge = outbound_bridges.get(target_channel_id)
        if not bridge:
            continue

        bridged_channel = globals.get_channel_from_id(target_channel_id)
        if not isinstance(bridged_channel, (discord.TextChannel, discord.Thread)):
            continue

        thread_splat: ThreadSplat = {}
        if isinstance(bridged_channel, discord.Thread):
            if not isinstance(bridged_channel.parent, discord.TextChannel):
                continue
            thread_splat = {"thread": bridged_channel}

        await bridge.webhook.edit_message(
            message_id=int(message_row.target_message),
            content=updated_message_content,
            **thread_splat,
        )

    session.close()


@globals.client.event
async def on_raw_message_delete(payload: discord.RawMessageDeleteEvent):
    """Called when a message is deleted. Unlike `on_message_delete()`, this is called regardless of the message being in the internal message cache or not.

    #### Args:
        - `payload`: The raw event payload data.
    """
    if not await globals.wait_until_ready():
        return

    outbound_bridges = bridges.get_outbound_bridges(payload.channel_id)
    if not outbound_bridges:
        return

    # Find all messages matching this one
    session = SQLSession(engine)
    bridged_messages: ScalarResult[DBMessageMap] = session.scalars(
        SQLSelect(DBMessageMap).where(DBMessageMap.source_message == payload.message_id)
    )
    for message_row in bridged_messages:
        target_channel_id = int(message_row.target_channel)
        bridge = outbound_bridges.get(target_channel_id)
        if not bridge:
            continue

        bridged_channel = globals.get_channel_from_id(target_channel_id)
        if not isinstance(bridged_channel, (discord.TextChannel, discord.Thread)):
            continue

        thread_splat: ThreadSplat = {}
        if isinstance(bridged_channel, discord.Thread):
            if not isinstance(bridged_channel.parent, discord.TextChannel):
                continue
            thread_splat = {"thread": bridged_channel}

        await bridge.webhook.delete_message(
            int(message_row.target_message),
            **thread_splat,
        )

    # If the message was bridged, delete its row
    # If it was a source of bridged messages, delete all rows of its bridged versions
    session.execute(
        SQLDelete(DBMessageMap).where(
            sql_or(
                DBMessageMap.source_message == str(payload.message_id),
                DBMessageMap.target_message == str(payload.message_id),
            )
        )
    )

    session.commit()
    session.close()


@globals.client.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    """Called when a message has a reaction added. Unlike `on_reaction_add()`, this is called regardless of the state of the internal message cache.

    #### Args:
        - `payload`: The raw event payload data.
    """
    if not await globals.wait_until_ready():
        return

    if not globals.client.user or payload.user_id == globals.client.user.id:
        # Don't bridge my own reaction
        return

    outbound_bridges = bridges.get_outbound_bridges(payload.channel_id)
    inbound_bridges = bridges.get_inbound_bridges(payload.channel_id)
    if not outbound_bridges and not inbound_bridges:
        return

    # Check whether I have access to the emoji
    reaction_emoji: discord.Emoji | discord.PartialEmoji | str | None
    reaction_emoji = payload.emoji
    if reaction_emoji.is_custom_emoji():
        # Custom emoji, I need to check whether it exists and is available to me
        if reaction_emoji.id:
            reaction_emoji = globals.client.get_emoji(reaction_emoji.id)
            if not reaction_emoji:
                return

            if not reaction_emoji.available:
                # TODO set up a personal emoji server to add emoji to
                return
        else:
            return
    else:
        # It's a standard emoji, it's fine
        reaction_emoji = reaction_emoji.name

    # Find all messages matching this one
    # First, check whether this message is bridged, in which case I need to find its source
    session = SQLSession(engine)
    source_message_map = session.scalars(
        SQLSelect(DBMessageMap).where(
            DBMessageMap.target_message == str(payload.message_id),
        )
    ).first()
    message_id_to_skip: int | None = None
    if isinstance(source_message_map, DBMessageMap):
        # This message was bridged, so find the original one, react to it, and then find any other bridged messages from it
        source_channel = globals.get_channel_from_id(
            int(source_message_map.source_channel)
        )
        if not source_channel:
            return

        assert isinstance(source_channel, (discord.TextChannel, discord.Thread))

        source_message_id = int(source_message_map.source_message)
        source_message = await source_channel.fetch_message(source_message_id)
        if source_message:
            await source_message.add_reaction(reaction_emoji)

        message_id_to_skip = (
            payload.message_id
        )  # Don't add a reaction back to this message
    else:
        # This message is (or might be) the source
        source_message_id = payload.message_id

    outbound_bridges = bridges.get_outbound_bridges(source_message_id)
    if not outbound_bridges:
        session.close()
        return

    bridged_messages: ScalarResult[DBMessageMap] = session.scalars(
        SQLSelect(DBMessageMap).where(
            sql_and(
                DBMessageMap.source_message == str(source_message_id),
                DBMessageMap.target_message != str(message_id_to_skip),
            )
        )
    )
    for message_row in bridged_messages:
        target_message_id = int(message_row.target_message)
        target_channel_id = int(message_row.target_channel)

        bridge = outbound_bridges.get(target_channel_id)
        if not bridge:
            continue

        bridged_channel = globals.get_channel_from_id(target_channel_id)
        if not isinstance(bridged_channel, (discord.TextChannel, discord.Thread)):
            continue

        bridged_message = await bridged_channel.fetch_message(target_message_id)
        await bridged_message.add_reaction(reaction_emoji)

    session.close()


globals.client.run(cast(str, globals.credentials["app_token"]), reconnect=True)
