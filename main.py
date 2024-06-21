from __future__ import annotations

import asyncio
from typing import TypedDict, cast

import discord
from sqlalchemy import Delete as SQLDelete
from sqlalchemy import ScalarResult
from sqlalchemy import Select as SQLSelect
from sqlalchemy import or_ as sql_or
from sqlalchemy.orm import Session as SQLSession

import commands
import globals
from bridge import bridges
from database import DBBridge, engine


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

    if message.webhook_id:
        # Don't bridge messages from webhooks
        return

    # I need to wait until the on_ready event is done before processing any messages
    time_waited = 0
    while not globals.is_ready and time_waited < 100:
        await asyncio.sleep(1)
        time_waited += 1
    if time_waited >= 100:
        # somethin' real funky going on here
        # I don't have error handling yet though
        print("Taking forever to get ready.")
        return

    outbound_bridges = bridges.get_outbound_bridges(message.channel.id)
    if not outbound_bridges:
        return

    # Send a message out to each target webhook
    for target_id, bridge in outbound_bridges.items():
        webhook = bridge.webhook
        if not webhook:
            continue

        target_channel = webhook.channel
        if not target_channel:
            continue

        assert isinstance(target_channel, discord.TextChannel)

        # attachments = []  # TODO
        # should_spoiler = message.channel.is_nsfw() and not target_channel.is_nsfw()

        tgt_member = target_channel.guild.get_member(message.author.id)
        if tgt_member:
            tgt_member_name = tgt_member.display_name
            tgt_avatar_url = tgt_member.display_avatar
        else:
            tgt_member_name = message.author.display_name
            tgt_avatar_url = message.author.display_avatar

        class ThreadSplat(TypedDict, total=False):
            thread: discord.Thread

        thread_splat: ThreadSplat = {}
        if target_id != target_channel.id:
            thread = target_channel.get_thread(target_id)
            assert thread
            thread_splat = {"thread": thread}

        await webhook.send(
            content=message.content,
            allowed_mentions=discord.AllowedMentions(
                users=True, roles=False, everyone=False
            ),
            avatar_url=tgt_avatar_url,
            username=tgt_member_name,
            wait=True,
            **thread_splat,
        )
        # TODO replies


globals.client.run(cast(str, globals.credentials["app_token"]), reconnect=True)
