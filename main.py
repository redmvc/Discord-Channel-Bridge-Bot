from __future__ import annotations

import asyncio
from typing import TypedDict, cast
from warnings import warn

import discord
from sqlalchemy import Delete as SQLDelete
from sqlalchemy import ScalarResult
from sqlalchemy import Select as SQLSelect
from sqlalchemy import and_ as sql_and
from sqlalchemy import or_ as sql_or
from sqlalchemy.exc import StatementError as SQLError
from sqlalchemy.orm import Session as SQLSession

import commands
import globals
from bridge import bridges
from database import DBAutoBridgeThreadChannels, DBBridge, DBMessageMap, engine
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

        session.commit()

        # And next I identify all automatically-thread-bridging channels
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

    await globals.command_tree.sync()
    print(f"{globals.client.user} is connected to the following servers:\n")
    for server in globals.client.guilds:
        print(f"{server.name}(id: {server.id})")

    globals.is_ready = True


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

    if message.webhook_id:
        # Don't bridge messages from webhooks
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
                local_replied_to_message_map: DBMessageMap | None = session.scalars(
                    SQLSelect(DBMessageMap).where(
                        DBMessageMap.target_message == str(replied_to_id)
                    )
                ).first()
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
                select_bridged_reply_to: SQLSelect = SQLSelect(DBMessageMap).where(
                    DBMessageMap.source_message == str(source_replied_to_id)
                )
                query_result: ScalarResult[DBMessageMap] = session.scalars(
                    select_bridged_reply_to
                )
                for message_map in query_result:
                    bridged_reply_to[int(message_map.target_channel)] = int(
                        message_map.target_message
                    )

            # Send a message out to each target webhook
            successful_bridges: dict[str, str] = {}
            for target_id, bridge in outbound_bridges.items():
                webhook = bridge.webhook
                if not webhook:
                    continue

                webhook_channel = webhook.channel
                if not isinstance(webhook_channel, discord.TextChannel):
                    continue

                target_channel = await bridge.target_channel

                thread_splat: ThreadSplat = {}
                if target_id != webhook_channel.id:
                    # The target channel is not the same as the webhook's channel, so it should be a thread
                    if not isinstance(target_channel, discord.Thread):
                        continue
                    thread_splat = {"thread": target_channel}

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
                            return msg if len(msg) < length else msg[: length - 1] + "…"

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

                bridged_message = await webhook.send(
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

                successful_bridges[str(target_id)] = str(bridged_message.id)

            if len(successful_bridges) == 0:
                return

            # Insert references to the linked messages into the message_mappings table
            source_message_id_str = str(message.id)
            source_channel_id_str = str(message.channel.id)
            session.add_all(
                [
                    DBMessageMap(
                        source_message=source_message_id_str,
                        source_channel=source_channel_id_str,
                        target_message=bridged_message_id,
                        target_channel=bridged_channel_id,
                    )
                    for bridged_message_id, bridged_channel_id in successful_bridges.items()
                ]
            )

            session.commit()
    except SQLError as e:
        if session:
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

    outbound_bridges = bridges.get_outbound_bridges(payload.channel_id)
    if not outbound_bridges:
        return

    # Find all messages matching this one
    try:
        with SQLSession(engine) as session:
            bridged_messages: ScalarResult[DBMessageMap] = session.scalars(
                SQLSelect(DBMessageMap).where(
                    DBMessageMap.source_message == payload.message_id
                )
            )
            for message_row in bridged_messages:
                target_channel_id = int(message_row.target_channel)
                bridge = outbound_bridges.get(target_channel_id)
                if not bridge:
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
                    await bridge.webhook.edit_message(
                        message_id=int(message_row.target_message),
                        content=updated_message_content,
                        **thread_splat,
                    )
                except discord.HTTPException as e:
                    warn(
                        "Ran into a Discord exception while trying to edit a message across a bridge:\n"
                        + str(e)
                    )
    except SQLError as e:
        warn("Ran into an SQL error while trying to edit a message:\n" + str(e))


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

    outbound_bridges = bridges.get_outbound_bridges(payload.channel_id)
    if not outbound_bridges:
        return

    # Find all messages matching this one
    session = None
    try:
        with SQLSession(engine) as session:
            bridged_messages: ScalarResult[DBMessageMap] = session.scalars(
                SQLSelect(DBMessageMap).where(
                    DBMessageMap.source_message == payload.message_id
                )
            )
            for message_row in bridged_messages:
                target_channel_id = int(message_row.target_channel)
                bridge = outbound_bridges.get(target_channel_id)
                if not bridge:
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
                    await bridge.webhook.delete_message(
                        int(message_row.target_message),
                        **thread_splat,
                    )
                except discord.HTTPException as e:
                    warn(
                        "Ran into a Discord exception while trying to delete a message across a bridge:\n"
                        + str(e)
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
    except SQLError as e:
        if session:
            session.close()

        warn("Ran into an SQL error while trying to delete a message:\n" + str(e))
        return


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
    session = None
    try:
        # First, check whether this message is bridged, in which case I need to find its source
        with SQLSession(engine) as session:
            source_message_map = session.scalars(
                SQLSelect(DBMessageMap).where(
                    DBMessageMap.target_message == str(payload.message_id),
                )
            ).first()
            message_id_to_skip: int | None = None
            if isinstance(source_message_map, DBMessageMap):
                # This message was bridged, so find the original one, react to it, and then find any other bridged messages from it
                source_channel = await globals.get_channel_from_id(
                    int(source_message_map.source_channel)
                )
                if not source_channel:
                    return

                assert isinstance(source_channel, (discord.TextChannel, discord.Thread))

                source_message_id = int(source_message_map.source_message)
                try:
                    source_message = await source_channel.fetch_message(
                        source_message_id
                    )
                    if source_message:
                        await source_message.add_reaction(reaction_emoji)
                except discord.HTTPException as e:
                    warn(
                        "Ran into a Discord exception while trying to add a reaction across a bridge:\n"
                        + str(e)
                    )

                message_id_to_skip = (
                    payload.message_id
                )  # Don't add a reaction back to this message
                source_channel_id = source_channel.id
            else:
                # This message is (or might be) the source
                source_message_id = payload.message_id
                source_channel_id = payload.channel_id

            outbound_bridges = bridges.get_outbound_bridges(source_channel_id)
            if not outbound_bridges:
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

                bridged_channel = await globals.get_channel_from_id(target_channel_id)
                if not isinstance(
                    bridged_channel, (discord.TextChannel, discord.Thread)
                ):
                    continue

                try:
                    bridged_message = await bridged_channel.fetch_message(
                        target_message_id
                    )
                    await bridged_message.add_reaction(reaction_emoji)
                except discord.HTTPException as e:
                    warn(
                        "Ran into a Discord exception while trying to add a reaction across a bridge:\n"
                        + str(e)
                    )
    except SQLError as e:
        if session:
            session.close()

        warn(
            "Ran into an SQL error while trying to add a reaction to a message:\n"
            + str(e)
        )


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

    if thread.last_message:
        # The message that was used to create the thread will need to be bridged, as the bridge didn't exist at the time
        await bridge_message_helper(thread.last_message)


globals.client.run(cast(str, globals.credentials["app_token"]), reconnect=True)
