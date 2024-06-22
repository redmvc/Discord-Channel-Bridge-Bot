import discord
from sqlalchemy import Delete as SQLDelete
from sqlalchemy import ScalarResult
from sqlalchemy import Select as SQLSelect
from sqlalchemy import and_ as sql_and
from sqlalchemy import or_ as sql_or
from sqlalchemy.dialects.mysql import insert as sql_insert
from sqlalchemy.orm import Session as SQLSession

import globals
from bridge import Bridge, bridges
from database import DBBridge, DBMessageMap, engine


@discord.app_commands.guild_only()
@globals.command_tree.command(
    name="bridge",
    description="Create a two-way bridge between two channels.",
)
async def bridge(
    interaction: discord.Interaction,
    target: str,
):
    message_channel = interaction.channel
    if not isinstance(message_channel, (discord.TextChannel, discord.Thread)):
        await interaction.response.send_message(
            "Please run this command from a text channel or a thread."
        )
        return

    target_channel = globals.mention_to_channel(target)
    if not isinstance(target_channel, (discord.TextChannel, discord.Thread)):
        # The argument passed needs to be a channel or thread
        await interaction.response.send_message(
            "Unsupported argument passed. Please pass a channel reference, ID, or link."
        )
        return

    if target_channel.id == message_channel.id:
        await interaction.response.send_message("You can't bridge a channel to itself.")
        return

    assert isinstance(interaction.user, discord.Member)
    if (
        not message_channel.permissions_for(interaction.user).manage_webhooks
        or not target_channel.permissions_for(interaction.user).manage_webhooks
    ):
        await interaction.response.send_message(
            "Please make sure you have 'Manage Webhooks' permission in both this and target channels."
        )
        return

    session = SQLSession(engine)
    await create_bridge_and_db(message_channel, target_channel, None, session)
    await create_bridge_and_db(target_channel, message_channel, None, session)
    session.commit()
    session.close()

    await interaction.response.send_message(
        "✅ Bridge created! Try sending a message from either channel 😁",
        ephemeral=True,
    )


@discord.app_commands.guild_only()
@globals.command_tree.command(
    name="outbound",
    description="Create an outbound bridge from this channel to target channel.",
)
async def outbound(
    interaction: discord.Interaction,
    target: str,
):
    message_channel = interaction.channel
    if not isinstance(message_channel, (discord.TextChannel, discord.Thread)):
        await interaction.response.send_message(
            "Please run this command from a text channel or a thread."
        )
        return

    target_channel = globals.mention_to_channel(target)
    if not isinstance(target_channel, (discord.TextChannel, discord.Thread)):
        # The argument passed needs to be a channel or thread
        await interaction.response.send_message(
            "Unsupported argument passed. Please pass a channel reference, ID, or link."
        )
        return

    if target_channel.id == message_channel.id:
        await interaction.response.send_message("You can't bridge a channel to itself.")
        return

    assert isinstance(interaction.user, discord.Member)
    if (
        not message_channel.permissions_for(interaction.user).manage_webhooks
        or not target_channel.permissions_for(interaction.user).manage_webhooks
    ):
        await interaction.response.send_message(
            "Please make sure you have 'Manage Webhooks' permission in both this and target channels."
        )
        return

    await create_bridge_and_db(message_channel, target_channel)
    await interaction.response.send_message(
        "✅ Bridge created! Try sending a message from this channel 😁",
        ephemeral=True,
    )


@discord.app_commands.guild_only()
@globals.command_tree.command(
    name="inbound",
    description="Create an inbound bridge from source channel to this channel.",
)
async def inbound(
    interaction: discord.Interaction,
    source: str,
):
    message_channel = interaction.channel
    if not isinstance(message_channel, (discord.TextChannel, discord.Thread)):
        await interaction.response.send_message(
            "Please run this command from a text channel or a thread."
        )
        return

    source_channel = globals.mention_to_channel(source)
    if not isinstance(source_channel, (discord.TextChannel, discord.Thread)):
        # The argument passed needs to be a channel or thread
        await interaction.response.send_message(
            "Unsupported argument passed. Please pass a channel reference, ID, or link."
        )
        return

    if source_channel.id == message_channel.id:
        await interaction.response.send_message("You can't bridge a channel to itself.")
        return

    assert isinstance(interaction.user, discord.Member)
    if (
        not message_channel.permissions_for(interaction.user).manage_webhooks
        or not source_channel.permissions_for(interaction.user).manage_webhooks
    ):
        await interaction.response.send_message(
            "Please make sure you have 'Manage Webhooks' permission in both this and target channels."
        )
        return

    await create_bridge_and_db(source_channel, message_channel)
    await interaction.response.send_message(
        "✅ Bridge created! Try sending a message from the other channel 😁",
        ephemeral=True,
    )


@discord.app_commands.guild_only()
@globals.command_tree.command(
    name="bridge_thread",
    description="Create threads across the bridge matching this one and bridge them.",
)
async def bridge_thread(
    interaction: discord.Interaction,
):
    message_thread = interaction.channel
    if not isinstance(message_thread, discord.Thread):
        await interaction.response.send_message(
            "Please run this command from a thread."
        )
        return

    if not isinstance(message_thread.parent, discord.TextChannel):
        await interaction.response.send_message(
            "Please run this command from a thread off a text channel."
        )
        return

    assert isinstance(interaction.user, discord.Member)
    if not message_thread.permissions_for(interaction.user).manage_webhooks:
        await interaction.response.send_message(
            "Please make sure you have 'Manage Webhooks' permission in this channel."
        )
        return

    outbound_bridges = bridges.get_outbound_bridges(message_thread.parent.id)
    inbound_bridges = bridges.get_inbound_bridges(message_thread.parent.id)
    if not outbound_bridges and not inbound_bridges:
        await interaction.response.send_message(
            "The parent channel isn't bridged to any other channels."
        )
        return

    await interaction.response.defer(thinking=True, ephemeral=True)

    # The IDs of threads are the same as that of their originating messages so we should try to create threads from the same messages
    session = SQLSession(engine)
    matching_starting_messages: dict[int, int] = {}
    try:
        await message_thread.parent.fetch_message(message_thread.id)

        source_starting_message = session.scalars(
            SQLSelect(DBMessageMap).where(
                DBMessageMap.target_message == str(message_thread.id)
            )
        ).first()
        if isinstance(source_starting_message, DBMessageMap):
            # The message that's starting this thread is bridged
            source_channel_id = int(source_starting_message.source_channel)
            source_message_id = int(source_starting_message.source_message)
            matching_starting_messages[source_channel_id] = source_message_id
        else:
            source_channel_id = message_thread.parent.id
            source_message_id = message_thread.id

        target_starting_messages: ScalarResult[DBMessageMap] = session.scalars(
            SQLSelect(DBMessageMap).where(
                DBMessageMap.source_message == str(source_message_id)
            )
        )
        for target_starting_message in target_starting_messages:
            matching_starting_messages[int(target_starting_message.target_channel)] = (
                int(target_starting_message.target_message)
            )
    except discord.NotFound:
        pass

    # Now find all channels that are bridged to the channel this thread's parent is bridged to and create threads there
    threads_created: dict[int, discord.Thread] = {}
    succeeded_at_least_once = False
    failed_at_least_once = False

    for idx in range(2):
        if idx == 0:
            list_of_bridges = outbound_bridges
        else:
            list_of_bridges = inbound_bridges
        if not list_of_bridges:
            continue

        for channel_id in list_of_bridges.keys():
            channel = globals.get_channel_from_id(channel_id)
            if not isinstance(channel, discord.TextChannel):
                # I can't create a thread inside a thread
                failed_at_least_once = True
                continue

            if (
                not channel.permissions_for(interaction.user).manage_webhooks
                or not channel.permissions_for(interaction.user).create_public_threads
            ):
                # User doesn't have permission to act there
                failed_at_least_once = True
                continue

            new_thread = threads_created.get(channel_id)
            if not new_thread and matching_starting_messages.get(channel_id):
                # I found a matching starting message, so I'll try to create the thread starting there
                matching_starting_message = await channel.fetch_message(
                    matching_starting_messages[channel_id]
                )

                if not matching_starting_message.thread:
                    # That message doesn't already have a thread, so I can create it
                    new_thread = await matching_starting_message.create_thread(
                        name=message_thread.name,
                        reason=f"Bridged from {message_thread.guild.name}#{message_thread.parent.name}#{message_thread.name}",
                    )

            if not new_thread:
                # Haven't created a thread yet, try to create it from the channel
                new_thread = await channel.create_thread(
                    name=message_thread.name,
                    reason=f"Bridged from {message_thread.guild.name}#{message_thread.parent.name}#{message_thread.name}",
                )

            if not new_thread:
                # Failed to create a thread somehow
                failed_at_least_once = True
                continue

            threads_created[channel_id] = new_thread
            if idx == 0:
                await create_bridge_and_db(message_thread, new_thread, None, session)
            else:
                await create_bridge_and_db(new_thread, message_thread, None, session)
            succeeded_at_least_once = True

    if succeeded_at_least_once:
        if not failed_at_least_once:
            await interaction.followup.send("✅ All threads created!")
        else:
            await interaction.followup.send(
                "⭕ Some but not all threads were created; this may have happened because you lacked Manage Webhooks or Create Public Threads permissions. Note that trying to run this command again will duplicate threads.",
            )
    else:
        await interaction.followup.send(
            "❌ Couldn't create any threads. Check that you have Manage Webhooks and Create Public Threads permissions in all relevant channels."
        )

    session.commit()
    session.close()


@discord.app_commands.guild_only()
@globals.command_tree.command(
    name="demolish",
    description="Demolish all bridges between this and target channel.",
)
async def demolish(
    interaction: discord.Interaction,
    target: str,
):
    message_channel = interaction.channel
    if not isinstance(message_channel, (discord.TextChannel, discord.Thread)):
        await interaction.response.send_message(
            "Please run this command from a text channel or a thread."
        )
        return

    target_channel = globals.mention_to_channel(target)
    if not isinstance(target_channel, (discord.TextChannel, discord.Thread)):
        # The argument passed needs to be a channel or thread
        await interaction.response.send_message(
            "Unsupported argument passed. Please pass a channel reference, ID, or link."
        )
        return

    assert isinstance(interaction.user, discord.Member)
    if (
        not message_channel.permissions_for(interaction.user).manage_webhooks
        or not target_channel.permissions_for(interaction.user).manage_webhooks
    ):
        await interaction.response.send_message(
            "Please make sure you have 'Manage Webhooks' permission in both this and target channels."
        )
        return

    await demolish_bridges(message_channel, target_channel)

    message_channel_id = str(message_channel.id)
    target_channel_id = str(target_channel.id)
    session = SQLSession(engine)

    delete_demolished_bridges = SQLDelete(DBBridge).where(
        sql_or(
            sql_and(
                DBBridge.source == message_channel_id,
                DBBridge.target == target_channel_id,
            ),
            sql_and(
                DBBridge.source == target_channel_id,
                DBBridge.target == message_channel_id,
            ),
        )
    )
    session.execute(delete_demolished_bridges)

    delete_demolished_messages = SQLDelete(DBMessageMap).where(
        sql_or(
            sql_and(
                DBMessageMap.source_channel == message_channel_id,
                DBMessageMap.target_channel == target_channel_id,
            ),
            sql_and(
                DBMessageMap.source_channel == target_channel_id,
                DBMessageMap.target_channel == message_channel_id,
            ),
        )
    )
    session.execute(delete_demolished_messages)

    session.commit()
    session.close()

    await interaction.response.send_message(
        "✅ Bridges demolished!",
        ephemeral=True,
    )


@discord.app_commands.guild_only()
@globals.command_tree.command(
    name="demolish_all",
    description="Demolish all bridges to and from this channel.",
)
async def demolish_all(
    interaction: discord.Interaction,
):
    message_channel = interaction.channel
    if not isinstance(message_channel, (discord.TextChannel, discord.Thread)):
        await interaction.response.send_message(
            "Please run this command from a text channel or a thread."
        )
        return

    assert isinstance(interaction.user, discord.Member)
    if not message_channel.permissions_for(interaction.user).manage_webhooks:
        await interaction.response.send_message(
            "Please make sure you have 'Manage Webhooks' permission in this channel."
        )
        return

    # I'll make a list of all channels that are currently bridged to or from this channel
    inbound_bridges = bridges.get_inbound_bridges(message_channel.id)
    paired_channels: set[int]
    if inbound_bridges:
        paired_channels = set(inbound_bridges.keys())
    else:
        paired_channels = set()

    outbound_bridges = bridges.get_outbound_bridges(message_channel.id)
    exceptions: set[int] = set()
    if outbound_bridges:
        for target_id in outbound_bridges.keys():
            target_channel = globals.get_channel_from_id(target_id)
            assert isinstance(target_channel, (discord.TextChannel, discord.Thread))
            if not target_channel.permissions_for(interaction.user).manage_webhooks:
                # If I don't have Manage Webhooks permission in the target, I can't destroy the bridge from there
                exceptions.add(target_id)
            else:
                paired_channels.add(target_id)

    for channel_id in paired_channels:
        await demolish_bridges(channel_id, message_channel)

    session = SQLSession(engine)
    message_channel_id = str(message_channel.id)
    exceptions_list = [str(i) for i in exceptions]

    delete_demolished_bridges = SQLDelete(DBBridge).where(
        sql_or(
            DBBridge.target == message_channel_id,
            sql_and(
                DBBridge.source == message_channel_id,
                DBBridge.target.not_in(exceptions_list),
            ),
        )
    )
    session.execute(delete_demolished_bridges)

    delete_demolished_messages = SQLDelete(DBMessageMap).where(
        sql_or(
            DBMessageMap.target_channel == message_channel_id,
            sql_and(
                DBMessageMap.source_channel == message_channel_id,
                DBMessageMap.target_channel.not_in(exceptions_list),
            ),
        )
    )
    session.execute(delete_demolished_messages)

    session.commit()
    session.close()

    if len(exceptions) == 0:
        await interaction.response.send_message(
            "✅ Bridges demolished!",
            ephemeral=True,
        )
    else:
        await interaction.response.send_message(
            "⭕ Inbound bridges demolished, but some outbound bridges may not have been, as some permissions were missing.",
            ephemeral=True,
        )


async def create_bridge_and_db(
    source: discord.TextChannel | discord.Thread | int,
    target: discord.TextChannel | discord.Thread | int,
    webhook: discord.Webhook | None = None,
    session: SQLSession | None = None,
) -> Bridge:
    """Create a one-way Bridge from source channel to target channel in `bridges`, creating a webhook if necessary, then inserts a reference to this new bridge into the database.

    #### Args:
        - `source`: Source channel for the Bridge, or ID of same.
        - `target`: Target channel for the Bridge, or ID of same.
        - `webhook`: Optionally, an already-existing webhook connecting these channels. Defaults to None.
        - `session`: Optionally, a session with the connection to the database. Defaults to None, in which case creates and closes a new one locally.
    """
    bridge = await create_bridge(source, target, webhook)

    if not session:
        close_after = True
        session = SQLSession(engine)
    else:
        close_after = False

    insert_bridge_row = (
        sql_insert(DBBridge)
        .values(
            source=str(globals.get_id_from_channel(source)),
            target=str(globals.get_id_from_channel(target)),
            webhook=str(bridge.webhook.id),
        )
        .on_duplicate_key_update(  # TODO abstract this away so it doesn't rely on being specifically MySQL?
            webhook=str(bridge.webhook.id),
        )
    )
    session.execute(insert_bridge_row)

    if close_after:
        session.commit()
        session.close()

    return bridge


async def create_bridge(
    source: discord.TextChannel | discord.Thread | int,
    target: discord.TextChannel | discord.Thread | int,
    webhook: discord.Webhook | None = None,
) -> Bridge:
    """Create a one-way Bridge from source channel to target channel in `bridges`, creating a webhook if necessary. This function does not alter the database entries in any way.

    #### Args:
        - `source`: Source channel for the Bridge, or ID of same.
        - `target`: Target channel for the Bridge, or ID of same.
        - `webhook`: Optionally, an already-existing webhook connecting these channels. Defaults to None.
    """

    return await bridges.create_bridge(source, target, webhook)


async def demolish_bridges(
    source: discord.TextChannel | discord.Thread | int,
    target: discord.TextChannel | discord.Thread | int,
):
    """Destroy all Bridges between source and target channels, removing them from `bridges` and deleting their webhooks. This function does not alter the database entries in any way.

    #### Args:
        - `source`: One end of the Bridge, or ID of same.
        - `target`: The other end of the Bridge, or ID of same.
    """

    await demolish_bridge_one_sided(source, target)
    await demolish_bridge_one_sided(target, source)


async def demolish_bridge_one_sided(
    source: discord.TextChannel | discord.Thread | int,
    target: discord.TextChannel | discord.Thread | int,
):
    """Destroy the Bridge going from source channel to target channel, removing it from `bridges` and deleting its webhook. This function does not alter the database entries in any way.

    #### Args:
        - `source`: One end of the Bridge, or ID of same.
        - `target`: The other end of the Bridge, or ID of same.
    """

    await bridges.demolish_bridge(source, target)


@globals.command_tree.context_menu(
    name="List Reactions",
)
async def list_reactions(interaction: discord.Interaction, message: discord.Message):
    """List all reactions and users who reacted on all sides of a bridge."""
    channel = message.channel
    if not isinstance(channel, (discord.TextChannel, discord.Thread)):
        await interaction.response.send_message(
            "Please run this command from a text channel or a thread.", ephemeral=True
        )
        return

    inbound_bridges = bridges.get_inbound_bridges(channel.id)
    outbound_bridges = bridges.get_outbound_bridges(channel.id)
    if not inbound_bridges and not outbound_bridges:
        await interaction.response.send_message(
            "This channel isn't bridged.", ephemeral=True
        )
        return

    bot_user_id = globals.client.user.id if globals.client.user else 0

    # First get the reactions on this message itself
    all_reactions: dict[str, set[int]] = {}
    for reaction in message.reactions:
        reaction_emoji_id = str(reaction.emoji)

        if not all_reactions.get(reaction_emoji_id):
            all_reactions[reaction_emoji_id] = set()

        async for user in reaction.users():
            if user.id != bot_user_id:
                all_reactions[reaction_emoji_id].add(user.id)

    # Then get the bridged ones
    session = SQLSession(engine)
    # We need to see whether this message is a bridged message and, if so, find its source
    source_message_map = session.scalars(
        SQLSelect(DBMessageMap).where(
            DBMessageMap.target_message == str(message.id),
        )
    ).first()
    source_message_id: int | None = None
    message_id_to_skip: int | None = None
    if isinstance(source_message_map, DBMessageMap):
        # This message was bridged, so find the original one and then find any other bridged messages from it
        source_channel = globals.get_channel_from_id(source_message_map.source_channel)
        if source_channel:
            source_channel_id = source_channel.id
            source_message_id = int(source_message_map.source_message)
            message_id_to_skip = message.id
    else:
        # This message is (or might be) the source
        source_message_id = message.id
        source_channel_id = channel.id

    # Then we find all messages bridged from the source
    outbound_bridges = bridges.get_outbound_bridges(source_channel_id)
    if not outbound_bridges:
        # If there are no outbound bridges we just skip over the next bit and get to the end
        source_message_id = None

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

        if not outbound_bridges or not outbound_bridges.get(target_channel_id):
            continue

        bridged_channel = globals.get_channel_from_id(target_channel_id)
        if not isinstance(bridged_channel, (discord.TextChannel, discord.Thread)):
            continue

        bridged_message = await bridged_channel.fetch_message(target_message_id)
        for reaction in bridged_message.reactions:
            reaction_emoji_id = str(reaction.emoji)

            if not all_reactions.get(reaction_emoji_id):
                all_reactions[reaction_emoji_id] = set()

            async for user in reaction.users():
                if user.id != bot_user_id:
                    all_reactions[reaction_emoji_id].add(user.id)

    session.close()

    if len(all_reactions) == 0:
        await interaction.response.send_message(
            "This message doesn't have any reactions.", ephemeral=True
        )
        return

    await interaction.response.send_message(
        "This message has the following reactions:\n"
        + "\n\n".join(
            [
                f"{reaction_emoji_id} "
                + " ".join([f"<@{user_id}>" for user_id in reaction_user_ids])
                for reaction_emoji_id, reaction_user_ids in all_reactions.items()
            ]
        ),
        ephemeral=True,
    )
