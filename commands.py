import asyncio
from typing import Any, AsyncIterator, Coroutine, Iterable, cast

import discord
from sqlalchemy import Delete as SQLDelete
from sqlalchemy import ScalarResult
from sqlalchemy import Select as SQLSelect
from sqlalchemy import and_ as sql_and
from sqlalchemy import or_ as sql_or
from sqlalchemy.exc import StatementError as SQLError
from sqlalchemy.orm import Session as SQLSession

import globals
from bridge import Bridge, bridges
from database import (
    DBAutoBridgeThreadChannels,
    DBBridge,
    DBEmojiMap,
    DBMessageMap,
    engine,
    sql_retry,
    sql_upsert,
)
from validations import validate_types


@globals.command_tree.command(
    name="help",
    description="Return a list of commands or detailed information about a command.",
)
@discord.app_commands.describe(command="The command to get detailed information about.")
async def help(interaction: discord.Interaction, command: str | None = None):
    if (
        globals.emoji_server
        and interaction.guild
        and interaction.guild.id == globals.emoji_server.id
    ):
        interaction_from_emoji_server = True
    else:
        interaction_from_emoji_server = False

    if not command:
        if interaction_from_emoji_server:
            map_emoji_mention = ", `/map_emoji`"
        else:
            map_emoji_mention = ""

        await interaction.response.send_message(
            "This bot bridges channels and threads to each other, mirroring messages sent from one to the other. When a message is bridged:"
            + "\n- its copies will show the avatar and name of the person who wrote the original message;"
            + "\n- attachments will be copied over;"
            + "\n- edits to the original message will be reflected in the bridged messages;"
            + "\n- whenever someone adds a reaction to one message the bot will add the same reaction (if it can) to all of its mirrors;"
            + "\n- and deleting the original message will delete its copies (but not vice-versa)."
            + "\nThreads created in a channel do not automatically get matched to other channels bridged to it; create and bridge them manually or use the `/bridge_thread` or `/auto_bridge_threads` command."
            + f"\n\nList of commands: `/bridge`, `/bridge_thread`, `/auto_bridge_threads`, `/demolish`, `/demolish_all`{map_emoji_mention}, `/help`.\nType `/help command` for detailed explanation of a command.",
            ephemeral=True,
        )
    else:
        command = command.lower()
        if command == "bridge":
            await interaction.response.send_message(
                "`/bridge target [direction]`"
                + "\nCreates a bridge between the current channel/thread and target channel/thread, creating a mirror of a message sent to one channel in the other. `target` must be a link to another channel or thread, its ID, or a mention to it."
                + "\nIf `direction` isn't included, the bridge is two-way; if it's set to `inbound` it will only send messages from the target channel to the current channel; if it's set to `outbound` it will only send messages from the current channel to the target channel."
                + "\n\nNote that message mirroring goes down outbound bridge chains: if channel A has an outbound bridge to channel B and channel B has an outbound bridge to channel C, messages sent in channel A will be mirrored in both channels B and C. _However_, this does not automatically create a bridge between A and C: if e.g. the bridge between A and B is demolished, messages from A will no longer be sent to C.",
                ephemeral=True,
            )
        elif command == "bridge_thread":
            await interaction.response.send_message(
                "`/bridge_thread`"
                + "\nWhen this command is called from within a thread that is in a channel that is bridged to other channels, the bot will attempt to create new threads in all such channels and bridge them to the original one. If the original channel is bridged to threads or if you don't have create thread permissions in the other channels, this command may not run to completion."
                + "\n\nNote that this command will not create bridges down bridge chains—that is, if channel A is bridged to channel B and channel B is bridged to channel C, but A is not bridged to C, executing this command in channel A will not create a thread in channel C.",
                ephemeral=True,
            )
        elif command == "auto_bridge_threads":
            await interaction.response.send_message(
                "`/auto_bridge_threads`"
                + "\nWhen this command is called from within a channel that is bridged to other channels, the bot will enable or disable automatic thread bridging, so that any threads created in this channel will also be created across all bridges involving it. You will need to run this command from within each channel you wish to enable automatic thread creation from."
                + "\n\nNote that this command will not create bridges down bridge chains—that is, if channel A is bridged to channel B and channel B is bridged to channel C, but A is not bridged to C, threads automatically created in channel A will not have a mirror thread in channel C.",
                ephemeral=True,
            )
        elif command == "demolish":
            await interaction.response.send_message(
                "`/demolish target`"
                + "\nDestroys any existing bridges between the current and target channels/threads, making messages from either channel no longer be mirrored to the other. `target` must be a link to another channel or thread, its ID, or a mention to it."
                + "\n\nNote that even if you recreate any of the bridges, the messages previously bridged will no longer be connected and so they will not share future reactions, edits, or deletions. Note also that this will only destroy bridges to and from the _current specific channel/thread_, not from any threads that spin off it or its parent.",
                ephemeral=True,
            )
        elif command == "demolish_all":
            await interaction.response.send_message(
                "`/demolish_all [channel_and_threads]`"
                + "\nDestroys any existing bridges involving the current channel or thread, making messages from it no longer be mirrored to other channels and making other channels' messages no longer be mirrored to it."
                + "\n\nIf you don't include `channel_and_threads` or set it to `False`, this will _only_ demolish bridges involving the _current specific channel/thread_. If instead you set `channel_and_threads` to `True`, this will demolish _all_ bridges involving the current channel/thread, its parent channel if it's a thread, and all of its or its parent channel's threads."
                + "\n\nNote that even if you recreate any of the bridges, the messages previously bridged will no longer be connected and so they will not share future reactions, edits, or deletions.",
                ephemeral=True,
            )
        elif command == "map_emoji" and interaction_from_emoji_server:
            await interaction.response.send_message(
                "`/map_emoji :internal_emoji: :external_emoji: [:external_emoji_2: [:external_emoji_3: ...]]`"
                + "\nCreates an internal mapping between an emoji from an external server which the bot doesn't have access to and an emoji stored in the bot's emoji server, so that they are considered equivalent by the bot when bridging reactions. You can also pass multiple external emoji separated by spaces to map all of them to the same internal one.",
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                "❌ Unrecognised command. Type `/help` for the full list.",
                ephemeral=True,
            )


@discord.app_commands.default_permissions(manage_webhooks=True)
@discord.app_commands.guild_only()
@globals.command_tree.command(
    name="bridge",
    description="Create a bridge between two channels.",
)
@discord.app_commands.describe(
    target="The channel to and/or from which to bridge.",
    direction="Whether to create an outbound or inbound bridge. Leave blank to create both.",
)
@discord.app_commands.choices(
    direction=[
        discord.app_commands.Choice(name="outbound", value="outbound"),
        discord.app_commands.Choice(name="inbound", value="inbound"),
    ]
)
async def bridge(
    interaction: discord.Interaction,
    target: str,
    direction: str | None = None,
):
    message_channel = interaction.channel
    if not isinstance(message_channel, (discord.TextChannel, discord.Thread)):
        await interaction.response.send_message(
            "❌ Please run this command from a text channel or a thread.",
            ephemeral=True,
        )
        return

    target_channel = await globals.mention_to_channel(target)
    if not isinstance(target_channel, (discord.TextChannel, discord.Thread)):
        # The argument passed needs to be a channel or thread
        await interaction.response.send_message(
            "❌ Unsupported argument passed. Please pass a channel reference, ID, or link.",
            ephemeral=True,
        )
        return

    if target_channel.id == message_channel.id:
        await interaction.response.send_message(
            "❌ You can't bridge a channel to itself.", ephemeral=True
        )
        return

    assert isinstance(interaction.user, discord.Member)
    assert interaction.guild
    target_channel_member = await globals.get_channel_member(
        target_channel, interaction.user.id
    )
    if (
        not message_channel.permissions_for(interaction.user).manage_webhooks
        or not target_channel_member
        or not target_channel.permissions_for(target_channel_member).manage_webhooks
        or not message_channel.permissions_for(interaction.guild.me).manage_webhooks
        or not target_channel.permissions_for(target_channel.guild.me).manage_webhooks
    ):
        await interaction.response.send_message(
            "❌ Please make sure both you and the bot have 'Manage Webhooks' permission in both this and target channels.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(thinking=True, ephemeral=True)

    join_threads: list[Coroutine] = []
    if isinstance(message_channel, discord.Thread) and not message_channel.me:
        try:
            join_threads.append(message_channel.join())
        except Exception:
            pass
    if isinstance(target_channel, discord.Thread) and not target_channel.me:
        try:
            join_threads.append(target_channel.join())
        except Exception:
            pass

    session = None
    try:
        with SQLSession(engine) as session:
            create_bridges = []
            if direction != "inbound":
                create_bridges.append(
                    create_bridge_and_db(message_channel, target_channel, session)
                )
            if direction != "outbound":
                create_bridges.append(
                    create_bridge_and_db(target_channel, message_channel, session)
                )

            await asyncio.gather(*create_bridges)
            session.commit()
    except SQLError:
        await interaction.followup.send(
            "❌ There was an issue with the connection to the database; bridge creation failed.",
            ephemeral=True,
        )
        if session:
            session.rollback()
            session.close()
        return

    if not direction:
        direction_str = "either"
    elif direction == "inbound":
        direction_str = "the other"
    else:
        direction_str = "this"
    await interaction.followup.send(
        f"✅ Bridge created! Try sending a message from {direction_str} channel 😁",
        ephemeral=True,
    )

    await asyncio.gather(*join_threads)


@discord.app_commands.default_permissions(
    manage_webhooks=True, create_public_threads=True
)
@discord.app_commands.guild_only()
@globals.command_tree.command(
    name="bridge_thread",
    description="Create threads across the bridge matching this one and bridge them.",
)
async def bridge_thread(interaction: discord.Interaction):
    message_thread = interaction.channel
    if not isinstance(message_thread, discord.Thread):
        await interaction.response.send_message(
            "❌ Please run this command from a thread.",
            ephemeral=True,
        )
        return

    if not isinstance(message_thread.parent, discord.TextChannel):
        await interaction.response.send_message(
            "❌ Please run this command from a thread off a text channel.",
            ephemeral=True,
        )
        return

    assert isinstance(interaction.user, discord.Member)
    assert interaction.guild
    if (
        not message_thread.permissions_for(interaction.user).manage_webhooks
        or not message_thread.permissions_for(interaction.user).create_public_threads
        or not message_thread.permissions_for(interaction.guild.me).manage_webhooks
        or not message_thread.permissions_for(
            interaction.guild.me
        ).create_public_threads
    ):
        await interaction.response.send_message(
            "❌ Please make sure both you and the bot have Manage Webhooks and Create Public Threads permissions in both this and target channels.",
            ephemeral=True,
        )
        return

    await bridge_thread_helper(message_thread, interaction.user.id, interaction)


@discord.app_commands.default_permissions(
    manage_webhooks=True, create_public_threads=True
)
@discord.app_commands.guild_only()
@globals.command_tree.command(
    name="auto_bridge_threads",
    description="Enable or disable automatic thread bridging from this channel.",
)
async def auto_bridge_threads(
    interaction: discord.Interaction,
):
    message_channel = interaction.channel
    if not isinstance(message_channel, discord.TextChannel):
        await interaction.response.send_message(
            "❌ Please run this command from a text channel.",
            ephemeral=True,
        )
        return

    assert isinstance(interaction.user, discord.Member)
    assert interaction.guild
    if (
        not message_channel.permissions_for(interaction.user).manage_webhooks
        or not message_channel.permissions_for(interaction.guild.me).manage_webhooks
    ):
        await interaction.response.send_message(
            "❌ Please make sure both you and the bot have Manage Webhooks and Create Public Threads permissions in both this and target channels.",
            ephemeral=True,
        )
        return

    outbound_bridges = bridges.get_outbound_bridges(message_channel.id)
    inbound_bridges = bridges.get_inbound_bridges(message_channel.id)
    if not outbound_bridges and not inbound_bridges:
        await interaction.response.send_message(
            "❌ This channel isn't bridged to any other channels.",
            ephemeral=True,
        )
        return

    # I need to check that the current channel is bridged to at least one other channel (as opposed to only threads)
    at_least_one_channel = False
    for bridge_list in (outbound_bridges, inbound_bridges):
        if not bridge_list:
            continue

        for target_id, bridge in bridge_list.items():
            if target_id == bridge.webhook.channel_id:
                at_least_one_channel = True
                break

        if at_least_one_channel:
            break
    if not at_least_one_channel:
        await interaction.response.send_message(
            "❌ This channel is only bridged to threads.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(thinking=True, ephemeral=True)

    session = None
    try:
        with SQLSession(engine) as session:
            if message_channel.id not in globals.auto_bridge_thread_channels:

                def add_to_table():
                    session.add(
                        DBAutoBridgeThreadChannels(channel=str(message_channel.id))
                    )

                await sql_retry(add_to_table)
                globals.auto_bridge_thread_channels.add(message_channel.id)

                response = "✅ Threads will now be automatically created across bridges when they are created in this channel."
            else:
                await stop_auto_bridging_threads_helper(message_channel.id, session)

                response = "✅ Threads will no longer be automatically created across bridges when they are created in this channel."

            session.commit()
    except SQLError:
        await interaction.followup.send(
            "❌ There was an issue with the connection to the database; setting or unsetting automatic thread creation across bridges failed.",
            ephemeral=True,
        )
        if session:
            session.rollback()
            session.close()
        return

    await interaction.followup.send(response, ephemeral=True)


@discord.app_commands.default_permissions(manage_webhooks=True)
@discord.app_commands.guild_only()
@globals.command_tree.command(
    name="demolish",
    description="Demolish all bridges between this and target channel.",
)
@discord.app_commands.describe(
    target="The channel to and from whose bridges to destroy."
)
async def demolish(interaction: discord.Interaction, target: str):
    message_channel = interaction.channel
    if not isinstance(message_channel, (discord.TextChannel, discord.Thread)):
        await interaction.response.send_message(
            "❌ Please run this command from a text channel or a thread.",
            ephemeral=True,
        )
        return

    target_channel = await globals.mention_to_channel(target)
    if not isinstance(target_channel, (discord.TextChannel, discord.Thread)):
        # The argument passed needs to be a channel or thread
        await interaction.response.send_message(
            "❌ Unsupported argument passed. Please pass a channel reference, ID, or link.",
            ephemeral=True,
        )
        return

    assert isinstance(interaction.user, discord.Member)
    assert interaction.guild
    target_channel_member = await globals.get_channel_member(
        target_channel, interaction.user.id
    )
    if (
        not message_channel.permissions_for(interaction.user).manage_webhooks
        or not target_channel_member
        or not target_channel.permissions_for(target_channel_member).manage_webhooks
        or not message_channel.permissions_for(interaction.guild.me).manage_webhooks
        or not target_channel.permissions_for(target_channel.guild.me).manage_webhooks
    ):
        await interaction.response.send_message(
            "❌ Please make sure both you and the bot have 'Manage Webhooks' permission in both this and target channels.",
            ephemeral=True,
        )
        return

    inbound_bridges = bridges.get_inbound_bridges(message_channel.id)
    outbound_bridges = bridges.get_outbound_bridges(message_channel.id)
    if (not inbound_bridges or not inbound_bridges.get(target_channel.id)) and (
        not outbound_bridges or not outbound_bridges.get(target_channel.id)
    ):
        await interaction.response.send_message(
            "❌ There are no bridges between current and target channels.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(thinking=True, ephemeral=True)

    demolishing = demolish_bridges(message_channel, target_channel)

    message_channel_id = str(message_channel.id)
    target_channel_id = str(target_channel.id)

    session = None
    try:
        with SQLSession(engine) as session:
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

            def execute_queries():
                session.execute(delete_demolished_bridges)
                session.execute(delete_demolished_messages)

            await sql_retry(execute_queries)
            await demolishing
            await validate_auto_bridge_thread_channels(
                {message_channel.id, target_channel.id}, session
            )
            session.commit()
    except SQLError:
        await interaction.followup.send(
            "❌ There was an issue with the connection to the database; thread and bridge creation failed.",
            ephemeral=True,
        )
        if session:
            session.rollback()
            session.close()
        return

    await interaction.followup.send(
        "✅ Bridges demolished!",
        ephemeral=True,
    )


@discord.app_commands.default_permissions(manage_webhooks=True)
@discord.app_commands.guild_only()
@globals.command_tree.command(
    name="demolish_all",
    description="Demolish all bridges to and from this channel.",
)
@discord.app_commands.describe(
    channel_and_threads="Set to true to demolish bridges attached to this channel's parent and/or other threads.",
)
async def demolish_all(
    interaction: discord.Interaction, channel_and_threads: bool | None = None
):
    message_channel = interaction.channel
    if not isinstance(message_channel, (discord.TextChannel, discord.Thread)):
        await interaction.response.send_message(
            "❌ Please run this command from a text channel or a thread.",
            ephemeral=True,
        )
        return

    assert isinstance(interaction.user, discord.Member)
    assert interaction.guild
    if (
        not message_channel.permissions_for(interaction.user).manage_webhooks
        or not message_channel.permissions_for(interaction.guild.me).manage_webhooks
    ):
        await interaction.response.send_message(
            "❌ Please make sure both you and the bot have 'Manage Webhooks' permission in both this and target channels.",
            ephemeral=True,
        )
        return

    # If channel_and_threads I'm going to demolish all bridges connected to the current channel and its threads
    if channel_and_threads:
        if isinstance(message_channel, discord.Thread):
            thread_parent_channel = message_channel.parent
            if not isinstance(thread_parent_channel, discord.TextChannel):
                await interaction.response.send_message(
                    "❌ Please run this command from a text channel or a thread off one.",
                    ephemeral=True,
                )
                return
        else:
            thread_parent_channel = message_channel

        channels_to_check = [thread_parent_channel] + thread_parent_channel.threads
    else:
        channels_to_check = [message_channel]
    channels_affected = {channel.id for channel in channels_to_check}
    lists_of_bridges = {
        channel.id: (
            bridges.get_inbound_bridges(channel.id),
            bridges.get_outbound_bridges(channel.id),
        )
        for channel in channels_to_check
    }

    found_bridges = any(
        [
            inbound_bridges is not None or outbound_bridges is not None
            for _, (inbound_bridges, outbound_bridges) in lists_of_bridges.items()
        ]
    )
    if not found_bridges:
        await interaction.response.send_message(
            "❌ There are no bridges associated with the current channel or thread(s).",
            ephemeral=True,
        )
        return

    await interaction.response.defer(thinking=True, ephemeral=True)

    # I'll make a list of all channels that are currently bridged to or from this channel
    bridges_being_demolished = []
    session = None
    try:
        with SQLSession(engine) as session:
            for channel_to_demolish_id, (
                inbound_bridges,
                outbound_bridges,
            ) in lists_of_bridges.items():
                paired_channels: set[int]
                if inbound_bridges:
                    paired_channels = set(inbound_bridges.keys())
                else:
                    paired_channels = set()

                exceptions: set[int] = set()
                if outbound_bridges:
                    for target_id in outbound_bridges.keys():
                        target_channel = await globals.get_channel_from_id(target_id)
                        assert isinstance(
                            target_channel, (discord.TextChannel, discord.Thread)
                        )
                        target_channel_member = await globals.get_channel_member(
                            target_channel, interaction.user.id
                        )
                        if (
                            not target_channel_member
                            or not target_channel.permissions_for(
                                target_channel_member
                            ).manage_webhooks
                            or not target_channel.permissions_for(
                                target_channel.guild.me
                            ).manage_webhooks
                        ):
                            # If I don't have Manage Webhooks permission in the target, I can't destroy the bridge from there
                            exceptions.add(target_id)
                        else:
                            paired_channels.add(target_id)

                channels_affected = channels_affected.union(paired_channels)

                channel_to_demolish_id_str = str(channel_to_demolish_id)
                exceptions_list = [str(i) for i in exceptions]

                delete_demolished_bridges = SQLDelete(DBBridge).where(
                    sql_or(
                        DBBridge.target == channel_to_demolish_id_str,
                        sql_and(
                            DBBridge.source == channel_to_demolish_id_str,
                            DBBridge.target.not_in(exceptions_list),
                        ),
                    )
                )

                delete_demolished_messages = SQLDelete(DBMessageMap).where(
                    sql_or(
                        DBMessageMap.target_channel == channel_to_demolish_id_str,
                        sql_and(
                            DBMessageMap.source_channel == channel_to_demolish_id_str,
                            DBMessageMap.target_channel.not_in(exceptions_list),
                        ),
                    )
                )

                def execute_queries():
                    session.execute(delete_demolished_bridges)
                    session.execute(delete_demolished_messages)

                await sql_retry(execute_queries)

                for paired_channel_id in paired_channels:
                    bridges_being_demolished.append(
                        demolish_bridges(paired_channel_id, channel_to_demolish_id)
                    )

            await validate_auto_bridge_thread_channels(channels_affected, session)

            session.commit()
    except SQLError:
        await interaction.followup.send(
            "❌ There was an issue with the connection to the database; bridge demolition failed.",
            ephemeral=True,
        )
        if session:
            session.rollback()
            session.close()
        return

    await asyncio.gather(*bridges_being_demolished)
    if len(exceptions) == 0:
        await interaction.followup.send(
            "✅ Bridges demolished!",
            ephemeral=True,
        )
    else:
        await interaction.followup.send(
            "⭕ Inbound bridges demolished, but some outbound bridges may not have been, as some permissions were missing.",
            ephemeral=True,
        )


@discord.app_commands.default_permissions(
    create_expressions=True, manage_expressions=True
)
@globals.command_tree.command(
    name="map_emoji",
    description="Create a mapping between emoji so that the bot considers them equivalent.",
    guild=globals.emoji_server,
)
@discord.app_commands.rename(
    internal_emoji_id_str="internal_emoji",
)
@discord.app_commands.describe(
    internal_emoji_id_str="The emoji from this server to map the external emoji to, or its ID.",
    external_emojis="The emoji/emojis from another server or its ID/their IDs.",
)
async def map_emoji(
    interaction: discord.Interaction,
    internal_emoji_id_str: str,
    external_emojis: str,
):
    if not globals.settings.get("emoji_server_id"):
        await interaction.response.send_message(
            "❌ Bot doesn't have an emoji server registered.", ephemeral=True
        )
        return

    external_emoji_ids_str = []
    external_emoji_names = []
    for external_emoji in external_emojis.split():
        external_emoji_id_str = (
            external_emoji.replace("<:", "")
            .replace("<", "")
            .replace(">", "")
            .replace("\\", "")
        )
        if ":" in external_emoji_id_str:
            emoji_data = external_emoji_id_str.split(":")
            external_emoji_ids_str.append(emoji_data[-1])
            external_emoji_names.append(emoji_data[-2])
        else:
            external_emoji_names.append("")

    internal_emoji_id_str = (
        internal_emoji_id_str.replace("<:", "")
        .replace("<", "")
        .replace(">", "")
        .replace("\\", "")
    )
    if ":" in internal_emoji_id_str:
        internal_emoji_id_str = internal_emoji_id_str.split(":")[-1]

    try:
        external_emoji_ids = [int(id) for id in external_emoji_ids_str]
        internal_emoji_id = int(internal_emoji_id_str)
    except Exception:
        await interaction.response.send_message(
            "❌ Emoji IDs not valid.", ephemeral=True
        )
        return

    internal_emoji = globals.client.get_emoji(internal_emoji_id)

    if (
        not internal_emoji
        or not internal_emoji.guild
        or not globals.emoji_server
        or internal_emoji.guild_id != globals.emoji_server.id
    ):
        await interaction.response.send_message(
            "❌ The first argument must be an emoji in the bot's registered emoji server.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(thinking=True, ephemeral=True)

    try:
        map_emojis = await asyncio.gather(
            *[
                map_emoji_helper(
                    external_emoji=id,
                    external_emoji_name=name,
                    internal_emoji=internal_emoji,
                )
                for id, name in zip(external_emoji_ids, external_emoji_names)
            ]
        )
    except Exception:
        await interaction.followup.send(
            f"❌ There was a database error trying to map emoji to {str(internal_emoji)}.",
            ephemeral=True,
        )
        return

    if not max(map_emojis):
        await interaction.followup.send(
            f"❌ There was a problem creating emoji mappings to {str(internal_emoji)}.",
            ephemeral=True,
        )
    elif not min(map_emojis):
        await interaction.followup.send(
            f"⭕ There was a problem creating some of the emoji mappings to {str(internal_emoji)}.",
            ephemeral=True,
        )
    else:
        await interaction.followup.send(
            f"✅ All emoji mappings to {str(internal_emoji)} created!",
            ephemeral=True,
        )


async def create_bridge_and_db(
    source: discord.TextChannel | discord.Thread | int,
    target: discord.TextChannel | discord.Thread | int,
    session: SQLSession | None = None,
    webhook: discord.Webhook | None = None,
) -> Bridge:
    """Create a one-way Bridge from source channel to target channel in `bridges`, creating a webhook if necessary, then inserts a reference to this new bridge into the database.

    #### Args:
        - `source`: Source channel for the Bridge, or ID of same.
        - `target`: Target channel for the Bridge, or ID of same.
        - `webhook`: Optionally, an already-existing webhook connecting these channels. Defaults to None.
        - `session`: Optionally, a session with the connection to the database. Defaults to None, in which case creates and closes a new one locally.

    #### Raises:
        - `ChannelTypeError`: The source or target channels are not text channels nor threads off a text channel.
        - `WebhookChannelError`: `webhook` is not attached to Bridge's target channel.
        - `HTTPException`: Deleting an existing webhook or creating a new one failed.
        - `Forbidden`: You do not have permissions to create or delete webhooks.

    #### Returns:
        - `Bridge`: The created `Bridge`.
    """
    if webhook:
        validate_types({"webhook": (webhook, discord.Webhook)})

    bridge = None
    try:
        if not session:
            close_after = True
            session = SQLSession(engine)
        else:
            validate_types({"session": (session, SQLSession)})
            close_after = False

        bridge = await create_bridge(source, target, webhook)
        insert_bridge_row = await sql_upsert(
            DBBridge,
            {
                "source": str(globals.get_id_from_channel(source)),
                "target": str(globals.get_id_from_channel(target)),
                "webhook": str(bridge.webhook.id),
            },
            {"webhook": str(bridge.webhook.id)},
        )

        def execute_query():
            session.execute(insert_bridge_row)

        await sql_retry(execute_query)
    except SQLError as e:
        if session:
            session.rollback()
            session.close()

        raise e
    except Exception as e:
        if session:
            session.rollback()
            session.close()
        if bridge:
            await bridges.demolish_bridge(source, target)
        raise e

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

    #### Raises:
        - `ChannelTypeError`: The source or target channels are not text channels nor threads off a text channel.
        - `WebhookChannelError`: `webhook` is not attached to Bridge's target channel.
        - `HTTPException`: Deleting an existing webhook or creating a new one failed.
        - `Forbidden`: You do not have permissions to create or delete webhooks.

    #### Returns:
        - `Bridge`: The created `Bridge`.
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

    #### Raises:
        - `HTTPException`: Deleting the webhook failed.
        - `Forbidden`: You do not have permissions to delete the webhook.
        - `ValueError`: The webhook does not have a token associated with it.
    """

    await asyncio.gather(
        demolish_bridge_one_sided(source, target),
        demolish_bridge_one_sided(target, source),
    )


async def demolish_bridge_one_sided(
    source: discord.TextChannel | discord.Thread | int,
    target: discord.TextChannel | discord.Thread | int,
):
    """Destroy the Bridge going from source channel to target channel, removing it from `bridges` and deleting its webhook. This function does not alter the database entries in any way.

    #### Args:
        - `source`: One end of the Bridge, or ID of same.
        - `target`: The other end of the Bridge, or ID of same.

    #### Raises:
        - `HTTPException`: Deleting the webhook failed.
        - `Forbidden`: You do not have permissions to delete the webhook.
        - `ValueError`: The webhook does not have a token associated with it.
    """

    await bridges.demolish_bridge(source, target)


async def bridge_thread_helper(
    thread_to_bridge: discord.Thread,
    user_id: int,
    interaction: discord.Interaction | None = None,
):
    """Create threads matching the current one across bridges.

    #### Args:
        - `thread_to_bridge`: The thread to bridge.
        - `user_id`: ID of the user that created the thread.
        - `interaction`: The interaction that called this function, if any. Defaults to None.

    #### Asserts:
        - `isinstance(thread_to_bridge.parent, discord.TextChannel)`
    """
    types_to_validate: dict = {
        "thread_to_bridge": (thread_to_bridge, discord.Thread),
        "user_id": (user_id, int),
    }
    if interaction:
        types_to_validate["interaction"] = (interaction, discord.Interaction)
    validate_types(types_to_validate)

    assert isinstance(thread_to_bridge.parent, discord.TextChannel)

    outbound_bridges = bridges.get_outbound_bridges(thread_to_bridge.parent.id)
    inbound_bridges = bridges.get_inbound_bridges(thread_to_bridge.parent.id)
    if not outbound_bridges:
        if interaction:
            await interaction.response.send_message(
                "❌ The parent channel doesn't have outbound bridges to any other channels.",
                ephemeral=True,
            )
        return

    # I need to check that the current channel is bridged to at least one other channel (as opposed to only threads)
    at_least_one_channel = False
    for target_id, bridge in outbound_bridges.items():
        if target_id == bridge.webhook.channel_id:
            at_least_one_channel = True
            break

    if not at_least_one_channel:
        if interaction:
            await interaction.response.send_message(
                "❌ The parent channel is only bridged to threads.",
                ephemeral=True,
            )
        return

    if interaction:
        await interaction.response.defer(thinking=True, ephemeral=True)

    # The IDs of threads are the same as that of their originating messages so we should try to create threads from the same messages
    session = None
    try:
        with SQLSession(engine) as session:
            matching_starting_messages: dict[int, int] = {}
            try:
                # I don't need to store it I just need to know whether it exists
                await thread_to_bridge.parent.fetch_message(thread_to_bridge.id)

                def get_source_starting_message():
                    return session.scalars(
                        SQLSelect(DBMessageMap).where(
                            DBMessageMap.target_message == str(thread_to_bridge.id)
                        )
                    ).first()

                source_starting_message: DBMessageMap | None = await sql_retry(
                    get_source_starting_message
                )
                if isinstance(source_starting_message, DBMessageMap):
                    # The message that's starting this thread is bridged
                    source_channel_id = int(source_starting_message.source_channel)
                    source_message_id = int(source_starting_message.source_message)
                    matching_starting_messages[source_channel_id] = source_message_id
                else:
                    source_channel_id = thread_to_bridge.parent.id
                    source_message_id = thread_to_bridge.id

                def get_target_starting_messages():
                    return session.scalars(
                        SQLSelect(DBMessageMap).where(
                            DBMessageMap.source_message == str(source_message_id)
                        )
                    )

                target_starting_messages: ScalarResult[DBMessageMap] = await sql_retry(
                    get_target_starting_messages
                )
                for target_starting_message in target_starting_messages:
                    matching_starting_messages[
                        int(target_starting_message.target_channel)
                    ] = int(target_starting_message.target_message)
            except discord.NotFound:
                pass

            # Now find all channels that are bridged to the channel this thread's parent is bridged to and create threads there
            threads_created: dict[int, discord.Thread] = {}
            succeeded_at_least_once = False
            bridged_threads = []
            failed_channels = []

            create_bridges: list[Coroutine] = []
            add_user_to_threads: list[Coroutine] = []
            try:
                add_user_to_threads.append(thread_to_bridge.join())
            except Exception:
                pass

            for channel_id in outbound_bridges.keys():
                channel = await globals.get_channel_from_id(channel_id)
                if not isinstance(channel, discord.TextChannel):
                    # I can't create a thread inside a thread
                    if channel:
                        bridged_threads.append(channel.id)
                    continue

                channel_member = await globals.get_channel_member(channel, user_id)
                if (
                    not channel_member
                    or not channel.permissions_for(channel_member).manage_webhooks
                    or not channel.permissions_for(channel_member).create_public_threads
                    or not channel.permissions_for(channel.guild.me).manage_webhooks
                    or not channel.permissions_for(
                        channel.guild.me
                    ).create_public_threads
                ):
                    # User doesn't have permission to act there
                    failed_channels.append(channel.id)
                    continue

                new_thread = threads_created.get(channel_id)
                thread_already_existed = new_thread is not None
                if not new_thread and matching_starting_messages.get(channel_id):
                    # I found a matching starting message, so I'll try to create the thread starting there
                    matching_starting_message = await channel.fetch_message(
                        matching_starting_messages[channel_id]
                    )

                    if not matching_starting_message.thread:
                        # That message doesn't already have a thread, so I can create it
                        new_thread = await matching_starting_message.create_thread(
                            name=thread_to_bridge.name,
                            reason=f"Bridged from {thread_to_bridge.guild.name}#{thread_to_bridge.parent.name}#{thread_to_bridge.name}",
                        )

                if not new_thread:
                    # Haven't created a thread yet, try to create it from the channel
                    new_thread = await channel.create_thread(
                        name=thread_to_bridge.name,
                        reason=f"Bridged from {thread_to_bridge.guild.name}#{thread_to_bridge.parent.name}#{thread_to_bridge.name}",
                        type=discord.ChannelType.public_thread,
                    )

                if not new_thread:
                    # Failed to create a thread somehow
                    failed_channels.append(channel.id)
                    continue

                if not thread_already_existed:
                    try:
                        add_user_to_threads.append(new_thread.join())
                    except Exception:
                        pass

                    if channel_member:
                        try:
                            add_user_to_threads.append(
                                new_thread.add_user(channel_member)
                            )
                        except Exception:
                            pass

                threads_created[channel_id] = new_thread
                create_bridges.append(
                    create_bridge_and_db(thread_to_bridge, new_thread, session)
                )
                if inbound_bridges and inbound_bridges[channel_id]:
                    create_bridges.append(
                        create_bridge_and_db(new_thread, thread_to_bridge, session)
                    )
                succeeded_at_least_once = True
            await asyncio.gather(*(create_bridges + add_user_to_threads))

            session.commit()
    except SQLError:
        if interaction:
            await interaction.followup.send(
                "❌ There was an issue with the connection to the database; thread and bridge creation failed.",
                ephemeral=True,
            )
        if session:
            session.rollback()
            session.close()
        return

    if interaction:
        if succeeded_at_least_once:
            if len(failed_channels) == 0:
                response = "✅ All threads created!"
            else:
                response = (
                    "⭕ Some but not all threads were created. This may have happened because you lacked Manage Webhooks or Create Public Threads permissions. The channels this command failed for were:\n"
                    + "\n".join(
                        f"- <#{failed_channel_id}>"
                        for failed_channel_id in failed_channels
                    )
                    + "\nTrying to run this command again will duplicate threads in the channels the command _succeeded_ at. If you wish to create threads in the channels this command failed for, it would be better to do so manually one by one."
                )

            if len(bridged_threads) > 0:
                response += (
                    "\n\nNote: this channel is bridged to at least one thread, and so this command was not able to create further threads in them. The threads bridged to this channel are:"
                    + "\n".join(f"- <#{thread_id}>" for thread_id in bridged_threads)
                )
        else:
            response = "❌ Couldn't create any threads. Make sure that you and the bot have Manage Webhooks and Create Public Threads permissions in all relevant channels."

        await interaction.followup.send(response, ephemeral=True)


async def stop_auto_bridging_threads_helper(
    channel_ids_to_remove: int | Iterable[int], session: SQLSession | None = None
):
    """Remove a group of channels from the auto_bridge_thread_channels table and list.

    #### Args:
        - `channel_ids_to_remove`: The IDs of the channels to remove.
        - `session`: SQL session for accessing the database. Optional, default None.

    #### Raises:
        - `SQLError`: Something went wrong accessing or modifying the database.
    """
    types_to_validate: dict = {
        "channel_ids_to_remove": (channel_ids_to_remove, (int, Iterable))
    }
    if session:
        types_to_validate["session"] = (session, SQLSession)
    validate_types(types_to_validate)

    if not isinstance(channel_ids_to_remove, set):
        if isinstance(channel_ids_to_remove, int):
            channel_ids_to_remove = {channel_ids_to_remove}
        else:
            channel_ids_to_remove = set(channel_ids_to_remove)

    if not session:
        session = SQLSession(engine)
        close_after = True
    else:
        close_after = False

    def execute_query():
        session.execute(
            SQLDelete(DBAutoBridgeThreadChannels).where(
                DBAutoBridgeThreadChannels.channel.in_(
                    [str(id) for id in channel_ids_to_remove]
                )
            )
        )

    await sql_retry(execute_query)

    globals.auto_bridge_thread_channels -= channel_ids_to_remove

    if close_after:
        session.commit()
        session.close()


async def validate_auto_bridge_thread_channels(
    channel_ids_to_check: int | Iterable[int], session: SQLSession | None = None
):
    """Check whether each one of a list of channels are in auto_bridge_thread_channels and, if so, whether they should be and, if not, remove them from there.

    #### Args:
        - `channel_ids_to_check`: IDs of the channels to check.
        - `session`: SQL session for accessing the database. Optional, default None.

    #### Raises:
        - `SQLError`: Something went wrong accessing or modifying the database.
    """
    validate_types({"channel_ids_to_check": (channel_ids_to_check, (int, Iterable))})

    if not isinstance(channel_ids_to_check, set):
        if isinstance(channel_ids_to_check, int):
            channel_ids_to_check = {channel_ids_to_check}
        else:
            channel_ids_to_check = set(channel_ids_to_check)

    channel_ids_to_remove = {
        id
        for id in channel_ids_to_check
        if id in globals.auto_bridge_thread_channels
        and not bridges.get_inbound_bridges(id)
        and not bridges.get_outbound_bridges(id)
    }

    if len(channel_ids_to_remove) == 0:
        return

    await stop_auto_bridging_threads_helper(channel_ids_to_remove, session)


async def map_emoji_helper(
    *,
    external_emoji: discord.Emoji | discord.PartialEmoji | int | None,
    external_emoji_name: str | None = None,
    internal_emoji: discord.Emoji,
    session: SQLSession | None = None,
) -> bool:
    """Create a mapping between external and internal emoji, recording it locally and saving it in the emoji_mappings table.

    #### Args:
        - `external_emoji`: The custom emoji that is not present in any servers the bot is in, or an ID of one.
        - `internal_emoji`: An emoji the bot has in its emoji server.
        - `session`: A connection to the database. Defaults to None.

    #### Raises:
        - `UnknownDBDialectError`: Invalid database dialect registered in `settings.json` file.
        - `SQLError`: SQL statement inferred from arguments was invalid or database connection failed.
    """
    if not external_emoji or (
        not isinstance(external_emoji, int) and not external_emoji.id
    ):
        return False

    types_to_validate: dict[str, tuple] = {
        "external_emoji": (external_emoji, (discord.Emoji, discord.PartialEmoji, int)),
        "internal_emoji": (internal_emoji, discord.Emoji),
    }
    if external_emoji_name:
        types_to_validate["external_emoji_name"] = (external_emoji_name, str)
    if session:
        types_to_validate["session"] = (session, SQLSession)
    validate_types(types_to_validate)

    external_emoji_id: int | None
    if isinstance(external_emoji, int):
        external_emoji_id = external_emoji
        if not external_emoji_name:
            external_emoji_name = ""
    else:
        external_emoji_id = external_emoji.id
        if not external_emoji_id:
            return False

        full_emoji = globals.client.get_emoji(external_emoji_id)
        if full_emoji:
            external_emoji = full_emoji

        external_emoji_name = external_emoji.name
        assert isinstance(external_emoji, discord.PartialEmoji)

    globals.emoji_mappings[external_emoji_id] = internal_emoji.id

    if isinstance(external_emoji, discord.Emoji) and external_emoji.guild:
        external_emoji_server_name = external_emoji.guild.name
    else:
        external_emoji_server_name = ""

    if not session:
        session = SQLSession(engine)
        close_after = True
    else:
        close_after = False

    try:
        upsert_emoji = await sql_upsert(
            DBEmojiMap,
            {
                "external_emoji": str(external_emoji_id),
                "external_emoji_name": external_emoji_name,
                "external_emoji_server_name": external_emoji_server_name,
                "internal_emoji": str(internal_emoji.id),
            },
            {
                "internal_emoji": str(internal_emoji.id),
            },
        )

        await sql_retry(lambda: session.execute(upsert_emoji))
    except SQLError as e:
        if session:
            session.rollback()
            session.close()

        raise e

    if close_after:
        session.commit()
        session.close()

    return True


@globals.command_tree.context_menu(name="List Reactions")
async def list_reactions(interaction: discord.Interaction, message: discord.Message):
    """List all reactions and users who reacted on all sides of a bridge."""
    assert globals.client.user
    bot_user_id = globals.client.user.id

    channel = message.channel
    if not isinstance(channel, (discord.TextChannel, discord.Thread)):
        await interaction.response.send_message(
            "❌ Please run this command from a text channel or a thread.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(thinking=True, ephemeral=True)

    # Now find the list of channels that can validly reach this one via inbound chains
    reachable_channel_ids = bridges.get_reachable_channels(channel.id, "inbound")

    # This variable is where I'll gather the list of users per reaction
    # The key of each entry is a reaction emoji ID
    # The entry is a list of coroutines to get the users that reacted with that emoji
    all_reactions_async: dict[str, list[Coroutine[Any, Any, set[int]]]] = {}

    # This function gets a list of user IDs from an async iterator associated with each reaction
    async def get_users_from_iterator(
        user_iterator: AsyncIterator[discord.Member | discord.User],
    ):
        reactions: set[int] = set()
        async for user in user_iterator:
            if user.id != bot_user_id:
                reactions.add(user.id)
        return reactions

    # This function gets the equivalent ID of an emoji, matching it to an internal one if possible
    def get_mapped_emoji_id(emoji: discord.PartialEmoji | discord.Emoji | str):
        if (
            not isinstance(emoji, str)
            and emoji.id
            and (mapped_emoji_id := globals.emoji_mappings.get(emoji.id))
            and (mapped_emoji := globals.client.get_emoji(mapped_emoji_id))
        ):
            return str(mapped_emoji)

        return str(emoji)

    # First get the reactions on this message itself
    def append_users_to_reactions_list(message: discord.Message):
        for reaction in message.reactions:
            reaction_emoji_id = get_mapped_emoji_id(reaction.emoji)

            if not all_reactions_async.get(reaction_emoji_id):
                all_reactions_async[reaction_emoji_id] = []

            all_reactions_async[reaction_emoji_id].append(
                get_users_from_iterator(reaction.users())
            )

    append_users_to_reactions_list(message)

    # Then get the bridged ones
    with SQLSession(engine) as session:
        # We need to see whether this message is a bridged message and, if so, find its source
        def get_source_message_map():
            return session.scalars(
                SQLSelect(DBMessageMap).where(
                    DBMessageMap.target_message == str(message.id),
                )
            ).first()

        source_message_map: DBMessageMap | None = await sql_retry(
            get_source_message_map
        )
        if isinstance(source_message_map, DBMessageMap):
            # This message was bridged, so find the original one and then find any other bridged messages from it
            source_channel_id = int(source_message_map.source_channel)
            source_message_id = int(source_message_map.source_message)

            if source_channel_id in reachable_channel_ids:
                # The only way this would not be true would be if the bridge that brought this message here in the first place had been destroyed
                source_channel = await globals.get_channel_from_id(source_channel_id)
                if isinstance(source_channel, (discord.TextChannel, discord.Thread)):
                    source_message = await source_channel.fetch_message(
                        source_message_id
                    )
                    append_users_to_reactions_list(source_message)
        else:
            # This message is (or might be) the source
            source_message_id = message.id
            source_channel_id = channel.id

        # Then we find all messages bridged from the source
        outbound_bridges = bridges.get_outbound_bridges(source_channel_id)
        if outbound_bridges:

            def get_bridged_messages():
                return session.scalars(
                    SQLSelect(DBMessageMap).where(
                        DBMessageMap.source_message == str(source_message_id)
                    )
                )

            bridged_messages: ScalarResult[DBMessageMap] = await sql_retry(
                get_bridged_messages
            )
            for message_row in bridged_messages:
                target_channel_id = int(message_row.target_channel)
                if (
                    target_channel_id not in reachable_channel_ids
                    or not outbound_bridges.get(target_channel_id)
                ):
                    continue

                bridged_channel = await globals.get_channel_from_id(target_channel_id)
                if not isinstance(
                    bridged_channel, (discord.TextChannel, discord.Thread)
                ):
                    continue

                target_message_id = int(message_row.target_message)
                bridged_message = await bridged_channel.fetch_message(target_message_id)
                append_users_to_reactions_list(bridged_message)

    # Now we resolve all of the async calls to get the final list of users per reaction
    async def get_list_of_reacting_users(
        list_of_reacters: list[Coroutine[Any, Any, set[int]]]
    ):
        gathered_users = await asyncio.gather(*list_of_reacters)
        set_of_users: set[int] = cast(set[int], set.union(*gathered_users))
        set_of_users.discard(bot_user_id)
        return set_of_users

    list_of_reacting_users_async = [
        get_list_of_reacting_users(list_of_reacters)
        for _, list_of_reacters in all_reactions_async.items()
    ]
    list_of_reacting_users = await asyncio.gather(*list_of_reacting_users_async)

    all_reactions = {
        reaction_id: users
        for reaction_id, users in zip(
            all_reactions_async.keys(), list_of_reacting_users
        )
        if len(users) > 0
    }

    if len(all_reactions) == 0:
        await interaction.followup.send(
            "❌ This message doesn't have any reactions.",
            ephemeral=True,
        )
        return

    await interaction.followup.send(
        f"[↪](<{message.jump_url}>) This message has the following reactions:\n\n"
        + "\n\n".join(
            [
                f"{reaction_emoji_id} "
                + " ".join([f"<@{user_id}>" for user_id in reaction_user_ids])
                for reaction_emoji_id, reaction_user_ids in all_reactions.items()
            ]
        ),
        ephemeral=True,
    )
