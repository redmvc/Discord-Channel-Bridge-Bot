import asyncio
import re
from typing import Any, AsyncIterator, Coroutine, Iterable, Literal

import discord
from beartype import beartype
from sqlalchemy import Delete as SQLDelete
from sqlalchemy import ScalarResult
from sqlalchemy import Select as SQLSelect
from sqlalchemy.exc import StatementError as SQLError
from sqlalchemy.orm import Session as SQLSession

import emoji_hash_map
import globals
from bridge import Bridge, bridges
from database import (
    DBAppWhitelist,
    DBAutoBridgeThreadChannels,
    DBMessageMap,
    engine,
    sql_retry,
)
from validations import ChannelTypeError, logger, validate_channels


@globals.command_tree.command(
    name="help",
    description="Return a list of commands or detailed information about a command.",
)
@discord.app_commands.describe(command="The command to get detailed information about.")
async def help(
    interaction: discord.Interaction,
    command: (
        Literal[
            "bridge",
            "bridge_thread",
            "auto_bridge_threads",
            "demolish",
            "demolish_all",
            "whitelist",
            "map_emoji",
            "hash_server_emoji",
        ]
        | None
    ) = None,
):
    logger.debug(
        "User with ID %s ran command /help%s from channel with ID %s.",
        interaction.user.id,
        f" {command}" if command else "",
        interaction.channel_id,
    )

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
            emoji_server_commands = ", `/map_emoji`, `/hash_server_emoji`"
        else:
            emoji_server_commands = ""

        await interaction.response.send_message(
            "This bot bridges channels and threads to each other, mirroring messages sent from one to the other. When a message is bridged:"
            + "\n- its copies will show the avatar and name of the person who wrote the original message;"
            + "\n- attachments will be copied over;"
            + "\n- edits to the original message will be reflected in the bridged messages;"
            + "\n- whenever someone adds a reaction to one message the bot will add the same reaction (if it can) to all of its mirrors;"
            + "\n- and deleting the original message will delete its copies (but not vice-versa)."
            + "\nThreads created in a channel do not automatically get matched to other channels bridged to it; create and bridge them manually or use the `/bridge_thread` or `/auto_bridge_threads` command."
            + f"\n\nList of commands: `/bridge`, `/bridge_thread`, `/auto_bridge_threads`, `/demolish`, `/demolish_all`, `/whitelist`{emoji_server_commands}, `/help`.\n\nType `/help command` for detailed explanation of a command. You can also go to [the bot's documentation page](<https://discord-channel-bridge-bot.readthedocs.io/en/latest/>) for detailed explanations of all commands available.",
            ephemeral=True,
        )
    else:
        if command == "bridge":
            await interaction.response.send_message(
                "`/bridge target [direction]`"
                + "\nNecessary permissions to run command: Manage Webhooks."
                + "\n\nCreates a bridge between the current channel/thread and target channel/thread, creating a mirror of a message sent to one channel in the other. `target` must be a link to another channel or thread, its ID, or a #mention of it."
                + "\nIf `direction` isn't included, the bridge is two-way; if it's set to `inbound` it will only send messages from the target channel to the current channel; if it's set to `outbound` it will only send messages from the current channel to the target channel."
                + "\n\nNote that message mirroring goes down outbound bridge chains: if channel A has an outbound bridge to channel B and channel B has an outbound bridge to channel C, messages sent in channel A will be mirrored in both channels B and C. _However_, this does not automatically create a bridge between A and C: if e.g. the bridge between A and B is demolished, messages from A will no longer be sent to C.",
                ephemeral=True,
            )
        elif command == "bridge_thread":
            await interaction.response.send_message(
                "`/bridge_thread`"
                + "\nNecessary permissions to run command: Manage Webhooks, Create Public Threads."
                + "\n\nWhen this command is called from within a thread that is in a channel that is bridged to other channels, the bot will attempt to create new threads in all such channels and bridge them to the original one. If the original channel is bridged to threads or if you don't have create thread permissions in the other channels, this command may not run to completion."
                + "\n\nNote that this command will not create bridges down bridge chains—that is, if channel A is bridged to channel B and channel B is bridged to channel C, but A is not bridged to C, executing this command in channel A will not create a thread in channel C.",
                ephemeral=True,
            )
        elif command == "auto_bridge_threads":
            await interaction.response.send_message(
                "`/auto_bridge_threads`"
                + "\nNecessary permissions to run command: Manage Webhooks, Create Public Threads."
                + "\n\nWhen this command is called from within a channel that is bridged to other channels, the bot will enable or disable automatic thread bridging, so that any threads created in this channel will also be created across all bridges involving it. You will need to run this command from within each channel you wish to enable automatic thread creation from."
                + "\n\nNote that this command will not create bridges down bridge chains—that is, if channel A is bridged to channel B and channel B is bridged to channel C, but A is not bridged to C, threads automatically created in channel A will not have a mirror thread in channel C.",
                ephemeral=True,
            )
        elif command == "demolish":
            await interaction.response.send_message(
                "`/demolish target`"
                + "\nNecessary permissions to run command: Manage Webhooks."
                + "\n\nDestroys any existing bridges between the current and target channels/threads, making messages from either channel no longer be mirrored to the other. `target` must be a link to another channel or thread, its ID, or a #mention of it."
                + "\n\nNote that even if you recreate any of the bridges, the messages previously bridged will no longer be connected and so they will not share future reactions, edits, or deletions. Note also that this will only destroy bridges to and from the _current specific channel/thread_, not from any threads that spin off it or its parent.",
                ephemeral=True,
            )
        elif command == "demolish_all":
            await interaction.response.send_message(
                "`/demolish_all [channel_and_threads]`"
                + "\nNecessary permissions to run command: Manage Webhooks."
                + "\n\nDestroys any existing bridges involving the current channel or thread, making messages from it no longer be mirrored to other channels and making other channels' messages no longer be mirrored to it."
                + "\n\nIf you don't include `channel_and_threads` or set it to `False`, this will _only_ demolish bridges involving the _current specific channel/thread_. If instead you set `channel_and_threads` to `True`, this will demolish _all_ bridges involving the current channel/thread, its parent channel if it's a thread, and all of its or its parent channel's threads."
                + "\n\nNote that even if you recreate any of the bridges, the messages previously bridged will no longer be connected and so they will not share future reactions, edits, or deletions.",
                ephemeral=True,
            )
        elif command == "whitelist":
            await interaction.response.send_message(
                "`/whitelist @bot [@bot_2 [@bot_3 ...]]`"
                + "\nNecessary permissions to run command: Manage Webhooks."
                + "\n\nAllows or disallows bridging messages sent by one or more bots from the current channel. Only works through outbound bridges: you can whitelist a bot so that messages sent by it in the current channel are bridged to other channels, but that will not make messages by that bot be bridged to the current channel if the bot is not whitelisted in the source channel."
                + "\n\nNote that this command is a toggle, so running it again will remove a bot from the blacklist. It also goes on a per-bot basis, so if you run `/whitelist @bot` then `/whitelist @bot @bot_2` then `@bot` will not be whitelisted but `@bot_2` will.",
                ephemeral=True,
            )
        elif command == "map_emoji" and interaction_from_emoji_server:
            await interaction.response.send_message(
                "`/map_emoji :internal_emoji: :external_emoji: [:external_emoji_2: [:external_emoji_3: ...]]`"
                + "\nNecessary permissions to run command: Create Expressions, Manage Expressions."
                + "\n\nCreates an internal mapping between an emoji from an external server which the bot doesn't have access to and an emoji stored in the bot's emoji server, so that they are considered equivalent by the bot when bridging reactions. You can also pass multiple external emoji separated by spaces to map all of them to the same internal one.",
                ephemeral=True,
            )
        elif command == "hash_server_emoji" and interaction_from_emoji_server:
            await interaction.response.send_message(
                "`/hash_server_emoji [server_id]`"
                + "\nNecessary permissions to run command: Create Expressions, Manage Expressions."
                + "\n\nLoads all of the emoji of a given server into the bot's hash map for equivalence matching. If `server_id` is not provided, will loas the emoji from every server the bot is connected to into the map.",
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
async def bridge(
    interaction: discord.Interaction,
    target: str,
    direction: Literal["outbound", "inbound"] | None = None,
):
    logger.debug(
        "User with ID %s ran command /bridge %s%s from channel with ID %s. Interaction ID: %s.",
        interaction.user.id,
        target,
        f" {direction}" if direction else "",
        interaction.channel_id,
        interaction.id,
    )

    message_channel = interaction.channel
    if not isinstance(message_channel, (discord.TextChannel, discord.Thread)):
        await interaction.response.send_message(
            "❌ Please run this command from a text channel or a thread.",
            ephemeral=True,
        )
        return

    target_channel = await mention_to_channel(target)
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

    join_threads: list[Coroutine[Any, Any, None]] = []
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
            create_bridges: list[Coroutine[Any, Any, Bridge]] = []
            if direction != "inbound":
                create_bridges.append(
                    bridges.create_bridge(
                        source=message_channel, target=target_channel, session=session
                    )
                )
            if direction != "outbound":
                create_bridges.append(
                    bridges.create_bridge(
                        source=target_channel, target=message_channel, session=session
                    )
                )

            await asyncio.gather(*create_bridges)
            session.commit()
    except Exception as e:
        if session:
            session.rollback()
            session.close()

        if isinstance(e, SQLError):
            await interaction.followup.send(
                "❌ There was an issue with the connection to the database; bridge creation failed.",
                ephemeral=True,
            )
            logger.warning("An SQL error occurred while running command /bridge: %s", e)
        else:
            await interaction.followup.send(
                "❌ An unknown error occurred.",
                ephemeral=True,
            )
            logger.error(
                "An unknown error occurred while running command /bridge: %s", e
            )

        raise

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

    logger.debug("Call to /bridge with interaction ID %s successful.", interaction.id)


@discord.app_commands.default_permissions(
    manage_webhooks=True, create_public_threads=True
)
@discord.app_commands.guild_only()
@globals.command_tree.command(
    name="bridge_thread",
    description="Create threads across the bridge matching this one and bridge them.",
)
async def bridge_thread(interaction: discord.Interaction):
    logger.debug(
        "User with ID %s ran command /bridge_thread from channel with ID %s. Interaction ID: %s.",
        interaction.user.id,
        interaction.channel_id,
        interaction.id,
    )

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

    try:
        await bridge_thread_helper(message_thread, interaction.user.id, interaction)
    except Exception as e:
        if isinstance(e, SQLError):
            await interaction.followup.send(
                "❌ There was an issue with the connection to the database; thread and bridge creation failed.",
                ephemeral=True,
            )
            logger.warning(
                "An SQL error occurred while running command /bridge_thread: %s",
                e,
            )
        else:
            await interaction.followup.send(
                "❌ An unknown error occurred.",
                ephemeral=True,
            )
            logger.error(
                "An unknown error occurred while running command /bridge_thread: %s",
                e,
            )

        raise

    logger.debug(
        "Call to /bridge_thread with interaction ID %s successful.", interaction.id
    )


@discord.app_commands.default_permissions(
    manage_webhooks=True, create_public_threads=True
)
@discord.app_commands.guild_only()
@globals.command_tree.command(
    name="auto_bridge_threads",
    description="Enable or disable automatic thread bridging from this channel.",
)
async def auto_bridge_threads(interaction: discord.Interaction):
    logger.debug(
        "User with ID %s ran command /auto_bridge_threads from channel with ID %s. Interaction ID: %s.",
        interaction.user.id,
        interaction.channel_id,
        interaction.id,
    )

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
            bridge_webhook = await bridge.webhook
            if target_id == bridge_webhook.channel_id:
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
                await sql_retry(
                    lambda: session.add(
                        DBAutoBridgeThreadChannels(channel=str(message_channel.id))
                    )
                )
                globals.auto_bridge_thread_channels.add(message_channel.id)

                response = "✅ Threads will now be automatically created across bridges when they are created in this channel."
            else:
                await stop_auto_bridging_threads_helper(message_channel.id, session)

                response = "✅ Threads will no longer be automatically created across bridges when they are created in this channel."

            session.commit()
    except Exception as e:
        if session:
            session.rollback()
            session.close()

        if isinstance(e, SQLError):
            await interaction.followup.send(
                "❌ There was an issue with the connection to the database; setting or unsetting automatic thread creation across bridges failed.",
                ephemeral=True,
            )
            logger.warning(
                "An SQL error occurred while running command /auto_bridge_threads: %s",
                e,
            )
        else:
            await interaction.followup.send(
                "❌ An unknown error occurred.",
                ephemeral=True,
            )
            logger.error(
                "An error occurred while running command /auto_bridge_threads: %s", e
            )

        raise

    await interaction.followup.send(response, ephemeral=True)

    logger.debug(
        "Call to /auto_bridge_threads with interaction ID %s successful.",
        interaction.id,
    )


@beartype
async def mention_to_channel(link_or_mention: str) -> globals.DiscordChannel | None:
    """Return the channel referenced by a channel mention or a Discord link to a channel, if it's valid, or None if it isn't.

    Parameters
    ----------
    link_or_mention : str
        Either a mention of a Discord channel with format "<#channel_id>" or a Discord link to one with format "https://discord.com/channels/server_id/channel_id".

    Returns
    -------
    :class:`~discord.abc.GuildChannel` | :class:`~discord.abc.PrivateChannel` | :class:`~discord.Thread` | :class:`~discord.PartialMessageable` | None
    """
    if link_or_mention.startswith("https://discord.com/channels"):
        try:
            while link_or_mention.endswith("/"):
                link_or_mention = link_or_mention[:-1]

            channel_id = int(link_or_mention.rsplit("/")[-1])
        except ValueError:
            return None
    else:
        try:
            channel_id = int(
                link_or_mention.replace("<", "").replace(">", "").replace("#", "")
            )
        except ValueError:
            return None

    return await globals.get_channel_from_id(channel_id)


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
    logger.debug(
        "User with ID %s ran command /demolish %s from channel with ID %s. Interaction ID: %s.",
        interaction.user.id,
        target,
        interaction.channel_id,
        interaction.id,
    )

    message_channel = interaction.channel
    if not isinstance(message_channel, (discord.TextChannel, discord.Thread)):
        await interaction.response.send_message(
            "❌ Please run this command from a text channel or a thread.",
            ephemeral=True,
        )
        return

    target_channel = await mention_to_channel(target)
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

    try:
        with SQLSession(engine) as session:
            await bridges.demolish_bridges(
                source_channel=message_channel,
                target_channel=target_channel,
                session=session,
            )
            await validate_auto_bridge_thread_channels(
                {message_channel.id, target_channel.id}, session
            )

            session.commit()
    except Exception as e:
        if isinstance(e, SQLError):
            await interaction.followup.send(
                "❌ There was an issue with the connection to the database; thread and bridge creation failed.",
                ephemeral=True,
            )
            logger.warning(
                "An SQL error occurred while running command /demolish: %s", e
            )
        else:
            await interaction.followup.send(
                "❌ An unknown error occurred.",
                ephemeral=True,
            )
            logger.error(
                "An unknown error occurred while running command /demolish: %s", e
            )

        raise

    await interaction.followup.send(
        "✅ Bridges demolished!",
        ephemeral=True,
    )

    logger.debug("Call to /demolish with interaction ID %s successful.", interaction.id)


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
    interaction: discord.Interaction,
    channel_and_threads: bool | None = None,
):
    logger.debug(
        "User with ID %s ran command /demolish_all%s from channel with ID %s. Interaction ID: %s.",
        interaction.user.id,
        " True" if channel_and_threads else "",
        interaction.channel_id,
        interaction.id,
    )

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
    bridges_being_demolished: list[Coroutine[Any, Any, None]] = []
    session = None
    exceptions: set[int] = set()
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

                if outbound_bridges:
                    for target_id in outbound_bridges.keys():
                        target_channel = await globals.get_channel_from_id(target_id)
                        if (
                            not isinstance(
                                target_channel,
                                (discord.TextChannel, discord.Thread),
                            )
                            or not (
                                target_channel_member
                                := await globals.get_channel_member(
                                    target_channel,
                                    interaction.user.id,
                                )
                            )
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

                bridges_being_demolished.append(
                    bridges.demolish_bridges(
                        source_channel=channel_to_demolish_id,
                        session=session,
                    )
                )
                bridges_being_demolished.append(
                    bridges.demolish_bridges(
                        target_channel=channel_to_demolish_id,
                        session=session,
                    )
                )

            await asyncio.gather(*bridges_being_demolished)
            await validate_auto_bridge_thread_channels(channels_affected, session)

            session.commit()
    except Exception as e:
        if session:
            session.rollback()
            session.close()

        if isinstance(e, SQLError):
            await interaction.followup.send(
                "❌ There was an issue with the connection to the database; bridge demolition failed.",
                ephemeral=True,
            )
            logger.warning(
                "An SQL error occurred while running command /demolish_all: %s", e
            )
        else:
            await interaction.followup.send(
                "❌ An unknown error occurred.",
                ephemeral=True,
            )
            logger.error(
                "An unknown error occurred while running command /demolish_all: %s", e
            )

        raise

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

    logger.debug(
        "Call to /demolish_all with interaction ID %s successful.", interaction.id
    )


@discord.app_commands.default_permissions(manage_webhooks=True)
@discord.app_commands.guild_only()
@globals.command_tree.command(
    name="whitelist",
    description="Add or remove bots or applications to or from a whitelist for the current channel.",
)
@discord.app_commands.describe(
    apps="Mentions or IDs of the app or apps to add to or remove from the whitelist."
)
async def whitelist(interaction: discord.Interaction, apps: str):
    logger.debug(
        "User with ID %s ran command /whitelist %s from channel with ID %s. Interaction ID: %s.",
        interaction.user.id,
        apps,
        interaction.channel_id,
        interaction.id,
    )

    channel = interaction.channel
    if not channel or not isinstance(channel, (discord.TextChannel, discord.Thread)):
        await interaction.response.send_message(
            "❌ Please run this command from a Text Channel or Thread.",
            ephemeral=True,
        )
        return

    if not channel.permissions_for(channel.guild.me).manage_webhooks:
        await interaction.response.send_message(
            "❌ I don't have Manage Webhooks permissions in this channel.",
            ephemeral=True,
        )
        return

    try:
        apps_to_toggle = set(
            [
                int(app_id)
                for app_id in apps.replace("<", "")
                .replace("@", "")
                .replace(">", "")
                .split()
            ]
        )
    except ValueError:
        await interaction.response.send_message("❌ App IDs not valid.", ephemeral=True)
        return

    channel_whitelist: set[int] | None = globals.per_channel_whitelist.get(channel.id)
    if not channel_whitelist:
        channel_whitelist = set()

    outbound_bridges = bridges.get_outbound_bridges(channel)
    if not outbound_bridges and not any(
        [app_id in channel_whitelist for app_id in apps_to_toggle]
    ):
        # None of the App IDs passed was already in the whitelist and there isn't an outbound bridge
        await interaction.response.send_message(
            "❌ This channel does not have any outbound bridges.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(thinking=True, ephemeral=True)

    apps_to_add: set[int] = set()
    apps_to_remove: set[int] = set()
    for app_id in apps_to_toggle:
        if app_id in channel_whitelist:
            apps_to_remove.add(app_id)
        else:
            member = await globals.get_channel_member(channel, app_id)
            if not member:
                await interaction.followup.send(
                    "❌ At least one app passed is not a member of the current channel.",
                    ephemeral=True,
                )
                return
            else:
                apps_to_add.add(app_id)

    session = None
    response: list[str] = []
    try:
        channel_id_str = str(channel.id)
        with SQLSession(engine) as session:
            run_queries: list[Coroutine[Any, Any, Any]] = []
            if len(apps_to_add) > 0:
                run_queries.append(
                    sql_retry(
                        lambda: session.add_all(
                            [
                                DBAppWhitelist(
                                    channel=channel_id_str,
                                    application=str(app_id),
                                )
                                for app_id in apps_to_add
                            ]
                        )
                    )
                )

                apps_to_add_str = ", ".join([f"<@{app_id}>" for app_id in apps_to_add])
                response.append(
                    f"✅ Added the following app(s) to this channel's whitelist: {apps_to_add_str}."
                )

            if len(apps_to_remove) > 0:
                remove_apps = SQLDelete(DBAppWhitelist).where(
                    DBAppWhitelist.channel == channel_id_str,
                    DBAppWhitelist.application.in_(
                        [str(app_id) for app_id in apps_to_remove]
                    ),
                )
                run_queries.append(sql_retry(lambda: session.execute(remove_apps)))

                apps_to_remove_str = ", ".join(
                    [f"<@{app_id}>" for app_id in apps_to_remove]
                )
                response.append(
                    f"✅ Removed the following app(s) from this channel's whitelist: {apps_to_remove_str}."
                )

            await asyncio.gather(*run_queries)
            session.commit()

            if not globals.per_channel_whitelist.get(channel.id):
                globals.per_channel_whitelist[channel.id] = set()
            globals.per_channel_whitelist[channel.id] = (
                globals.per_channel_whitelist[channel.id].union(apps_to_add)
                - apps_to_remove
            )
            if len(globals.per_channel_whitelist[channel.id]) == 0:
                del globals.per_channel_whitelist[channel.id]
    except Exception as e:
        if session:
            session.rollback()
            session.close()

        if isinstance(e, SQLError):
            await interaction.followup.send(
                "❌ There was a problem accessing the database.",
                ephemeral=True,
            )
            logger.warning(
                "An SQL error occurred while running command whitelist: %s", e
            )
        else:
            await interaction.followup.send(
                "❌ An unknown error occurred.",
                ephemeral=True,
            )
            logger.error(
                "An unknown error occurred while running command whitelist: %s", e
            )

        raise

    await interaction.followup.send("\n".join(response), ephemeral=True)

    logger.debug(
        "Call to /whitelist with interaction ID %s successful.", interaction.id
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
    internal_emoji_id_str="The emoji from this server to map the external emoji to.",
    external_emojis="The emoji/emojis from another server.",
)
async def map_emoji(
    interaction: discord.Interaction,
    internal_emoji_id_str: str,
    external_emojis: str,
):
    logger.debug(
        "User with ID %s ran command /map_emoji %s %s from channel with ID %s and arguments. Interaction ID: %s.",
        interaction.user.id,
        internal_emoji_id_str,
        external_emojis,
        interaction.channel_id,
        interaction.id,
    )

    if not globals.settings.get("emoji_server_id"):
        await interaction.response.send_message(
            "❌ Bot doesn't have an emoji server registered.", ephemeral=True
        )
        return

    external_emojis_set: set[tuple[str, str | int]] = set(
        re.findall(r"<(a?:[^:]+):(\d+)>", external_emojis)
    )
    internal_emoji_split = re.findall(r"<(a?:[^:]+):(\d+)>", internal_emoji_id_str)[0]

    try:
        external_emojis_set = {(name, int(id)) for name, id in external_emojis_set}
        internal_emoji_id = int(internal_emoji_split[1])
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

    session = None
    try:
        with SQLSession(engine) as session:
            image_hash = await emoji_hash_map.map.get_hash(
                emoji=internal_emoji, session=session
            )
            map_emojis = await asyncio.gather(
                *[
                    emoji_hash_map.map.map_emoji(
                        external_emoji_id=id,
                        external_emoji_name=name,
                        internal_emoji=internal_emoji,
                        image_hash=image_hash,
                        session=session,
                    )
                    for name, id in external_emojis_set
                ]
            )
    except Exception as e:
        if session:
            session.rollback()
            session.close()

        if isinstance(e, SQLError):
            await interaction.followup.send(
                f"❌ There was a database error trying to map emoji to {str(internal_emoji)}.",
                ephemeral=True,
            )
            logger.warning(
                "An SQL error occurred when running command /map_emoji: %s", e
            )
        else:
            await interaction.followup.send(
                f"❌ There was an unknown error trying to map emoji to {str(internal_emoji)}.",
                ephemeral=True,
            )
            logger.error(
                "An unknown error occurred when running command /map_emoji: %s", e
            )

        raise

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

    logger.debug(
        "Call to /map_emoji with interaction ID %s successful.", interaction.id
    )


@discord.app_commands.default_permissions(
    create_expressions=True, manage_expressions=True
)
@globals.command_tree.command(
    name="hash_server_emoji",
    description="Load all of the emoji of a server or servers into the bot's hash map for equivalence matching.",
    guild=globals.emoji_server,
)
@discord.app_commands.rename(
    server_id_str="server",
)
@discord.app_commands.describe(
    server_id_str="The ID of the server to load.",
)
async def hash_server_emoji(
    interaction: discord.Interaction,
    server_id_str: str | None = None,
):
    logger.debug(
        "User with ID %s ran command /hash_server_emoji%s from channel with ID %s. Interaction ID: %s.",
        interaction.user.id,
        f" {server_id_str}" if server_id_str else "",
        interaction.channel_id,
        interaction.id,
    )

    if server_id_str:
        try:
            server_id = int(server_id_str)
        except ValueError:
            await interaction.response.send_message(
                "❌ Server ID passed is not a valid numerical ID.", ephemeral=True
            )
            return

        server = globals.client.get_guild(server_id)
        if not server:
            await interaction.response.send_message(
                "❌ Server ID passed is not an ID of a server the bot is in.",
                ephemeral=True,
            )
            return

        message = f"Are you sure you want to hash all of the emoji in {server.name}? This may take a bit and make the bot unresponsive in the meantime."
    elif len(globals.client.guilds) == 0:
        await interaction.response.send_message(
            "❌ This bot is not in any servers.",
            ephemeral=True,
        )
        return
    else:
        server = None
        message = f"Are you **sure** you want to hash the emoji of all {len(globals.client.guilds)} servers this bot is in? This may take multiple minutes and make the bot unresponsive in the meantime."

    view = discord.ui.View()
    view.add_item(ConfirmHashServer(interaction, server))
    view.add_item(CancelHashServer(interaction))

    await interaction.response.send_message(message, view=view, ephemeral=True)


class CancelHashServer(discord.ui.Button[Any]):
    def __init__(self, original_interaction: discord.Interaction):
        super().__init__(label="No", style=discord.ButtonStyle.grey)
        self._original_interaction = original_interaction

    async def callback(self, interaction: discord.Interaction):
        await self._original_interaction.edit_original_response(
            view=None, content="Request cancelled."
        )
        logger.debug(
            "Call to /hash_server_emoji with interaction ID %s cancelled.",
            interaction.id,
        )


class ConfirmHashServer(discord.ui.Button[Any]):
    def __init__(
        self,
        original_interaction: discord.Interaction,
        server_to_hash: discord.Guild | None = None,
    ):
        if server_to_hash:
            super().__init__(label="Yes", style=discord.ButtonStyle.red)
        else:
            super().__init__(label="Yes", style=discord.ButtonStyle.danger, emoji="⚠️")

        self._original_interaction = original_interaction
        self._server_to_hash_id = server_to_hash.id if server_to_hash else None

    async def callback(self, interaction: discord.Interaction):
        # await self._original_interaction.delete_original_response()
        await self._original_interaction.edit_original_response(
            view=None, content="Hashing..."
        )
        await interaction.response.defer(thinking=True, ephemeral=True)
        try:
            await emoji_hash_map.map.load_server_emoji(self._server_to_hash_id)
        except ValueError:
            await interaction.followup.send(
                "❌ Server ID passed is not an ID of a server the bot is in.",
                ephemeral=True,
            )
            return
        except Exception as e:
            if isinstance(e, SQLError):
                await interaction.followup.send(
                    "❌ There was a problem with the database connection.",
                    ephemeral=True,
                )
                logger.warning(
                    "An SQL error occurred while trying to hash a server: %s", e
                )
            else:
                await interaction.followup.send(
                    "❌ An unknown error occurred.",
                    ephemeral=True,
                )
                logger.error(
                    "An unknown error occurred while trying to hash a server: %s", e
                )
            raise

        await interaction.followup.send("✅ Successfully hashed emoji!", ephemeral=True)

        logger.debug(
            "Call to /hash_server_emoji with interaction ID %s successful.",
            interaction.id,
        )


@beartype
async def bridge_thread_helper(
    thread_to_bridge: discord.Thread,
    user_id: int,
    interaction: discord.Interaction | None = None,
):
    """Create threads matching the current one across bridges.

    Parameters
    ----------
    thread_to_bridge : :class:`~discord.Thread`
        The thread to bridge.
    user_id : int
        The ID of the user that created the thread.
    interaction : :class:`~discord.Interaction` | None, optional
        The interaction that called this function, if any. Defaults to None.
    """
    thread_parent = await globals.get_channel_parent(thread_to_bridge)

    outbound_bridges = bridges.get_outbound_bridges(thread_parent.id)
    inbound_bridges = bridges.get_inbound_bridges(thread_parent.id)
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
        bridge_webhook = await bridge.webhook
        if target_id == bridge_webhook.channel_id:
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
                await thread_parent.fetch_message(thread_to_bridge.id)
                select_message_map: SQLSelect[tuple[DBMessageMap]] = SQLSelect(
                    DBMessageMap
                ).where(DBMessageMap.target_message == str(thread_to_bridge.id))
                source_starting_message: DBMessageMap | None = await sql_retry(
                    lambda: session.scalars(select_message_map).first()
                )
                if isinstance(source_starting_message, DBMessageMap):
                    # The message that's starting this thread is bridged
                    source_channel_id = int(source_starting_message.source_channel)
                    source_message_id = int(source_starting_message.source_message)
                    matching_starting_messages[source_channel_id] = source_message_id
                else:
                    source_channel_id = thread_parent.id
                    source_message_id = thread_to_bridge.id

                select_message_map: SQLSelect[tuple[DBMessageMap]] = SQLSelect(
                    DBMessageMap
                ).where(DBMessageMap.source_message == str(source_message_id))
                target_starting_messages: ScalarResult[DBMessageMap] = await sql_retry(
                    lambda: session.scalars(select_message_map)
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
            bridged_threads: list[int] = []
            failed_channels: list[int] = []

            create_bridges: list[Coroutine[Any, Any, Bridge]] = []
            add_user_to_threads: list[Coroutine[Any, Any, None]] = []
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
                            reason=f"Bridged from {thread_to_bridge.guild.name}#{thread_parent.name}#{thread_to_bridge.name}",
                        )

                if not new_thread:
                    # Haven't created a thread yet, try to create it from the channel
                    new_thread = await channel.create_thread(
                        name=thread_to_bridge.name,
                        reason=f"Bridged from {thread_to_bridge.guild.name}#{thread_parent.name}#{thread_to_bridge.name}",
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
                    bridges.create_bridge(
                        source=thread_to_bridge, target=new_thread, session=session
                    )
                )
                if inbound_bridges and inbound_bridges[channel_id]:
                    create_bridges.append(
                        bridges.create_bridge(
                            source=new_thread, target=thread_to_bridge, session=session
                        )
                    )
                succeeded_at_least_once = True
            await asyncio.gather(*(create_bridges + add_user_to_threads))

            session.commit()
    except Exception:
        if session:
            session.rollback()
            session.close()

        raise

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


@beartype
async def stop_auto_bridging_threads_helper(
    channel_ids_to_remove: int | Iterable[int],
    session: SQLSession | None = None,
):
    """Remove a group of channels from the auto_bridge_thread_channels table and list.

    Parameters
    ----------
    channel_ids_to_remove : int | Iterable[int]
        The IDs of the channels to remove.
    session : :class:`~sqlalchemy.orm.Session` | None, optional
        An SQLAlchemy ORM Session connecting to the database. Defaults to None, in which case a new one will be created.

    Raises
    ------
    `sqlalchemy.exc.StatementError`
        Something went wrong accessing or modifying the database.
    """
    if not isinstance(channel_ids_to_remove, set):
        if isinstance(channel_ids_to_remove, int):
            channel_ids_to_remove = {channel_ids_to_remove}
        else:
            channel_ids_to_remove = set(channel_ids_to_remove)

    close_after = False
    try:
        if not session:
            session = SQLSession(engine)
            close_after = True

        await sql_retry(
            lambda: session.execute(
                SQLDelete(DBAutoBridgeThreadChannels).where(
                    DBAutoBridgeThreadChannels.channel.in_(
                        [str(id) for id in channel_ids_to_remove]
                    )
                )
            )
        )

        globals.auto_bridge_thread_channels -= channel_ids_to_remove
    except Exception:
        if close_after and session:
            session.rollback()
            session.close()

        raise

    if close_after:
        session.commit()
        session.close()


@beartype
async def validate_auto_bridge_thread_channels(
    channel_ids_to_check: int | Iterable[int],
    session: SQLSession | None = None,
):
    """Check whether each one of a list of channels are in auto_bridge_thread_channels and, if so, whether they should be and, if not, remove them from there.

    Parameters
    ----------
    channel_ids_to_check : int | Iterable[int]
        The IDs of the channels to check.
    session : :class:`~sqlalchemy.orm.Session` | None, optional
        An SQLAlchemy ORM Session connecting to the database. Defaults to None, in which case a new one will be created.

    Raises
    ------
    `sqlalchemy.exc.StatementError`
        Something went wrong accessing or modifying the database.
    """
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


@discord.app_commands.default_permissions(read_messages=True)
@discord.app_commands.guild_only()
@globals.command_tree.context_menu(name="List Reactions")
async def list_reactions(interaction: discord.Interaction, message: discord.Message):
    """List all reactions and users who reacted on all sides of a bridge."""
    logger.debug(
        "User with ID %s requested a list of reactions attached to message with ID %s in channel with ID %s. Interaction ID: %s.",
        interaction.user.id,
        message.id,
        interaction.channel_id,
        interaction.id,
    )

    if not globals.client.user:
        await interaction.response.send_message(
            "❌ Bot is not logged in.",
            ephemeral=True,
        )
        logger.warning(
            "'List Reactions' command was called before the bot was logged in."
        )
        return

    if not (message_server := message.guild):
        await interaction.response.send_message(
            "❌ Please run this command from a text channel or a thread off one in a server the bot is in.",
            ephemeral=True,
        )
        return

    bot_user_id = globals.client.user.id
    if not (bot_member := message_server.get_member(bot_user_id)):
        await interaction.response.send_message(
            "❌ The bot is not in this server.",
            ephemeral=True,
        )
        return

    channel = message.channel
    if not channel.permissions_for(bot_member).read_messages:
        await interaction.response.send_message(
            "❌ The bot does not have access to this message.",
            ephemeral=True,
        )
        return

    try:
        validate_channels(channel=channel, log_error=False)
    except ChannelTypeError:
        await interaction.response.send_message(
            "❌ Please run this command from a text channel or a thread off one.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(thinking=True, ephemeral=True)

    # Now find the list of channels that can validly reach this one via inbound chains
    reachable_channel_ids = await bridges.get_reachable_channels(channel.id, "inbound")

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
    async def get_mapped_emoji_id(emoji: discord.PartialEmoji | discord.Emoji | str):
        if isinstance(emoji, str):
            return emoji

        if not emoji.id:
            # Non-custom emoji
            return str(emoji)

        # Custom emoji
        if mapped_emoji := emoji_hash_map.map.get_accessible_emoji(emoji.id):
            # If there is an emoji I have access to that matches this one, return it
            return str(mapped_emoji)
        elif not globals.emoji_server:
            # I don't have an emoji server to copy the emoji into so I'll just return its string
            return str(emoji)

        # Try to copy this emoji into my emoji server
        if isinstance(emoji, discord.PartialEmoji):
            copied_emoji = await emoji_hash_map.map.copy_emoji_into_server(
                emoji_to_copy=emoji, session=session
            )
        else:
            copied_emoji = await emoji_hash_map.map.copy_emoji_into_server(
                emoji_to_copy_id=emoji.id,
                emoji_to_copy_name=emoji.name,
                session=session,
            )
        if copied_emoji:
            return str(copied_emoji)

        # Failed to copy the emoji
        return str(emoji)

    # First get the reactions on this message itself
    async def append_users_to_reactions_list(message: discord.Message):
        for reaction in message.reactions:
            reaction_emoji_id = await get_mapped_emoji_id(reaction.emoji)

            if not all_reactions_async.get(reaction_emoji_id):
                all_reactions_async[reaction_emoji_id] = []

            all_reactions_async[reaction_emoji_id].append(
                get_users_from_iterator(reaction.users())
            )

    try:
        await append_users_to_reactions_list(message)
    except discord.Forbidden:
        await interaction.followup.send(
            "❌ The bot does not have access to this message.",
            ephemeral=True,
        )
        return

    # Then get the bridged ones
    session = None
    at_least_one_inaccessible_bridge = False
    try:
        with SQLSession(engine) as session:
            # We need to see whether this message is a bridged message and, if so, find its source
            select_message_map: SQLSelect[tuple[DBMessageMap]] = SQLSelect(
                DBMessageMap
            ).where(
                DBMessageMap.target_message == str(message.id),
            )
            source_message_map: DBMessageMap | None = await sql_retry(
                lambda: session.scalars(select_message_map).first()
            )
            if isinstance(source_message_map, DBMessageMap):
                # This message was bridged, so find the original one and then find any other bridged messages from it
                source_channel_id = int(source_message_map.source_channel)
                source_message_id = int(source_message_map.source_message)

                if source_channel_id in reachable_channel_ids:
                    # The only way this would not be true would be if the bridge that brought this message here in the first place had been destroyed
                    source_channel = await globals.get_channel_from_id(
                        source_channel_id
                    )
                    if isinstance(
                        source_channel,
                        (discord.TextChannel, discord.Thread),
                    ):
                        try:
                            source_message = await source_channel.fetch_message(
                                source_message_id
                            )
                            await append_users_to_reactions_list(source_message)
                        except discord.Forbidden:
                            at_least_one_inaccessible_bridge = True
            else:
                # This message is (or might be) the source
                source_message_id = message.id
                source_channel_id = channel.id

            # Then we find all messages bridged from the source
            outbound_bridges = bridges.get_outbound_bridges(source_channel_id)
            if outbound_bridges:
                select_message_map: SQLSelect[tuple[DBMessageMap]] = SQLSelect(
                    DBMessageMap
                ).where(DBMessageMap.source_message == str(source_message_id))
                bridged_messages: ScalarResult[DBMessageMap] = await sql_retry(
                    lambda: session.scalars(select_message_map)
                )
                for message_row in bridged_messages:
                    target_channel_id = int(message_row.target_channel)
                    if (
                        target_channel_id not in reachable_channel_ids
                        or not outbound_bridges.get(target_channel_id)
                    ):
                        continue

                    bridged_channel = await globals.get_channel_from_id(
                        target_channel_id
                    )
                    if not isinstance(
                        bridged_channel,
                        (discord.TextChannel, discord.Thread),
                    ):
                        continue

                    target_message_id = int(message_row.target_message)
                    bridged_message = await bridged_channel.fetch_message(
                        target_message_id
                    )
                    try:
                        await append_users_to_reactions_list(bridged_message)
                    except discord.Forbidden:
                        at_least_one_inaccessible_bridge = True
    except Exception as e:
        if session:
            session.rollback()
            session.close()

        if isinstance(e, SQLError):
            await interaction.followup.send(
                "❌ There was a problem accessing the database.",
                ephemeral=True,
            )
            logger.warning(
                "There was a problem accessing the database when trying to list reactions: %s",
                e,
            )
        else:
            await interaction.followup.send(
                "❌ An unknown error occurred.",
                ephemeral=True,
            )
            logger.error(
                "An unknown error occurred when trying to list reactions: %s", e
            )
        return

    # Now we resolve all of the async calls to get the final list of users per reaction
    try:

        async def get_list_of_reacting_users(
            list_of_reacters: list[Coroutine[Any, Any, set[int]]],
        ) -> set[int]:
            gathered_users = await asyncio.gather(*list_of_reacters)
            set_of_users: set[int] = set().union(*gathered_users)
            set_of_users.discard(bot_user_id)
            return set_of_users

        ordered_reaction_ids = []
        list_of_reacting_users_async = []
        for reaction_id, list_of_reacters in all_reactions_async.items():
            ordered_reaction_ids.append(reaction_id)
            list_of_reacting_users_async.append(
                get_list_of_reacting_users(list_of_reacters)
            )
        list_of_reacting_users = await asyncio.gather(*list_of_reacting_users_async)

        all_reactions = {
            reaction_id: users
            for reaction_id, users in zip(ordered_reaction_ids, list_of_reacting_users)
            if len(users) > 0
        }
    except Exception as e:
        if isinstance(e, discord.errors.HTTPException):
            await interaction.followup.send(
                "❌ There was a problem requesting the reactions from the Discord API. Please make sure that the bot has access to the channel you are trying to run this command from or try again in a few minutes.",
                ephemeral=True,
            )
            logger.warning(
                "There was a problem requesting reactions from the Discord API when running /list_reactions: %s",
                e,
            )
        else:
            await interaction.followup.send(
                "❌ An unknown error occurred.",
                ephemeral=True,
            )
            logger.error(
                "An unknown error occurred when running /list_reactions: %s", e
            )
        raise

    if len(all_reactions) == 0:
        if not at_least_one_inaccessible_bridge:
            followup_message = (
                f"[↪](<{message.jump_url}>) This message doesn't have any reactions."
            )
        else:
            followup_message = f"[↪](<{message.jump_url}>) Did not find any reactions to this message but at least one of its bridged versions could not be accessed by the bot."
        await interaction.followup.send(followup_message, ephemeral=True)

        logger.debug(
            "Reaction list request with interaction ID %s successful.", interaction.id
        )
        return

    followup_message = (
        f"[↪](<{message.jump_url}>) This message has the following reactions:\n\n"
        + "\n\n".join(
            [
                f"{reaction_emoji_id} "
                + " ".join([f"<@{user_id}>" for user_id in reaction_user_ids])
                for reaction_emoji_id, reaction_user_ids in all_reactions.items()
            ]
        )
    )
    if at_least_one_inaccessible_bridge:
        followup_message = f"{followup_message}\n\nHowever, at least one of the message's bridged versions could not be accessed by the bot, so there might be more reactions than just these."
    await interaction.followup.send(followup_message, ephemeral=True)

    logger.debug(
        "Reaction list request with interaction ID %s successful.", interaction.id
    )
