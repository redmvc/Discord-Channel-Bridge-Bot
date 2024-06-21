import discord
import mysql.connector
import mysql.connector.abstracts

import globals
from bridge import Bridge, bridges


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

    assert isinstance(interaction.user, discord.Member)
    if (
        not message_channel.permissions_for(interaction.user).manage_webhooks
        or not target_channel.permissions_for(interaction.user).manage_webhooks
    ):
        await interaction.response.send_message(
            "Please make sure you have 'Manage Webhooks' permission in both this and target channels."
        )
        return

    assert globals.conn
    cur = globals.conn.cursor()

    await create_bridge_and_db(message_channel, target_channel, None, cur)
    await create_bridge_and_db(target_channel, message_channel, None, cur)

    globals.conn.commit()
    cur.close()

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

    assert globals.conn
    cur = globals.conn.cursor()

    await demolish_bridges(message_channel, target_channel)
    cur.execute(
        """
        DELETE FROM
            bridges
        WHERE
            (source = %(first_channel)s AND target = %(second_channel)s)
            OR (source = %(second_channel)s AND target = %(first_channel)s)
        """,
        {
            "first_channel": str(message_channel.id),
            "second_channel": str(target_channel.id),
        },
    )

    globals.conn.commit()
    cur.close()

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

    assert globals.conn
    cur = globals.conn.cursor()

    for channel_id in paired_channels:
        await demolish_bridges(channel_id, message_channel)

    exception_str = (  # Unfortunately this kind of injection is the only way to get this to work
        ""
        if len(exceptions) == 0
        else "AND target NOT IN (" + ", ".join(f"'{i}'" for i in exceptions) + ")"
    )
    cur.execute(
        f"""
        DELETE FROM
            bridges
        WHERE
            target = %(channel)s
            OR (source = %(channel)s {exception_str})
        """,
        {"channel": str(message_channel.id)},
    )

    globals.conn.commit()
    cur.close()

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
    cur: mysql.connector.abstracts.MySQLCursorAbstract | None = None,
) -> Bridge:
    """Create a one-way Bridge from source channel to target channel in `bridges`, creating a webhook if necessary, then inserts a reference to this new bridge into the database.

    #### Args:
        - `source`: Source channel for the Bridge, or ID of same.
        - `target`: Target channel for the Bridge, or ID of same.
        - `webhook`: Optionally, an already-existing webhook connecting these channels. Defaults to None.
        - `cur`: Optionally, a cursor for the connection to the database. Defaults to None, in which case creates and closes a new one locally.
    """
    bridge = await create_bridge(source, target, webhook)

    assert globals.conn
    if not cur:
        close_after = True
        cur = globals.conn.cursor()
    else:
        close_after = False
    cur.execute(
        """
        INSERT INTO bridges (source, target, webhook)
        VALUES (%(source_id)s, %(target_id)s, %(webhook_id)s)
        ON DUPLICATE KEY UPDATE webhook = %(webhook_id)s
        """,
        {
            "source_id": str(globals.get_id_from_channel(source)),
            "target_id": str(globals.get_id_from_channel(target)),
            "webhook_id": str(bridge.webhook.id),
        },
    )
    if close_after:
        globals.conn.commit()
        cur.close()

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
