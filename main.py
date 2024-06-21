from __future__ import annotations

import asyncio
from typing import TypedDict, cast

import discord
import mysql.connector
import mysql.connector.abstracts

import globals
from bridge import Bridge, Bridges

bridges = Bridges()


@globals.client.event
async def on_ready():
    """Called when the client is done preparing the data received from Discord. Usually after login is successful and the Client.guilds and co. are filled up."""
    if globals.is_ready:
        return

    # I am going to try to identify all existing bridges
    # First, I fetch all target channels registered in the db
    globals.conn = mysql.connector.connect(
        host=globals.credentials["db_host"],
        port=globals.credentials["db_port"],
        user=globals.credentials["db_user"],
        passwd=globals.credentials["db_pwd"],
        database=globals.credentials["db_name"],
        buffered=True,
        autocommit=False,
    )
    cur = globals.conn.cursor()
    cur.execute(
        "SELECT source, target, webhook FROM bridges;"
    )  # columns are "id", "source", "target", and "webhook", where "id" is a primary key and ("source", "target") have a uniqueness constraint

    registered_bridges = cast(list[tuple[str, str, str]], cur.fetchall())
    invalid_channels: set[str] = set()
    invalid_webhooks: set[str] = set()
    for source_id_str, target_id_str, webhook_id_str in registered_bridges:
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
                await create_bridge(source_id, target_id, webhook)
        except Exception:
            invalid_webhooks.add(webhook_id_str)

            if source_channel and target_channel:
                # There *should* be a webhook there and I have access to the channels
                await create_bridge_and_db(source_id, target_id, None, cur)

    if len(invalid_channels) > 0:
        cur.executemany(
            """
            DELETE FROM bridges
            WHERE source = %(channel_id)s
                OR target = %(channel_id)s
            """,
            [{"channel_id": channel_id_str} for channel_id_str in invalid_channels],
        )

    if len(invalid_webhooks) > 0:
        cur.executemany(
            """
            DELETE FROM bridges
            WHERE webhook = %s
            """,
            [(webhook_id_str,) for webhook_id_str in invalid_webhooks],
        )

    globals.conn.commit()
    cur.close()

    await globals.command_tree.sync()
    print(f"{globals.client.user} is connected to the following servers:\n")
    for server in globals.client.guilds:
        print(f"{server.name}(id: {server.id})")

    globals.is_ready = True


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
    global bridges

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
        {
            "channel": str(message_channel.id),
        },
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


@globals.client.event
async def on_message(message: discord.Message):
    """Called when a Message is created and sent.

    This requires Intents.messages to be enabled."""
    global bridges

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
    global bridges

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

    global bridges

    await bridges.demolish_bridge(source, target)


globals.client.run(cast(str, globals.credentials["app_token"]), reconnect=True)
