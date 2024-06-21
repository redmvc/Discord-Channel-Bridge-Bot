from __future__ import annotations

import asyncio
import re
from typing import TypedDict, cast

import discord
import mysql.connector
import mysql.connector.abstracts

import globals
from bridge import Bridges


@globals.client.event
async def on_ready():
    if globals.is_ready:
        return

    while not globals.globals_are_initialised:
        await asyncio.sleep(1)

    # The names of webhooks created by the Bridges class are formatted like: `:bridge: (src_id tgt_id)`
    webhook_name_parser = re.compile(r"^:bridge: \((?P<src>\d+) (?P<tgt>\d+)\)$")

    # I am going to try to identify all existing bridges
    # First, I fetch all target channels registered in the db
    conn = mysql.connector.connect(
        host=globals.credentials["db_host"],
        port=globals.credentials["db_port"],
        user=globals.credentials["db_user"],
        passwd=globals.credentials["db_pwd"],
        database=globals.credentials["db_name"],
        buffered=True,
        autocommit=False,
    )
    cur = conn.cursor()
    cur.execute(
        "SELECT UNIQUE(target) FROM bridges;"
    )  # columns are "id", "source", and "target"
    target_ids = cur.fetchall()
    for target_id in target_ids:
        # Only the channel IDs are stored, so I have to find the appropriate channel for this bridge
        target_id_str = cast(tuple[str], target_id)[0]
        target_id = int(target_id_str)
        target = globals.client.get_channel(target_id)
        if not isinstance(target, (discord.TextChannel, discord.Thread)):
            # This ID isn't a valid bridged ID, I'll remove it from my database
            cur.execute("DELETE FROM bridges WHERE target = %s;", (target_id_str,))
            conn.commit()
            continue

        # And the channel its webhooks will be attached to
        webhook_channel = (
            target.parent if isinstance(target, discord.Thread) else target
        )
        if not isinstance(webhook_channel, discord.TextChannel):
            # This ID isn't a valid bridged ID, I'll remove it from my database
            cur.execute("DELETE FROM bridges WHERE target = %s;", (target_id_str,))
            conn.commit()
            continue

        # Then I get all webhooks attached to that channel whose target ID (stored in their names) is the current ID
        candidate_webhooks = await webhook_channel.webhooks()
        webhooks = [
            (webhook, match.group("src"))
            for webhook in candidate_webhooks
            if webhook.name
            and (match := webhook_name_parser.fullmatch(webhook.name))
            and match
            and match.group("tgt") == target_id_str
        ]

        # Next I get every bridge that has this channel/thread as its target
        cur.execute(
            """
            SELECT
                id, source
            FROM
                bridges
            WHERE
                target = %s;
            """,
            (target_id_str,),
        )
        bridges = cur.fetchall()
        for bridge in bridges:
            bridge_id, source_id_str = cast(tuple[int, str], bridge)
            source_id = int(source_id_str)
            # I try to find a webhook that has the same source as this bridge's entry
            webhook = next(
                iter([w for w, src in webhooks if src == source_id_str]), None
            )
            if webhook:
                # Found the webhook, so I'm going to store it in my list of bridges
                await create_bridge(source_id, target, webhook)
            else:
                # I couldn't find that webhook, so I'll delete that entry from the db
                cur.execute(
                    """
                    DELETE FROM bridges
                    WHERE id = %s;
                    """,
                    (bridge_id,),
                )
                conn.commit()

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

    await create_bridge(message_channel, target_channel)
    cur.execute(
        """
        INSERT INTO bridges (source, target)
        SELECT %(source_id)s, %(target_id)s
        WHERE NOT EXISTS (
            SELECT 1
            FROM bridges
            WHERE source = %(source_id)s
                AND target = %(target_id)s
        )
        """,
        {"source_id": str(message_channel.id), "target_id": str(target_channel.id)},
    )

    await create_bridge(target_channel, message_channel)
    cur.execute(
        """
        INSERT INTO bridges (source, target)
        SELECT %(source_id)s, %(target_id)s
        WHERE NOT EXISTS (
            SELECT 1
            FROM bridges
            WHERE source = %(source_id)s
                AND target = %(target_id)s
        """,
        {"source_id": str(target_channel.id), "target_id": str(message_channel.id)},
    )

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

    assert globals.conn
    cur = globals.conn.cursor()

    await create_bridge(message_channel, target_channel)
    cur.execute(
        """
        INSERT INTO bridges (source, target)
        SELECT %(source_id)s, %(target_id)s
        WHERE NOT EXISTS (
            SELECT 1
            FROM bridges
            WHERE source = %(source_id)s
                AND target = %(target_id)s
        )
        """,
        {"source_id": str(message_channel.id), "target_id": str(target_channel.id)},
    )

    globals.conn.commit()
    cur.close()

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

    assert globals.conn
    cur = globals.conn.cursor()

    await create_bridge(source_channel, message_channel)
    cur.execute(
        """
        INSERT INTO bridges (source, target)
        SELECT %(source_id)s, %(target_id)s
        WHERE NOT EXISTS (
            SELECT 1
            FROM bridges
            WHERE source = %(source_id)s
                AND target = %(target_id)s
        """,
        {"source_id": str(source_channel.id), "target_id": str(message_channel.id)},
    )

    globals.conn.commit()
    cur.close()

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
    paired_channels = set(globals.inbound_bridges[message_channel.id].keys())
    outbound = globals.outbound_bridges[message_channel.id]
    exceptions: set[int] = set()
    for target_id in outbound.get_webhooks().keys():
        target_channel = globals.client.get_channel(target_id)
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
    if not isinstance(message.channel, (discord.TextChannel, discord.Thread)):
        return

    if message.webhook_id:
        # Don't bridge messages from webhooks
        return

    # I need to wait until the on_ready event is done before processing any messages
    global is_ready
    time_waited = 0
    while not globals.is_ready and time_waited < 100:
        await asyncio.sleep(1)
        time_waited += 1
    if time_waited >= 100:
        # somethin' real funky going on here
        # I don't have error handling yet though
        print("Taking forever to get ready.")
        return

    bridge = globals.outbound_bridges.get(message.channel.id)
    if not bridge:
        return

    # Send a message out to each target webhook
    outbound_webhooks = bridge.get_webhooks()
    for target_id, webhook in outbound_webhooks.items():
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


async def create_bridge(
    source: discord.TextChannel | discord.Thread | int,
    target: discord.TextChannel | discord.Thread | int,
    webhook: discord.Webhook | None = None,
):
    global outbound_bridges, inbound_bridges

    if isinstance(source, int):
        source_id = source
    else:
        source_id = source.id

    if isinstance(target, int):
        target = cast(
            discord.TextChannel | discord.Thread, globals.client.get_channel(target)
        )
    assert isinstance(target, discord.TextChannel | discord.Thread)

    if not globals.outbound_bridges.get(source_id):
        globals.outbound_bridges[source_id] = Bridges(source_id)
    await globals.outbound_bridges[source_id].add_target(target, webhook)

    if not globals.inbound_bridges.get(target.id):
        globals.inbound_bridges[target.id] = {}
    globals.inbound_bridges[target.id][source_id] = globals.outbound_bridges[source_id]


async def demolish_bridges(
    source: discord.TextChannel | discord.Thread | int,
    target: discord.TextChannel | discord.Thread | int,
):
    if isinstance(source, int):
        source_id = source
    else:
        source_id = source.id

    if isinstance(target, int):
        target_id = target
    else:
        target_id = target.id

    await demolish_bridge_one_sided(source_id, target_id)
    await demolish_bridge_one_sided(target_id, source_id)


async def demolish_bridge_one_sided(source_id, target_id):
    if globals.outbound_bridges.get(source_id):
        bridge = globals.outbound_bridges[source_id]
        await bridge.demolish(target_id)
        if len(bridge.get_webhooks()) == 0:
            del globals.outbound_bridges[source_id]

    if globals.inbound_bridges.get(target_id):
        del globals.inbound_bridges[target_id][source_id]


globals.init()
globals.client.run(cast(str, globals.credentials["app_token"]), reconnect=True)
