from __future__ import annotations

import asyncio
import json
import re
from typing import TypedDict, cast

import discord
import mysql.connector
import mysql.connector.abstracts

from bridge import Bridges

# Create the client connection
intents = discord.Intents()
intents.emojis_and_stickers = True
intents.guilds = True
intents.members = True
intents.message_content = True
intents.messages = True
intents.reactions = True
intents.typing = True
intents.webhooks = True
client = discord.Client(intents=intents)
command_tree = discord.app_commands.CommandTree(client)
credentials = json.load(open("credentials.json"))

is_ready = False  # whether the bot is ready and doesn't need to be readied again
conn: (
    mysql.connector.pooling.PooledMySQLConnection
    | mysql.connector.abstracts.MySQLConnectionAbstract
    | None
) = None  # the connection to the database
outbound_bridges: dict[int, Bridges] = {}
inbound_bridges: dict[int, dict[int, Bridges]] = {}


@client.event
async def on_ready():
    global is_ready, conn, outbound_bridges, inbound_bridges
    if is_ready:
        return

    # The names of webhooks created by the Bridges class are formatted like: `:bridge: (src_id tgt_id)`
    webhook_name_parser = re.compile(r"^:bridge: \((?P<src>\d+) (?P<tgt>\d+)\)$")

    # I am going to try to identify all existing bridges
    # First, I fetch all target channels registered in the db
    conn = mysql.connector.connect(
        host=credentials["db_host"],
        port=credentials["db_port"],
        user=credentials["db_user"],
        passwd=credentials["db_pwd"],
        database=credentials["db_name"],
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
        target = client.get_channel(target_id)
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

    await command_tree.sync()
    print(f"{client.user} is connected to the following servers:\n")
    for server in client.guilds:
        print(f"{server.name}(id: {server.id})")

    is_ready = True


@discord.app_commands.guild_only()
@command_tree.command(
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

    target_channel = get_channel(target)
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

    global conn
    assert conn
    cur = conn.cursor()

    await create_bridge(message_channel, target_channel)
    cur.execute(
        """
        INSERT INTO bridges (source, target)
        VALUES (%s, %s)
        """,
        (str(message_channel.id), str(target_channel.id)),
    )
    await create_bridge(target_channel, message_channel)
    cur.execute(
        """
        INSERT INTO bridges (source, target)
        VALUES (%s, %s)
        """,
        (str(target_channel.id), str(message_channel.id)),
    )

    conn.commit()
    cur.close()

    await interaction.response.send_message(
        "✅ Bridge created! Try sending a message from either channel 😁",
        ephemeral=True,
    )


@discord.app_commands.guild_only()
@command_tree.command(
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

    target_channel = get_channel(target)
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

    global conn
    assert conn
    cur = conn.cursor()

    await create_bridge(message_channel, target_channel)
    cur.execute(
        """
        INSERT INTO bridges (source, target)
        VALUES (%s, %s)
        """,
        (str(message_channel.id), str(target_channel.id)),
    )

    conn.commit()
    cur.close()

    await interaction.response.send_message(
        "✅ Bridge created! Try sending a message from this channel 😁",
        ephemeral=True,
    )


@discord.app_commands.guild_only()
@command_tree.command(
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

    source_channel = get_channel(source)
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

    global conn
    assert conn
    cur = conn.cursor()

    await create_bridge(source_channel, message_channel)
    cur.execute(
        """
        INSERT INTO bridges (source, target)
        VALUES (%s, %s)
        """,
        (str(source_channel.id), str(message_channel.id)),
    )

    conn.commit()
    cur.close()

    await interaction.response.send_message(
        "✅ Bridge created! Try sending a message from the other channel 😁",
        ephemeral=True,
    )


@discord.app_commands.guild_only()
@command_tree.command(
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

    target_channel = get_channel(target)
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

    global conn
    assert conn
    cur = conn.cursor()

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

    conn.commit()
    cur.close()

    await interaction.response.send_message(
        "✅ Bridges demolished!",
        ephemeral=True,
    )


@client.event
async def on_message(message: discord.Message):
    if not isinstance(message.channel, (discord.TextChannel, discord.Thread)):
        return

    if message.webhook_id:
        # Don't bridge messages from webhooks
        return

    # I need to wait until the on_ready event is done before processing any messages
    global is_ready
    time_waited = 0
    while not is_ready and time_waited < 100:
        await asyncio.sleep(1)
        time_waited += 1
    if time_waited >= 100:
        # somethin' real funky going on here
        # I don't have error handling yet though
        print("Taking forever to get ready.")
        return

    bridge = outbound_bridges.get(message.channel.id)
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


def get_channel(
    link_or_mention: str,
) -> discord.guild.GuildChannel | discord.Thread | discord.abc.PrivateChannel | None:
    if link_or_mention.startswith("https://discord.com/channels"):
        try:
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
    return client.get_channel(channel_id)


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
        target = cast(discord.TextChannel | discord.Thread, client.get_channel(target))
    assert isinstance(target, discord.TextChannel | discord.Thread)

    if not outbound_bridges.get(source_id):
        outbound_bridges[source_id] = Bridges(source_id)
    await outbound_bridges[source_id].add_target(target, webhook)

    if not inbound_bridges.get(target.id):
        inbound_bridges[target.id] = {}
    inbound_bridges[target.id][source_id] = outbound_bridges[source_id]


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
    if outbound_bridges.get(source_id):
        bridge = outbound_bridges[source_id]
        await bridge.demolish(target_id)
        if len(bridge.get_webhooks()) == 0:
            del outbound_bridges[source_id]

    if inbound_bridges.get(target_id):
        del inbound_bridges[target_id][source_id]


client.run(credentials["app_token"], reconnect=True)
