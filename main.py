from __future__ import annotations

import json
import re
from typing import cast

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

    print(f"{client.user} is connected to the following servers:\n")
    for server in client.guilds:
        print(f"{server.name}(id: {server.id})")

    is_ready = True


@client.event
async def on_message(message: discord.Message):
    print(message.content)


def get_channel(
    link_or_mention: str,
) -> discord.guild.GuildChannel | discord.Thread | discord.abc.PrivateChannel | None:
    if link_or_mention.startswith("<#"):
        try:
            channel_id = int(link_or_mention.split("<#")[1].split(">")[0])
        except ValueError:
            return None
        return client.get_channel(channel_id)
    elif link_or_mention.startswith("https://discord.com/channels"):
        try:
            channel_id = int(link_or_mention.rsplit("/")[0])
        except ValueError:
            return None
        return client.get_channel(channel_id)
    return None


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
