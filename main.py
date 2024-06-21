from bridge import Bridges
import discord
import json
from typing import cast

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
outbound_bridges: dict[int, Bridges] = {}
inbound_bridges: dict[int, dict[int, Bridges]] = {}


@client.event
async def on_ready():
    global is_ready
    if is_ready:
        return

    print(f"{client.user} is connected to the following servers:\n")
    for server in client.guilds:
        print(f"{server.name}(id: {server.id})")

    is_ready = True


@client.event
async def on_message(message: discord.Message):
    print(message.content)


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
        inbound_bridges[target.id]
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
