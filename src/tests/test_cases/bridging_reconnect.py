import asyncio
from unittest.mock import patch

import discord
import test_runner
from test_runner import (
    create_bridge,
    demolish_bridges,
    expect,
    give_manage_webhook_perms,
)

import common
from bridge import bridges
from events import bridge_unbridged_messages


class Reconnecting(test_runner.TestCase):
    order = 35
    dependencies = ["CreatingBridges", "DemolishingBridges", "BridgingMessages"]

    def __init__(self):
        super().__init__(test_runner.test_runner)


reconnect_bridging_tests = Reconnecting()


@reconnect_bridging_tests.test
async def bridges_unbridged_messages(
    bridge_bot: discord.Client,
    tester_bot: discord.Client,
    testing_server: discord.Guild,
    testing_channels: tuple[
        discord.TextChannel,
        discord.TextChannel,
        discord.TextChannel,
        discord.TextChannel,
    ],
) -> list[str]:
    await give_manage_webhook_perms(tester_bot, testing_server)

    channel_1 = testing_channels[0]
    channel_2 = testing_channels[1]
    await demolish_bridges(channel_1, channel_and_threads=True)
    await create_bridge(channel_1, channel_2.id)

    # Send a message that gets bridged normally (establishes DB state)
    await channel_1.send("already bridged")
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": "already bridged", "be_from": bridge_bot},
    )

    # Temporarily suppress bridging by making get_outbound_bridges return None
    # for channel_1. The gateway-triggered on_message will exit early.
    original = bridges.get_outbound_bridges

    def mock_get_outbound(source):
        if common.get_id_from_channel(source) == channel_1.id:
            return None
        return original(source)

    with patch.object(bridges, "get_outbound_bridges", side_effect=mock_get_outbound):
        await channel_1.send("missed during disconnect")
        await asyncio.sleep(1)  # let the gateway event fire and be ignored

    # Patch removed — now call bridge_unbridged_messages to catch up
    await bridge_unbridged_messages()

    # The missed message should now appear in channel_2
    _, f = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": "missed during disconnect", "be_from": bridge_bot},
    )
    failure_messages += f

    return failure_messages


@reconnect_bridging_tests.test
async def does_not_duplicate_already_bridged_messages(
    bridge_bot: discord.Client,
    tester_bot: discord.Client,
    testing_server: discord.Guild,
    testing_channels: tuple[
        discord.TextChannel,
        discord.TextChannel,
        discord.TextChannel,
        discord.TextChannel,
    ],
) -> list[str]:
    await give_manage_webhook_perms(tester_bot, testing_server)

    channel_1 = testing_channels[0]
    channel_2 = testing_channels[1]
    await demolish_bridges(channel_1, channel_and_threads=True)
    await create_bridge(channel_1, channel_2.id)

    # Send a message that gets bridged normally
    await channel_1.send("already bridged")
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": "already bridged", "be_from": bridge_bot},
    )

    # Call bridge_unbridged_messages — should be a no-op
    await bridge_unbridged_messages()

    # No new message should appear
    _, f = await expect("no_new_message", in_channel=channel_2, timeout=5)
    failure_messages += f

    return failure_messages
