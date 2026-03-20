import asyncio
from unittest.mock import AsyncMock, patch

import discord
import test_runner
from test_runner import (
    create_bridge,
    demolish_bridges,
    expect,
    give_manage_webhook_perms,
)

import events
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
    first_message, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": "already bridged", "be_from": bridge_bot},
    )

    if first_message is None:
        return failure_messages

    # Temporarily suppress processing messages
    with patch.object(events, "bridge_message_helper", new=AsyncMock()):
        await channel_1.send("missed during disconnect")
        await asyncio.sleep(1)  # let the gateway on_message fire and complete

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
