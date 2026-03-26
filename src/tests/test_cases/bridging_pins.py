import discord
import test_runner
from test_runner import (
    create_bridge,
    demolish_bridges,
    expect,
    give_manage_webhook_perms,
    give_pin_perms,
    remove_pin_perms,
)


class BridgingPins(test_runner.TestCase):
    order = 85
    dependencies = ["CreatingBridges", "DemolishingBridges", "BridgingMessages"]

    def __init__(self):
        super().__init__(test_runner.test_runner)


pin_bridging_tests = BridgingPins()


@pin_bridging_tests.test
async def does_not_work_without_permission(
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
    await remove_pin_perms(bridge_bot, testing_server)

    channel_1 = testing_channels[0]
    channel_2 = testing_channels[1]
    await demolish_bridges(channel_1, channel_and_threads=True)
    await create_bridge(channel_1, channel_2.id)

    # Send message
    original_message = await channel_1.send("pin without permission")
    bridged_message, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": "pin without permission", "be_from": bridge_bot},
    )
    if not bridged_message:
        return failure_messages

    # Pin original
    await original_message.pin()
    _, f = await expect(bridged_message, to="not_be_pinned", timeout=5)
    failure_messages += f

    # Unpin the original
    await original_message.unpin()

    return failure_messages


@pin_bridging_tests.test
async def works(
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
    await give_pin_perms(bridge_bot, testing_server)

    channel_1 = testing_channels[0]
    channel_2 = testing_channels[1]
    await demolish_bridges(channel_1, channel_and_threads=True)
    await create_bridge(channel_1, channel_2.id)

    # Send message
    original_message = await channel_1.send("pin this message")
    bridged_message, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": "pin this message", "be_from": bridge_bot},
    )
    if not bridged_message:
        return failure_messages

    # Pin original — bridged copy should be pinned too
    await original_message.pin()
    _, f = await expect(bridged_message, to="be_pinned")
    failure_messages += f

    # Clean up
    await original_message.unpin()

    return failure_messages


@pin_bridging_tests.test
async def unpinning_works(
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
    await give_pin_perms(bridge_bot, testing_server)

    channel_1 = testing_channels[0]
    channel_2 = testing_channels[1]
    await demolish_bridges(channel_1, channel_and_threads=True)
    await create_bridge(channel_1, channel_2.id)

    # Send message
    original_message = await channel_1.send("pin then unpin")
    bridged_message, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": "pin then unpin", "be_from": bridge_bot},
    )
    if not bridged_message:
        return failure_messages

    # Pin original, wait for bridged copy to be pinned
    await original_message.pin()
    _, f = await expect(bridged_message, to="be_pinned")
    failure_messages += f

    # Unpin original — bridged copy should be unpinned too
    await original_message.unpin()
    _, f = await expect(bridged_message, to="not_be_pinned")
    failure_messages += f

    return failure_messages


@pin_bridging_tests.test
async def works_from_bridged_side(
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
    await give_pin_perms(bridge_bot, testing_server)

    channel_1 = testing_channels[0]
    channel_2 = testing_channels[1]
    await demolish_bridges(channel_1, channel_and_threads=True)
    await create_bridge(channel_1, channel_2.id)

    # Send message
    original_message = await channel_1.send("pin from bridged side")
    bridged_message, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": "pin from bridged side", "be_from": bridge_bot},
    )
    if not bridged_message:
        return failure_messages

    # Pin the bridged copy — original should be pinned too
    await bridged_message.pin()
    _, f = await expect(original_message, to="be_pinned")
    failure_messages += f

    # Clean up
    await bridged_message.unpin()

    return failure_messages


@pin_bridging_tests.test
async def does_not_work_if_bridge_demolished(
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
    await give_pin_perms(bridge_bot, testing_server)

    channel_1 = testing_channels[0]
    channel_2 = testing_channels[1]
    await demolish_bridges(channel_1, channel_and_threads=True)
    await create_bridge(channel_1, channel_2.id)

    # Send message
    original_message = await channel_1.send("pin after demolish")
    bridged_message, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": "pin after demolish", "be_from": bridge_bot},
    )
    if not bridged_message:
        return failure_messages

    # Demolish bridge
    await demolish_bridges(channel_1, channel_and_threads=True)

    # Pin original — bridged copy should NOT be pinned
    await original_message.pin()
    _, f = await expect(bridged_message, to="not_be_pinned", timeout=5)
    failure_messages += f

    # Clean up
    await original_message.unpin()

    return failure_messages
