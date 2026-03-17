import discord
import test_runner
from test_runner import (
    create_bridge,
    demolish_bridges,
    expect,
    give_manage_webhook_perms,
)


class BridgingDeletions(test_runner.TestCase):
    order = 70
    dependencies = ["CreatingBridges", "DemolishingBridges", "BridgingMessages"]

    def __init__(self):
        super().__init__(test_runner.test_runner)


deletion_bridging_tests = BridgingDeletions()


@deletion_bridging_tests.test
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

    channel_1 = testing_channels[0]
    channel_2 = testing_channels[1]
    await demolish_bridges(channel_1, channel_and_threads=True)
    await create_bridge(channel_1, channel_2.id)

    # Send message
    original_message = await channel_1.send("message to be deleted")
    bridged_message, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": "message to be deleted", "be_from": bridge_bot},
    )
    if not bridged_message:
        return failure_messages

    # Delete message
    await original_message.delete()
    _, f = await expect(bridged_message, to="be_deleted")
    failure_messages += f

    return failure_messages


@deletion_bridging_tests.test
async def works_down_chains(
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
    channel_3 = testing_channels[2]
    await demolish_bridges(channel_1, channel_and_threads=True)
    await demolish_bridges(channel_2, channel_and_threads=True)
    await create_bridge(channel_1, channel_2.id)
    await create_bridge(channel_2, channel_3.id)

    # Send message
    original_message = await channel_1.send("message to be deleted in chain")

    bridged_message_2, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": "message to be deleted in chain", "be_from": bridge_bot},
    )

    bridged_message_3, f = await expect(
        "next_message",
        in_channel=channel_3,
        to={"equal": "message to be deleted in chain", "be_from": bridge_bot},
    )
    failure_messages += f

    if not (bridged_message_2 or bridged_message_3):
        return failure_messages

    # Delete message
    await original_message.delete()

    if bridged_message_2:
        _, f = await expect(bridged_message_2, to="be_deleted")
        failure_messages += f
    if bridged_message_3:
        _, f = await expect(bridged_message_3, to="be_deleted")
        failure_messages += f

    return failure_messages


@deletion_bridging_tests.test
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

    channel_1 = testing_channels[0]
    channel_2 = testing_channels[1]
    await demolish_bridges(channel_1, channel_and_threads=True)
    await create_bridge(channel_1, channel_2.id)

    # Send message
    original_message = await channel_1.send("message that should not be deleted")
    bridged_message, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": "message that should not be deleted", "be_from": bridge_bot},
    )
    if not bridged_message:
        return failure_messages

    # Demolish bridge
    await demolish_bridges(channel_1, channel_and_threads=True)

    # Delete message
    await original_message.delete()
    _, f = await expect(bridged_message, to="not_be_deleted", timeout=5)
    failure_messages += f

    return failure_messages


@deletion_bridging_tests.test
async def deletes_forwarded_messages(
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
    channel_3 = testing_channels[2]
    await demolish_bridges(channel_1, channel_and_threads=True)
    await demolish_bridges(channel_3, channel_and_threads=True)
    await create_bridge(channel_1, channel_2.id)

    # Send a message in channel_3 (unbridged) then forward it to channel_1
    original_message = await channel_3.send("message to forward then delete")
    forwarded_message = await original_message.forward(channel_1)

    # Wait for the header message ("forwarded by...") in channel_2
    header_message, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"contain": "forwarded"},
    )
    if not header_message:
        return failure_messages

    # Wait for the forwarded message in channel_2
    bridged_forward, f = await expect(
        "next_message",
        in_channel=channel_2,
        to={"be_a_forward_of": original_message},
    )
    failure_messages += f
    if not bridged_forward:
        return failure_messages

    # Delete the forward in channel_1
    await forwarded_message.delete()

    # Expect both header and forwarded message to be deleted
    _, f = await expect(header_message, to="be_deleted")
    failure_messages += f

    _, f = await expect(bridged_forward, to="be_deleted")
    failure_messages += f

    return failure_messages
