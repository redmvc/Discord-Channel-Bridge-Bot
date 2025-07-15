import asyncio
import random
import sys
from pathlib import Path

import discord

sys.path.append(str(Path(__file__).parent.parent))
import test_runner
from test_runner import (
    create_bridge,
    demolish_bridges,
    expect,
    give_manage_webhook_perms,
    remove_manage_webhook_perms,
)


class DemolishingBridges(test_runner.TestCase):
    def __init__(self):
        super().__init__(test_runner.test_runner)


bridge_demolition_tests = DemolishingBridges()


@bridge_demolition_tests.test
async def requires_valid_channel(
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
    channel_1 = testing_channels[0]
    message_sent = await demolish_bridges(channel_1, 1234, send_message=True)
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Unsupported argument passed. Please pass a channel reference, ID, or link.",
        },
    )

    return failure_messages


@bridge_demolition_tests.test
async def requires_manage_webhook_permissions(
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
    await remove_manage_webhook_perms(tester_bot, testing_server)

    channel_1 = testing_channels[0]
    channel_2 = testing_channels[1]
    message_sent = await demolish_bridges(channel_1, channel_2.id, send_message=True)

    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Please make sure both you and the bot have 'Manage Webhooks' permission in both this and target channels.",
        },
    )

    return failure_messages


@bridge_demolition_tests.test
async def requires_bridges_to_exist(
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

    # First really demolish them without sending a command
    await demolish_bridges(channel_1, channel_2.id)

    # Now try to send the command to demolish them
    message_sent = await demolish_bridges(channel_1, channel_2.id, send_message=True)
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "There are no bridges between current and target channels.",
        },
    )

    return failure_messages


@bridge_demolition_tests.test
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
    await create_bridge(channel_1, channel_2.id)

    # Create bridge
    message_sent = await demolish_bridges(channel_1, channel_2.id, send_message=True)
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Interaction was deferred with with thinking = True.",
        },
    )
    _, f = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Bridges demolished!",
        },
    )
    failure_messages += f

    # Send message from channel_1
    content = "message from channel 1"
    await channel_1.send(content)
    _, f = await expect("no_new_message", in_channel=channel_2, timeout=5)
    failure_messages += f

    # Send message from channel_2
    content = "message from channel 2"
    await channel_2.send(content)
    _, f = await expect("no_new_message", in_channel=channel_1, timeout=5)
    failure_messages += f

    return failure_messages


@bridge_demolition_tests.test
async def works_when_demolishing_all(
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
    await demolish_bridges(channel_1, channel_2)

    thread_1 = await channel_1.create_thread(
        name=f"thread_{random.randint(0, 10000)}",
        type=discord.ChannelType.public_thread,
    )
    thread_2 = await channel_2.create_thread(
        name=f"thread_{random.randint(0, 10000)}",
        type=discord.ChannelType.public_thread,
    )
    await create_bridge(channel_1, channel_2.id)
    await create_bridge(thread_1, thread_2.id)

    # Demolish bridges
    message_sent = await demolish_bridges(
        channel_1,
        channel_and_threads=True,
        send_message=True,
    )
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Interaction was deferred with with thinking = True.",
        },
    )
    _, f = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Bridges demolished!",
        },
    )
    failure_messages += f

    # Send message from channel_1 and thread_1
    await asyncio.gather(
        channel_1.send("message from channel 1"),
        thread_1.send("message from thread 1"),
    )
    expectations = await asyncio.gather(
        expect("no_new_message", in_channel=channel_2, timeout=5),
        expect("no_new_message", in_channel=thread_2, timeout=5),
    )
    for _, f in expectations:
        failure_messages += f

    # Send message from channel_2 and thread_2
    await asyncio.gather(
        channel_2.send("message from channel 2"),
        thread_2.send("message from thread 2"),
    )
    expectations = await asyncio.gather(
        expect("no_new_message", in_channel=channel_1, timeout=5),
        expect("no_new_message", in_channel=thread_1, timeout=5),
    )
    for _, f in expectations:
        failure_messages += f

    return failure_messages
