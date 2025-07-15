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
)


class AutoBridgeThreads(test_runner.TestCase):
    def __init__(self):
        super().__init__(test_runner.test_runner)


auto_bridge_threads_tests = AutoBridgeThreads()


@auto_bridge_threads_tests.test
async def needs_to_be_from_channel(
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

    # Bridge thread from channel 1
    thread_1 = await channel_1.create_thread(
        name=f"thread_{random.randint(0, 10000)}",
        type=discord.ChannelType.public_thread,
    )

    message_sent = await thread_1.send("/auto_bridge_threads")
    _, failure_messages = await expect(
        "next_message",
        in_channel=thread_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Please run this command from a text channel.",
        },
    )

    return failure_messages


@auto_bridge_threads_tests.test
async def requires_bridges(
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
    await demolish_bridges(channel_1, channel_and_threads=True)

    message_sent = await channel_1.send("/auto_bridge_threads")
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "This channel isn't bridged to any other channels.",
        },
    )

    return failure_messages


@auto_bridge_threads_tests.test
async def requires_bridge_to_channel(
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
    channel_2 = testing_channels[0]
    await demolish_bridges(channel_1, channel_and_threads=True)

    thread_2 = await channel_2.create_thread(
        name=f"thread_{random.randint(0, 10000)}",
        type=discord.ChannelType.public_thread,
    )
    await create_bridge(channel_1, thread_2.id)

    message_sent = await channel_1.send("/auto_bridge_threads")
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "This channel is only bridged to threads.",
        },
    )

    return failure_messages


@auto_bridge_threads_tests.test
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
    channel_3 = testing_channels[2]
    await demolish_bridges(channel_1, channel_and_threads=True)
    await demolish_bridges(channel_2, channel_and_threads=True)
    await demolish_bridges(channel_3, channel_and_threads=True)

    await create_bridge(channel_1, channel_2.id)
    await create_bridge(channel_2, channel_3.id)

    # Auto bridge threads
    message_sent = await channel_2.send("/auto_bridge_threads")
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Interaction was deferred with with thinking = True.",
        },
    )

    _, f = await expect(
        "next_message",
        in_channel=channel_2,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Threads will now be automatically created across bridges when they are created in this channel.",
        },
    )
    failure_messages += f

    # Create thread
    thread_2_name = f"thread_{random.randint(0, 10000)}"
    thread_2 = await channel_2.create_thread(
        name=thread_2_name,
        type=discord.ChannelType.public_thread,
    )

    expectations = await asyncio.gather(
        expect(
            "thread",
            in_channel=channel_1,
            with_name=thread_2_name,
            to="exist",
            timeout=2,
        ),
        expect(
            "thread",
            in_channel=channel_3,
            with_name=thread_2_name,
            to="exist",
            timeout=2,
        ),
    )

    threads_bridged_to_thread_2: list[discord.Thread] = []
    for t, f in expectations:
        failure_messages += f
        if t:
            threads_bridged_to_thread_2.append(t)
    if len(threads_bridged_to_thread_2) != 2:
        return failure_messages

    # Empty "bridge bot added person to thread" message
    expectations = await asyncio.gather(
        expect(
            "next_message",
            in_channel=threads_bridged_to_thread_2[0],
            to={"equal": ""},
        ),
        expect(
            "next_message",
            in_channel=threads_bridged_to_thread_2[1],
            to={"equal": ""},
        ),
    )
    for _, f in expectations:
        failure_messages += f

    # Send message from thread 2
    content = "message from thread 2"
    await thread_2.send(content)

    expectations = await asyncio.gather(
        expect(
            "next_message",
            in_channel=threads_bridged_to_thread_2[0],
            to={"equal": content, "be_from": bridge_bot},
        ),
        expect(
            "next_message",
            in_channel=threads_bridged_to_thread_2[1],
            to={"equal": content, "be_from": bridge_bot},
        ),
    )
    for _, f in expectations:
        failure_messages += f

    # Send message from thread 1
    content = "message from thread 1"
    await threads_bridged_to_thread_2[0].send(content)

    expectations = await asyncio.gather(
        expect(
            "next_message",
            in_channel=thread_2,
            to={"equal": content, "be_from": bridge_bot},
        ),
        expect(
            "next_message",
            in_channel=threads_bridged_to_thread_2[1],
            to={"equal": content, "be_from": bridge_bot},
        ),
    )
    for _, f in expectations:
        failure_messages += f

    # Disable auto_bridge_threads
    message_sent = await channel_2.send("/auto_bridge_threads")
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Interaction was deferred with with thinking = True.",
        },
    )

    _, f = await expect(
        "next_message",
        in_channel=channel_2,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Threads will no longer be automatically created across bridges when they are created in this channel.",
        },
    )
    failure_messages += f

    # Sending messages from existing threads still works
    content = "message from thread 2, pt. 2"
    await thread_2.send(content)

    expectations = await asyncio.gather(
        expect(
            "next_message",
            in_channel=threads_bridged_to_thread_2[0],
            to={"equal": content, "be_from": bridge_bot},
        ),
        expect(
            "next_message",
            in_channel=threads_bridged_to_thread_2[1],
            to={"equal": content, "be_from": bridge_bot},
        ),
    )
    for _, f in expectations:
        failure_messages += f

    # New threads aren't bridged
    thread_2_name = f"thread_{random.randint(0, 10000)}"
    thread_2 = await channel_2.create_thread(
        name=thread_2_name,
        type=discord.ChannelType.public_thread,
    )

    expectations = await asyncio.gather(
        expect(
            "thread",
            in_channel=channel_1,
            with_name=thread_2_name,
            to="not_exist",
            timeout=2,
        ),
        expect(
            "thread",
            in_channel=channel_3,
            with_name=thread_2_name,
            to="not_exist",
            timeout=2,
        ),
    )

    for _, f in expectations:
        failure_messages += f

    return failure_messages
