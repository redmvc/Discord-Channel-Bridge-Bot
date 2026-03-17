import asyncio
import random

import discord
import test_runner
from test_runner import (
    create_bridge,
    demolish_bridges,
    expect,
    give_manage_webhook_perms,
    set_nsfw,
)


class BridgingMessages(test_runner.TestCase):
    order = 30
    dependencies = ["CreatingBridges", "DemolishingBridges"]

    def __init__(self):
        super().__init__(test_runner.test_runner)


message_bridging_tests = BridgingMessages()


@message_bridging_tests.test
async def between_channels_works(
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
    await demolish_bridges(channel_2, channel_and_threads=True)

    # -----
    # Two-way bridge
    await create_bridge(channel_1, channel_2.id)

    # Send message from channel_1
    content = "message from channel 1"
    await channel_1.send(content)
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": content, "be_from": bridge_bot},
    )

    # Send message from channel_2
    content = "message from channel 2"
    await channel_2.send(content)
    _, f = await expect(
        "next_message",
        in_channel=channel_1,
        to={"equal": content, "be_from": bridge_bot},
    )
    failure_messages += f

    # -----
    # Outbound bridge
    await demolish_bridges(channel_1, channel_2)
    await create_bridge(channel_1, channel_2.id, direction="outbound")

    # Send message from channel_1
    content = "message from channel 1"
    await channel_1.send(content)
    _, f = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": content, "be_from": bridge_bot},
    )
    failure_messages += f

    # Send message from channel_2
    content = "message from channel 2"
    await channel_2.send(content)
    _, f = await expect(
        "no_new_message",
        in_channel=channel_1,
        timeout=5,
    )
    failure_messages += f

    # -----
    # Inbound bridge
    await demolish_bridges(channel_1, channel_2)
    await create_bridge(channel_1, channel_2.id, direction="inbound")

    # Send message from channel_1
    content = "message from channel 1"
    await channel_1.send(content)
    _, f = await expect(
        "no_new_message",
        in_channel=channel_2,
        timeout=5,
    )
    failure_messages += f

    # Send message from channel_2
    content = "message from channel 2"
    await channel_2.send(content)
    _, f = await expect(
        "next_message",
        in_channel=channel_1,
        to={"equal": content, "be_from": bridge_bot},
    )
    failure_messages += f

    return failure_messages


@message_bridging_tests.test
async def between_threads_works(
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

    thread_1 = await channel_1.create_thread(
        name=f"thread_{random.randint(0, 10000)}",
        type=discord.ChannelType.public_thread,
    )
    thread_2 = await channel_2.create_thread(
        name=f"thread_{random.randint(0, 10000)}",
        type=discord.ChannelType.public_thread,
    )

    await create_bridge(thread_1, thread_2.id)

    # Send message from thread_1
    content = "message from thread 1"
    await thread_1.send(content)
    _, failure_messages = await expect(
        "next_message",
        in_channel=thread_2,
        to={"equal": content, "be_from": bridge_bot},
    )

    # Send message from thread_2
    content = "message from thread 2"
    await thread_2.send(content)
    _, f = await expect(
        "next_message",
        in_channel=thread_1,
        to={"equal": content, "be_from": bridge_bot},
    )
    failure_messages += f

    return failure_messages


@message_bridging_tests.test
async def from_thread_to_channel_works(
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

    thread_1 = await channel_1.create_thread(
        name=f"thread_{random.randint(0, 10000)}",
        type=discord.ChannelType.public_thread,
    )

    await create_bridge(thread_1, channel_2.id)

    # Send message from thread_1
    content = "message from thread 1"
    await thread_1.send(content)
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": content, "be_from": bridge_bot},
    )

    # Send message from channel_2
    content = "message from channel 2"
    await channel_2.send(content)
    _, f = await expect(
        "next_message",
        in_channel=thread_1,
        to={"equal": content, "be_from": bridge_bot},
    )
    failure_messages += f

    return failure_messages


@message_bridging_tests.test
async def from_channel_to_thread_works(
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

    thread_2 = await channel_2.create_thread(
        name=f"thread_{random.randint(0, 10000)}",
        type=discord.ChannelType.public_thread,
    )

    await create_bridge(channel_1, thread_2.id)

    # Send message from channel_1
    content = "message from channel 1"
    await channel_1.send(content)
    _, f = await expect(
        "next_message",
        in_channel=thread_2,
        to={"equal": content, "be_from": bridge_bot},
    )
    failure_messages = f

    # Send message from thread_2
    content = "message from thread 2"
    await thread_2.send(content)
    _, f = await expect(
        "next_message",
        in_channel=channel_1,
        to={"equal": content, "be_from": bridge_bot},
    )
    failure_messages += f

    return failure_messages


@message_bridging_tests.test
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
    channel_4 = testing_channels[3]

    # -----
    await demolish_bridges(channel_1)
    await demolish_bridges(channel_2)
    await demolish_bridges(channel_3)
    await demolish_bridges(channel_4)

    # -----
    await create_bridge(channel_1, channel_2)
    await create_bridge(channel_2, channel_3, direction="outbound")
    await create_bridge(channel_4, channel_2, direction="outbound")

    # 1 <-> 2 -> 3
    #      /|\
    #       4

    # -----
    # Send message from channel_1
    content = "message from channel 1"
    await channel_1.send(content)
    failure_messages = []
    expectations = await asyncio.gather(
        expect(
            "no_new_message",
            in_channel=channel_1,
            timeout=5,
        ),
        expect(
            "next_message",
            in_channel=channel_2,
            to={"equal": content, "be_from": bridge_bot},
        ),
        expect(
            "next_message",
            in_channel=channel_3,
            to={"equal": content, "be_from": bridge_bot},
        ),
        expect(
            "no_new_message",
            in_channel=channel_4,
            timeout=5,
        ),
    )
    for _, f in expectations:
        failure_messages += f

    expectations = await asyncio.gather(
        expect(
            "no_new_message",
            in_channel=channel_2,
            timeout=5,
        ),
        expect(
            "no_new_message",
            in_channel=channel_3,
            timeout=5,
        ),
    )
    for _, f in expectations:
        failure_messages += f

    # Send message from channel_2
    content = "message from channel 2"
    await channel_2.send(content)
    expectations = await asyncio.gather(
        expect(
            "next_message",
            in_channel=channel_1,
            to={"equal": content, "be_from": bridge_bot},
        ),
        expect(
            "no_new_message",
            in_channel=channel_2,
            timeout=5,
        ),
        expect(
            "next_message",
            in_channel=channel_3,
            to={"equal": content, "be_from": bridge_bot},
        ),
        expect(
            "no_new_message",
            in_channel=channel_4,
            timeout=5,
        ),
    )
    for _, f in expectations:
        failure_messages += f

    expectations = await asyncio.gather(
        expect(
            "no_new_message",
            in_channel=channel_1,
            timeout=5,
        ),
        expect(
            "no_new_message",
            in_channel=channel_3,
            timeout=5,
        ),
    )
    for _, f in expectations:
        failure_messages += f

    # Send message from channel_3
    content = "message from channel 3"
    await channel_3.send(content)
    expectations = await asyncio.gather(
        expect(
            "no_new_message",
            in_channel=channel_1,
            timeout=5,
        ),
        expect(
            "no_new_message",
            in_channel=channel_2,
            timeout=5,
        ),
        expect(
            "no_new_message",
            in_channel=channel_3,
            timeout=5,
        ),
        expect(
            "no_new_message",
            in_channel=channel_4,
            timeout=5,
        ),
    )
    for _, f in expectations:
        failure_messages += f

    # Send message from channel_4
    content = "message from channel 4"
    await channel_4.send(content)
    expectations = await asyncio.gather(
        expect(
            "next_message",
            in_channel=channel_1,
            to={"equal": content, "be_from": bridge_bot},
        ),
        expect(
            "next_message",
            in_channel=channel_2,
            to={"equal": content, "be_from": bridge_bot},
        ),
        expect(
            "next_message",
            in_channel=channel_3,
            to={"equal": content, "be_from": bridge_bot},
        ),
        expect(
            "no_new_message",
            in_channel=channel_4,
            timeout=5,
        ),
    )
    for _, f in expectations:
        failure_messages += f

    expectations = await asyncio.gather(
        expect(
            "no_new_message",
            in_channel=channel_1,
            timeout=5,
        ),
        expect(
            "no_new_message",
            in_channel=channel_2,
            timeout=5,
        ),
        expect(
            "no_new_message",
            in_channel=channel_3,
            timeout=5,
        ),
    )
    for _, f in expectations:
        failure_messages += f

    return failure_messages


@message_bridging_tests.test
async def works_for_forwards(
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
    await create_bridge(channel_1, channel_2.id)
    await demolish_bridges(channel_3, channel_and_threads=True)

    # Send a message in unbridged channel_3, then forward it to bridged channel_1
    original_message_content = "message to be forwarded"
    original_message = await channel_3.send(original_message_content)
    await original_message.forward(channel_1)

    # Expect header + forwarded message in channel_2
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"contain": "forwarded by", "be_from": bridge_bot},
    )
    _, f = await expect(
        "next_message",
        in_channel=channel_2,
        to={"be_from": bridge_bot, "be_a_forward_of": original_message},
    )
    failure_messages += f

    return failure_messages


@message_bridging_tests.test
async def blocks_forwarding_from_nsfw_channels(
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

    # Mark channels 1 and 3 as NSFW and 2 as SFW
    await set_nsfw({channel_1.id: True, channel_2.id: False, channel_3.id: True})

    # Send a message in NSFW channel_3, then forward it to NSFW channel_1
    original_message = await channel_3.send("nsfw content")
    await original_message.forward(channel_1)

    # Expect error message in channel_2 about NSFW blocking
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"contain": "NSFW channel"},
    )

    # Restore channels 1 and 3 to SFW
    await set_nsfw({channel_1.id: False, channel_3.id: False})

    return failure_messages


@message_bridging_tests.test
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
    await demolish_bridges(channel_1, channel_2)
    await create_bridge(channel_1, channel_2.id)

    # Verify bridge works
    content = "should be bridged"
    await channel_1.send(content)
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": content, "be_from": bridge_bot},
    )

    # Demolish bridge
    await demolish_bridges(channel_1, channel_2.id)

    # Verify messages no longer forward
    await channel_1.send("should not be bridged")
    _, f = await expect("no_new_message", in_channel=channel_2, timeout=5)
    failure_messages += f

    await channel_2.send("should not be bridged either")
    _, f = await expect("no_new_message", in_channel=channel_1, timeout=5)
    failure_messages += f

    return failure_messages


@message_bridging_tests.test
async def does_not_work_when_all_bridges_demolished(
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

    # Demolish all bridges
    await demolish_bridges(channel_1, channel_and_threads=True)

    # Send message from channel_1 and thread_1
    await asyncio.gather(
        channel_1.send("message from channel 1"),
        thread_1.send("message from thread 1"),
    )
    failure_messages = []
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
