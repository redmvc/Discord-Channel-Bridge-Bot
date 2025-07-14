import asyncio
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


class ChannelBridgingTests(test_runner.TestCase):
    def __init__(self):
        super().__init__(test_runner.test_runner)


channel_bridging_tests = ChannelBridgingTests()


@channel_bridging_tests.test
async def creating_bridge_requires_valid_channel(
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
    message_sent = await create_bridge(channel_1, 1234, send_message=True)
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Unsupported argument passed. Please pass a channel reference, ID, or link.",
        },
    )
    return failure_messages


@channel_bridging_tests.test
async def creating_bridge_requires_different_channel(
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
    message_sent = await create_bridge(channel_1, channel_1, send_message=True)
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "You can't bridge a channel to itself.",
        },
    )

    return failure_messages


@channel_bridging_tests.test
async def creating_bridge_requires_manage_webhook_permissions(
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
    message_sent = await create_bridge(channel_1, channel_2.id, send_message=True)

    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Please make sure both you and the bot have 'Manage Webhooks' permission in both this and target channels.",
        },
    )

    return failure_messages


@channel_bridging_tests.test
async def creating_two_way_bridges_works(
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

    # Create bridge
    message_sent = await create_bridge(channel_1, channel_2.id, send_message=True)
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
            "contain": "Bridge created! Try sending a message from either channel",
        },
    )
    failure_messages += f

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
        "next_message",
        in_channel=channel_1,
        to={"equal": content, "be_from": bridge_bot},
    )
    failure_messages += f

    return failure_messages


@channel_bridging_tests.test
async def creating_outbound_bridges_works(
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

    channel_1 = testing_channels[1]
    channel_2 = testing_channels[2]
    await demolish_bridges(channel_1, channel_2)

    # Create bridge
    message_sent = await create_bridge(
        channel_1,
        channel_2.id,
        direction="outbound",
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
            "contain": "Bridge created! Try sending a message from this channel",
        },
    )
    failure_messages += f

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
        heartbeat=5,
    )
    failure_messages += f

    return failure_messages


@channel_bridging_tests.test
async def creating_inbound_bridges_works(
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

    channel_1 = testing_channels[1]
    channel_2 = testing_channels[3]
    await demolish_bridges(channel_1, channel_2)

    # Create bridge
    message_sent = await create_bridge(
        channel_1,
        channel_2.id,
        direction="inbound",
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
            "contain": "Bridge created! Try sending a message from the other channel",
        },
    )
    failure_messages += f

    # Send message from channel_1
    content = "message from channel 1"
    await channel_1.send(content)
    _, f = await expect(
        "no_new_message",
        in_channel=channel_2,
        timeout=5,
        heartbeat=5,
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


@channel_bridging_tests.test
async def bridge_chains_work(
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
            heartbeat=5,
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
            heartbeat=5,
        ),
    )
    for _, f in expectations:
        failure_messages += f

    expectations = await asyncio.gather(
        expect(
            "no_new_message",
            in_channel=channel_2,
            timeout=5,
            heartbeat=5,
        ),
        expect(
            "no_new_message",
            in_channel=channel_3,
            timeout=5,
            heartbeat=5,
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
            heartbeat=5,
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
            heartbeat=5,
        ),
    )
    for _, f in expectations:
        failure_messages += f

    expectations = await asyncio.gather(
        expect(
            "no_new_message",
            in_channel=channel_1,
            timeout=5,
            heartbeat=5,
        ),
        expect(
            "no_new_message",
            in_channel=channel_3,
            timeout=5,
            heartbeat=5,
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
            heartbeat=5,
        ),
        expect(
            "no_new_message",
            in_channel=channel_2,
            timeout=5,
            heartbeat=5,
        ),
        expect(
            "no_new_message",
            in_channel=channel_3,
            timeout=5,
            heartbeat=5,
        ),
        expect(
            "no_new_message",
            in_channel=channel_4,
            timeout=5,
            heartbeat=5,
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
            heartbeat=5,
        ),
    )
    for _, f in expectations:
        failure_messages += f

    expectations = await asyncio.gather(
        expect(
            "no_new_message",
            in_channel=channel_1,
            timeout=5,
            heartbeat=5,
        ),
        expect(
            "no_new_message",
            in_channel=channel_2,
            timeout=5,
            heartbeat=5,
        ),
        expect(
            "no_new_message",
            in_channel=channel_3,
            timeout=5,
            heartbeat=5,
        ),
    )
    for _, f in expectations:
        failure_messages += f

    return failure_messages


@channel_bridging_tests.test
async def demolishing_bridges_requires_valid_channel(
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


@channel_bridging_tests.test
async def demolishing_bridges_requires_manage_webhook_permissions(
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


@channel_bridging_tests.test
async def demolishing_bridges_requires_bridges_to_exist(
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


@channel_bridging_tests.test
async def demolishing_bridges_works(
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
    _, f = await expect("no_new_message", in_channel=channel_2, timeout=5, heartbeat=5)
    failure_messages += f

    # Send message from channel_2
    content = "message from channel 2"
    await channel_2.send(content)
    _, f = await expect("no_new_message", in_channel=channel_1, timeout=5, heartbeat=5)
    failure_messages += f

    return failure_messages
