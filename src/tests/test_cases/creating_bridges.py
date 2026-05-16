import discord
import test_runner
from test_runner import (
    create_bridge,
    demolish_bridges,
    expect,
    give_manage_webhook_perms,
    remove_manage_webhook_perms,
)


class CreatingBridges(test_runner.TestCase):
    order = 10

    def __init__(self):
        super().__init__(test_runner.test_runner)


bridge_creation_tests = CreatingBridges()


@bridge_creation_tests.test
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
    message_sent = await create_bridge(channel_1, 1234, send_message=True)
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Unsupported argument passed. Please pass a channel reference, ID, or link.",
            "be_ephemeral": True,
        },
    )
    return failure_messages


@bridge_creation_tests.test
async def requires_target_channel_to_be_different(
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
            "be_ephemeral": True,
        },
    )

    return failure_messages


@bridge_creation_tests.test
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
    message_sent = await create_bridge(channel_1, channel_2.id, send_message=True)

    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Please make sure both you and the bot have 'Manage Webhooks' permission in both this and target channels.",
            "be_ephemeral": True,
        },
    )

    return failure_messages


@bridge_creation_tests.test
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

    # -----
    # Two-way bridge
    await demolish_bridges(channel_1, channel_2)
    message_sent = await create_bridge(channel_1, channel_2.id, send_message=True)
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Interaction was deferred with thinking = True.",
            "be_ephemeral": True,
        },
    )
    _, f = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Bridge created! Try sending a message from either channel",
            "be_ephemeral": True,
        },
    )
    failure_messages += f

    # -----
    # Outbound bridge
    await demolish_bridges(channel_1, channel_2)
    message_sent = await create_bridge(
        channel_1,
        channel_2.id,
        direction="outbound",
        send_message=True,
    )
    _, f = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Interaction was deferred with thinking = True.",
            "be_ephemeral": True,
        },
    )
    failure_messages += f
    _, f = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Bridge created! Try sending a message from this channel",
            "be_ephemeral": True,
        },
    )
    failure_messages += f

    # -----
    # Inbound bridge
    await demolish_bridges(channel_1, channel_2)
    message_sent = await create_bridge(
        channel_1,
        channel_2.id,
        direction="inbound",
        send_message=True,
    )
    _, f = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Interaction was deferred with thinking = True.",
            "be_ephemeral": True,
        },
    )
    failure_messages += f
    _, f = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Bridge created! Try sending a message from the other channel",
            "be_ephemeral": True,
        },
    )
    failure_messages += f

    return failure_messages


@bridge_creation_tests.test
async def rejects_duplicate_two_way(
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

    # Pre-create a two-way bridge
    await create_bridge(channel_1, channel_2.id)

    duplicate_message = (
        "A two-way bridge between this and target channels already exists."
    )
    failure_messages: list[str] = []

    # Try two-way again
    message_sent = await create_bridge(channel_1, channel_2.id, send_message=True)
    _, f = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": duplicate_message,
            "be_ephemeral": True,
        },
    )
    failure_messages += f

    # Try outbound
    message_sent = await create_bridge(
        channel_1,
        channel_2.id,
        direction="outbound",
        send_message=True,
    )
    _, f = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": duplicate_message,
            "be_ephemeral": True,
        },
    )
    failure_messages += f

    # Try inbound
    message_sent = await create_bridge(
        channel_1,
        channel_2.id,
        direction="inbound",
        send_message=True,
    )
    _, f = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": duplicate_message,
            "be_ephemeral": True,
        },
    )
    failure_messages += f

    return failure_messages


@bridge_creation_tests.test
async def rejects_duplicate_outbound(
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

    # Pre-create an outbound bridge
    await create_bridge(channel_1, channel_2.id, direction="outbound")

    # Try outbound again
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
            "equal": (
                "❕ An outbound bridge from this channel to target channel already exists."
            ),
            "be_ephemeral": True,
        },
    )

    return failure_messages


@bridge_creation_tests.test
async def rejects_duplicate_inbound(
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

    # Pre-create an inbound bridge
    await create_bridge(channel_1, channel_2.id, direction="inbound")

    # Try inbound again
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
            "equal": (
                "❕ An inbound bridge to this channel from target channel already exists."
            ),
            "be_ephemeral": True,
        },
    )

    return failure_messages


@bridge_creation_tests.test
async def completes_two_way_from_outbound(
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

    # Pre-create an outbound bridge (channel_1 → channel_2)
    await create_bridge(channel_1, channel_2.id, direction="outbound")

    # Request a two-way bridge — should send a notice then create the missing inbound half
    message_sent = await create_bridge(channel_1, channel_2.id, send_message=True)

    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": (
                "An outbound bridge from this channel to target channel already exists. Creating inbound bridge."
            ),
            "be_ephemeral": True,
        },
    )

    _, f = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Interaction was deferred with thinking = True.",
            "be_ephemeral": True,
        },
    )
    failure_messages += f

    _, f = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Bridge created!",
            "be_ephemeral": True,
        },
    )
    failure_messages += f

    # Verify the new inbound half works: send from channel_2, expect bridged copy in channel_1
    content = "message from channel 2 after autocomplete"
    await channel_2.send(content)
    _, f = await expect(
        "next_message",
        in_channel=channel_1,
        to={"equal": content, "be_from": bridge_bot},
    )
    failure_messages += f

    return failure_messages


@bridge_creation_tests.test
async def completes_two_way_from_inbound(
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

    # Pre-create an inbound bridge (channel_2 → channel_1)
    await create_bridge(channel_1, channel_2.id, direction="inbound")

    # Request a two-way bridge — should send a notice then create the missing outbound half
    message_sent = await create_bridge(channel_1, channel_2.id, send_message=True)

    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": (
                "An inbound bridge to this channel from target channel already exists. Creating outbound bridge."
            ),
            "be_ephemeral": True,
        },
    )

    _, f = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Interaction was deferred with thinking = True.",
            "be_ephemeral": True,
        },
    )
    failure_messages += f

    _, f = await expect(
        "next_message",
        in_channel=channel_1,
        to={
            "be_a_reply_to": message_sent,
            "contain": "Bridge created!",
            "be_ephemeral": True,
        },
    )
    failure_messages += f

    # Verify the new outbound half works: send from channel_1, expect bridged copy in channel_2
    content = "message from channel 1 after autocomplete"
    await channel_1.send(content)
    _, f = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": content, "be_from": bridge_bot},
    )
    failure_messages += f

    return failure_messages
