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
