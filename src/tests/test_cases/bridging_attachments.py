import io
import sys
from pathlib import Path

import discord

sys.path.append(str(Path(__file__).parent.parent))
import test_runner
from test_runner import create_bridge, expect, give_manage_webhook_perms

ASSET_PATH = Path(__file__).parent.parent / "assets" / "test_file.txt"


class BridgingAttachments(test_runner.TestCase):
    def __init__(self):
        super().__init__(test_runner.test_runner)


attachment_bridging_tests = BridgingAttachments()


@attachment_bridging_tests.test
async def works_without_text(
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

    # Send message with only an attachment
    await channel_1.send(file=discord.File(ASSET_PATH))
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={
            "be_from": bridge_bot,
            "have_attachment": {"whose_filename_equals": "test_file.txt"},
        },
    )

    return failure_messages


@attachment_bridging_tests.test
async def works_with_text(
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

    # Send message with text and an attachment
    await channel_1.send("hello", file=discord.File(ASSET_PATH))
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={
            "equal": "hello",
            "be_from": bridge_bot,
            "have_attachment": {"whose_filename_equals": "test_file.txt"},
        },
    )

    return failure_messages


@attachment_bridging_tests.test
async def preserve_spoiler(
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

    # Send message with a spoiler attachment
    await channel_1.send(file=discord.File(ASSET_PATH, spoiler=True))
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={
            "be_from": bridge_bot,
            "have_attachment": {
                "whose_filename_contains": "test_file",
                "be_spoiler": True,
            },
        },
    )

    await channel_1.send(file=discord.File(ASSET_PATH, spoiler=False))
    _, f = await expect(
        "next_message",
        in_channel=channel_2,
        to={
            "be_from": bridge_bot,
            "have_attachment": {
                "whose_filename_contains": "test_file",
                "be_spoiler": False,
            },
        },
    )
    failure_messages += f

    return failure_messages


@attachment_bridging_tests.test
async def works_with_multiple_attachments(
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

    # Send message with multiple attachments
    await channel_1.send(
        files=[
            discord.File(ASSET_PATH),
            discord.File(io.BytesIO(b"second file contents"), filename="second.txt"),
        ]
    )
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={
            "be_from": bridge_bot,
            "have_attachments": [
                {"whose_filename_equals": "test_file.txt"},
                {"whose_filename_equals": "second.txt"},
            ],
        },
    )

    return failure_messages
