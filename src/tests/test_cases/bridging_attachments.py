import io
import sys
from pathlib import Path
from unittest.mock import PropertyMock, patch

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
async def preserves_spoiler(
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


@attachment_bridging_tests.test
async def drops_oversized_attachment(
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

    # Patch filesize_limit to 100 bytes so our attachment is "too large"
    with patch.object(
        discord.Guild,
        "filesize_limit",
        new_callable=PropertyMock,
        return_value=100,
    ):
        await channel_1.send(
            file=discord.File(io.BytesIO(b"\x00" * 100), filename="big.bin")
        )
        _, failure_messages = await expect(
            "next_message",
            in_channel=channel_2,
            to={
                "be_from": bridge_bot,
                "not_have_attachment": True,
                "have_embed": {
                    "whose_description_contains": "could not be sent due to message size limits",
                },
            },
        )

    return failure_messages


@attachment_bridging_tests.test
async def drops_when_cumulative_too_large(
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

    # Patch filesize_limit to 100 bytes; send three ~40-byte files
    # First two fit (cumulative 80), third would push to 120 and gets dropped
    with patch.object(
        discord.Guild,
        "filesize_limit",
        new_callable=PropertyMock,
        return_value=100,
    ):
        await channel_1.send(
            files=[
                discord.File(io.BytesIO(b"\x00" * 40), filename="part1.bin"),
                discord.File(io.BytesIO(b"\x00" * 40), filename="part2.bin"),
                discord.File(io.BytesIO(b"\x00" * 40), filename="part3.bin"),
                discord.File(io.BytesIO(b"\x00" * 10), filename="part4.bin"),
            ]
        )
        _, failure_messages = await expect(
            "next_message",
            in_channel=channel_2,
            to={
                "be_from": bridge_bot,
                "have_attachments": [
                    {"whose_filename_equals": "part1.bin"},
                    {"whose_filename_equals": "part2.bin"},
                    {"whose_filename_equals": "part4.bin"},
                ],
                "have_embed": {
                    "whose_description_contains": "could not be sent due to message size limits",
                },
            },
        )

    return failure_messages


@attachment_bridging_tests.test
async def drops_oversized_with_real_large_files(
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

    # Send two ~6MB files — each individually < 10MB limit, but cumulative ~12MB > 10MB
    await channel_1.send(
        files=[
            discord.File(io.BytesIO(b"\x00" * 6_000_000), filename="large1.bin"),
            discord.File(io.BytesIO(b"\x00" * 6_000_000), filename="large2.bin"),
        ]
    )
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={
            "be_from": bridge_bot,
            "have_attachments": [
                {"whose_filename_equals": "large1.bin"},
            ],
            "have_embed": {
                "whose_description_contains": "could not be sent due to message size limits",
            },
        },
        timeout=20,
    )

    return failure_messages
