from pathlib import Path

import discord
import test_runner
from test_runner import (
    create_bridge,
    demolish_bridges,
    expect,
    give_manage_webhook_perms,
)

ASSET_PATH = Path(__file__).parent.parent / "assets" / "test_file.txt"


class BridgingEdits(test_runner.TestCase):
    def __init__(self):
        super().__init__(test_runner.test_runner)


edit_bridging_tests = BridgingEdits()


@edit_bridging_tests.test
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
    original_content = "original message"
    original_message = await channel_1.send(original_content)
    bridged_message, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": original_content, "be_from": bridge_bot},
    )
    if not bridged_message:
        return failure_messages

    # Edit message
    edited_content = "edited message"
    await original_message.edit(content=edited_content)
    _, failure_messages = await expect(
        bridged_message,
        to={"be_edited": True, "equal": edited_content},
    )

    return failure_messages


@edit_bridging_tests.test
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
    original_content = "original message"
    original_message = await channel_1.send(original_content)

    bridged_message_2, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": original_content, "be_from": bridge_bot},
    )

    bridged_message_3, f = await expect(
        "next_message",
        in_channel=channel_3,
        to={"equal": original_content, "be_from": bridge_bot},
    )
    failure_messages += f

    if not (bridged_message_2 or bridged_message_3):
        return failure_messages

    # Edit message
    edited_content = "edited message"
    await original_message.edit(content=edited_content)

    if bridged_message_2:
        _, f = await expect(
            bridged_message_2,
            to={"be_edited": True, "equal": edited_content},
        )
        failure_messages += f
    if bridged_message_3:
        _, f = await expect(
            bridged_message_3,
            to={"be_edited": True, "equal": edited_content},
        )
        failure_messages += f

    return failure_messages


@edit_bridging_tests.test
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
    original_content = "original message"
    original_message = await channel_1.send(original_content)
    bridged_message, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": original_content, "be_from": bridge_bot},
    )
    if not bridged_message:
        return failure_messages

    # Demolish bridge
    await demolish_bridges(channel_1, channel_and_threads=True)

    # Edit message
    edited_content = "edited message"
    await original_message.edit(content=edited_content)
    _, failure_messages = await expect(
        bridged_message,
        to="not_be_edited",
        timeout=5,
    )

    # Recreate bridge
    await create_bridge(channel_1, channel_2.id)
    edited_content = "edited message 2"
    await original_message.edit(content=edited_content)
    _, f = await expect(
        bridged_message,
        to="not_be_edited",
        timeout=5,
    )
    failure_messages += f

    return failure_messages


@edit_bridging_tests.test
async def allows_no_content_if_attachments_present(
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

    # Send message with text and attachment
    original_message = await channel_1.send("hello", file=discord.File(ASSET_PATH))
    bridged_message, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={
            "equal": "hello",
            "be_from": bridge_bot,
            "have_attachment": {"whose_filename_equals": "test_file.txt"},
        },
    )
    if not bridged_message:
        return failure_messages

    # Edit to remove text content (keep attachment)
    await original_message.edit(content="")
    _, f = await expect(
        bridged_message,
        to={
            "be_edited": True,
            "equal": "",
            "have_attachment": {"whose_filename_equals": "test_file.txt"},
        },
    )
    failure_messages += f

    return failure_messages
