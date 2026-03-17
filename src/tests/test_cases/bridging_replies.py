from pathlib import Path

import discord
import test_runner
from test_runner import (
    create_bridge,
    demolish_bridges,
    expect,
    give_manage_webhook_perms,
)

import common

ASSET_PATH = Path(__file__).parent.parent / "assets" / "test_file.txt"


class BridgingReplies(test_runner.TestCase):
    order = 50
    dependencies = ["CreatingBridges", "DemolishingBridges", "BridgingMessages"]

    def __init__(self):
        super().__init__(test_runner.test_runner)


reply_bridging_tests = BridgingReplies()


@reply_bridging_tests.test
async def warns_when_original_is_not_bridged(
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

    # Send unbridged message to channel_1
    original_message_content = "unbridged message 1"
    original_message = await channel_1.send(original_message_content)

    # Bridge channels
    await create_bridge(channel_1, channel_2.id)

    # reply to unbridged message
    reply_content = "reply to unbridged message"
    await original_message.reply(reply_content)
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to=[
            {
                "equal": reply_content,
                "be_from": bridge_bot,
                "have_embed": {
                    "not_have_url": True,
                    # "whose_description_contains": "-# The message being replied to has not been bridged or has been deleted."
                    # TODO: uncomment once async sessions land and the warning text is enabled
                },
            },
            {
                "have_embed": {
                    "whose_description_contains": common.truncate(
                        original_message_content,
                        50,
                    )
                }
            },
        ],
    )

    return failure_messages


@reply_bridging_tests.test
async def warns_when_bridged_message_was_deleted(
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

    # Send message to channel_1
    original_message_content = "original message"
    original_message = await channel_1.send(original_message_content)
    bridged_original_message, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": original_message_content, "be_from": bridge_bot},
    )

    if not bridged_original_message:
        return failure_messages
    await bridged_original_message.delete()

    # reply to original message whose bridged version has been deleted
    reply_content = "reply to deleted message"
    await original_message.reply(reply_content)
    _, f = await expect(
        "next_message",
        in_channel=channel_2,
        to=[
            {
                "equal": reply_content,
                "be_from": bridge_bot,
                "have_embed": {
                    "not_have_url": True,
                    # "whose_description_contains": "-# The message being replied to has not been bridged or has been deleted."
                    # TODO: uncomment once async sessions land and the warning text is enabled
                },
            },
            {
                "have_embed": {
                    "whose_description_contains": common.truncate(
                        original_message_content,
                        50,
                    )
                }
            },
        ],
    )
    failure_messages += f

    return failure_messages


@reply_bridging_tests.test
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

    # Send message to channel_1
    original_message_content = "original message"
    original_message = await channel_1.send(original_message_content)
    bridged_original_message, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": original_message_content, "be_from": bridge_bot},
    )
    if not bridged_original_message:
        return failure_messages

    #
    reply_content = "reply to original message in original channel"
    await original_message.reply(reply_content)
    _, f = await expect(
        "next_message",
        in_channel=channel_2,
        to=[
            {
                "equal": reply_content,
                "be_from": bridge_bot,
                "have_embed": {
                    "whose_description_contains": f"(https://discord.com/channels/{channel_2.guild.id}/{channel_2.id}/{bridged_original_message.id})",
                    "whose_url_equals": f"https://discord.com/channels/{channel_2.guild.id}/{channel_2.id}/{bridged_original_message.id}",
                },
            },
            {
                "have_embed": {
                    "whose_description_contains": common.truncate(
                        original_message_content,
                        50,
                    )
                }
            },
        ],
    )
    failure_messages += f

    #
    reply_content = "reply to bridged version of original message"
    await bridged_original_message.reply(reply_content)
    _, f = await expect(
        "next_message",
        in_channel=channel_1,
        to=[
            {
                "equal": reply_content,
                "be_from": bridge_bot,
                "have_embed": {
                    "whose_description_contains": f"(https://discord.com/channels/{channel_1.guild.id}/{channel_1.id}/{original_message.id})",
                    "whose_url_equals": f"https://discord.com/channels/{channel_1.guild.id}/{channel_1.id}/{original_message.id}",
                },
            },
            {
                "have_embed": {
                    "whose_description_contains": common.truncate(
                        original_message_content,
                        50,
                    )
                }
            },
        ],
    )
    failure_messages += f

    return failure_messages


@reply_bridging_tests.test
async def truncates_message_length_correctly(
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

    # Send message to channel_1
    original_message_content = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Duis placerat sed eros quis sagittis. Praesent eu rhoncus lectus, ac facilisis nisl. Nunc vel justo sit amet mi tempus volutpat id ac dui. Sed condimentum vitae sapien id luctus. Nam sodales hendrerit nunc, vitae dapibus elit imperdiet in. Nam eu diam id enim fringilla ultrices. Nunc consequat finibus magna elementum iaculis. Vestibulum id pulvinar augue. Vestibulum imperdiet mattis leo nec ullamcorper. Sed tristique est eget pulvinar volutpat. Nulla posuere, est pretium placerat suscipit, risus erat pulvinar urna, ut dictum ligula orci vitae nunc. Cras bibendum massa lorem, nec auctor nisi viverra."
    original_message = await channel_1.send(original_message_content)
    bridged_original_message, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": original_message_content, "be_from": bridge_bot},
    )
    if not bridged_original_message:
        return failure_messages

    #
    reply_content = "reply to original message in original channel"
    await original_message.reply(reply_content)
    _, f = await expect(
        "next_message",
        in_channel=channel_2,
        to=[
            {
                "equal": reply_content,
                "be_from": bridge_bot,
                "have_embed": {
                    "whose_description_contains": f"(https://discord.com/channels/{channel_2.guild.id}/{channel_2.id}/{bridged_original_message.id})",
                    "whose_url_equals": f"https://discord.com/channels/{channel_2.guild.id}/{channel_2.id}/{bridged_original_message.id}",
                },
            },
            {
                "have_embed": {
                    "whose_description_contains": common.truncate(
                        original_message_content,
                        50,
                    )
                }
            },
        ],
    )
    failure_messages += f

    return failure_messages


@reply_bridging_tests.test
async def works_when_replying_to_forward(
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
    original_forwarded_content = "original forwarded content"
    original_message = await channel_3.send(original_forwarded_content)
    forwarded_message = await original_message.forward(channel_1)

    # Wait for header + forwarded message in channel_2
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"contain": "forwarded by", "be_from": bridge_bot},
    )
    bridged_forward, f = await expect(
        "next_message",
        in_channel=channel_2,
        to={"be_from": bridge_bot, "be_a_forward_of": original_message},
    )
    failure_messages += f
    if not bridged_forward:
        return failure_messages

    # Reply to the forwarded message in channel_1
    reply_content = "reply to the forward"
    await forwarded_message.reply(reply_content)
    _, f = await expect(
        "next_message",
        in_channel=channel_2,
        to=[
            {
                "equal": reply_content,
                "be_from": bridge_bot,
                "have_embed": {
                    "whose_url_equals": f"https://discord.com/channels/{channel_2.guild.id}/{channel_2.id}/{bridged_forward.id}",
                },
            },
            {
                "have_embed": {
                    "whose_description_contains": "↱ "
                    + common.truncate(
                        original_forwarded_content,
                        50,
                    )
                }
            },
        ],
    )
    failure_messages += f

    return failure_messages


@reply_bridging_tests.test
async def works_with_attachment_only_message(
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

    # Send attachment-only message (no text)
    original_message = await channel_1.send(file=discord.File(ASSET_PATH))
    bridged_original, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={
            "be_from": bridge_bot,
            "have_attachment": {"whose_filename_equals": "test_file.txt"},
        },
    )
    if not bridged_original:
        return failure_messages

    # Reply to the attachment-only message
    reply_content = "replying to attachment"
    await original_message.reply(reply_content)
    _, f = await expect(
        "next_message",
        in_channel=channel_2,
        to=[
            {
                "equal": reply_content,
                "be_from": bridge_bot,
                "have_embed": {
                    "whose_url_equals": f"https://discord.com/channels/{channel_2.guild.id}/{channel_2.id}/{bridged_original.id}",
                },
            },
            {"have_embed": {"whose_description_contains": "*Click to see attachment*"}},
        ],
    )
    failure_messages += f

    return failure_messages


@reply_bridging_tests.test
async def works_with_forward_of_attachment_only_message(
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

    # Send attachment-only message in unbridged channel_3, then forward to channel_1
    original_message = await channel_3.send(file=discord.File(ASSET_PATH))
    forwarded_message = await original_message.forward(channel_1)

    # Wait for header + forwarded message in channel_2
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"contain": "forwarded by", "be_from": bridge_bot},
    )
    bridged_forward, f = await expect(
        "next_message",
        in_channel=channel_2,
        to={"be_from": bridge_bot, "be_a_forward_of": original_message},
    )
    failure_messages += f
    if not bridged_forward:
        return failure_messages

    # Reply to the forwarded message in channel_1
    reply_content = "replying to forwarded attachment"
    await forwarded_message.reply(reply_content)
    _, f = await expect(
        "next_message",
        in_channel=channel_2,
        to=[
            {
                "equal": reply_content,
                "be_from": bridge_bot,
                "have_embed": {
                    "whose_url_equals": f"https://discord.com/channels/{channel_2.guild.id}/{channel_2.id}/{bridged_forward.id}",
                },
            },
            {
                "have_embed": {
                    "whose_description_contains": "↱ *Click to see attachment*"
                }
            },
        ],
    )
    failure_messages += f

    return failure_messages
