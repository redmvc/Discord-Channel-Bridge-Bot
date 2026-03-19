import asyncio

import discord
import test_runner
from test_runner import (
    create_bridge,
    demolish_bridges,
    expect,
    give_manage_webhook_perms,
)


class BridgingReactions(test_runner.TestCase):
    order = 80
    dependencies = ["CreatingBridges", "DemolishingBridges", "BridgingMessages"]

    def __init__(self):
        super().__init__(test_runner.test_runner)


reaction_bridging_tests = BridgingReactions()


@reaction_bridging_tests.test
async def adds_reactions(
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
    original_message = await channel_1.send("react to this")
    bridged_message, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": "react to this", "be_from": bridge_bot},
    )
    if not bridged_message:
        return failure_messages

    # Add reaction to original message
    await original_message.add_reaction("\N{THUMBS UP SIGN}")
    _, f = await expect(
        bridged_message,
        to={"get_reaction": {"emoji": "\N{THUMBS UP SIGN}"}},
    )
    failure_messages += f

    # Add reaction to bridged message
    await bridged_message.add_reaction("\N{HEAVY BLACK HEART}")
    _, f = await expect(
        original_message,
        to={"get_reaction": {"emoji": "\N{HEAVY BLACK HEART}"}},
    )
    failure_messages += f

    return failure_messages


@reaction_bridging_tests.test
async def removes_reactions(
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
    original_message = await channel_1.send("react then unreact")
    bridged_message, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": "react then unreact", "be_from": bridge_bot},
    )
    if not bridged_message:
        return failure_messages

    # Add reaction and wait for it to be bridged
    await original_message.add_reaction("\N{THUMBS UP SIGN}")
    _, f = await expect(
        bridged_message,
        to={"get_reaction": {"emoji": "\N{THUMBS UP SIGN}"}},
    )
    failure_messages += f

    # Remove reaction
    assert tester_bot.user
    await original_message.remove_reaction("\N{THUMBS UP SIGN}", tester_bot.user)
    _, f = await expect(
        bridged_message,
        to={"have_reaction_removed": {"emoji": "\N{THUMBS UP SIGN}"}},
    )
    failure_messages += f

    return failure_messages


@reaction_bridging_tests.test
async def works_when_clearing_one_emoji(
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
    original_message = await channel_1.send("clear one emoji")
    bridged_message, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": "clear one emoji", "be_from": bridge_bot},
    )
    if not bridged_message:
        return failure_messages

    # Add two reactions
    await original_message.add_reaction("\N{THUMBS UP SIGN}")
    _, f = await expect(
        bridged_message,
        to={"get_reaction": {"emoji": "\N{THUMBS UP SIGN}"}},
    )
    failure_messages += f

    await original_message.add_reaction("\N{HEAVY BLACK HEART}")
    _, f = await expect(
        bridged_message,
        to={"get_reaction": {"emoji": "\N{HEAVY BLACK HEART}"}},
    )
    failure_messages += f

    # Clear only the thumbs up
    await original_message.clear_reaction("\N{THUMBS UP SIGN}")
    _, f = await expect(
        bridged_message,
        to={"have_reaction_removed": {"emoji": "\N{THUMBS UP SIGN}"}},
    )
    failure_messages += f

    return failure_messages


@reaction_bridging_tests.test
async def works_when_clearing_all_reactions(
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
    original_message = await channel_1.send("clear all reactions")
    bridged_message, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": "clear all reactions", "be_from": bridge_bot},
    )
    if not bridged_message:
        return failure_messages

    # Add two reactions
    await original_message.add_reaction("\N{THUMBS UP SIGN}")
    _, f = await expect(
        bridged_message,
        to={"get_reaction": {"emoji": "\N{THUMBS UP SIGN}"}},
    )
    failure_messages += f

    await original_message.add_reaction("\N{HEAVY BLACK HEART}")
    _, f = await expect(
        bridged_message,
        to={"get_reaction": {"emoji": "\N{HEAVY BLACK HEART}"}},
    )
    failure_messages += f

    # Clear all reactions
    await original_message.clear_reactions()
    _, f = await expect(
        bridged_message,
        to={"have_reaction_removed": {"emoji": "\N{THUMBS UP SIGN}"}},
    )
    failure_messages += f

    _, f = await expect(
        bridged_message,
        to={"have_reaction_removed": {"emoji": "\N{HEAVY BLACK HEART}"}},
    )
    failure_messages += f

    return failure_messages


@reaction_bridging_tests.test
async def only_removes_when_all_sources_unreact(
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

    # Send message in channel_1, wait for it to arrive in channel_2 and channel_3
    original_message = await channel_1.send("partial unreact test")
    bridged_msg_2, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": "partial unreact test", "be_from": bridge_bot},
    )
    bridged_msg_3, f = await expect(
        "next_message",
        in_channel=channel_3,
        to={"equal": "partial unreact test", "be_from": bridge_bot},
    )
    failure_messages += f
    if not (bridged_msg_2 and bridged_msg_3):
        return failure_messages

    # React in channel_1 → bridge bot adds reaction in channels 2 and 3
    await original_message.add_reaction("\N{THUMBS UP SIGN}")
    _, f = await expect(
        bridged_msg_2,
        to={"get_reaction": {"emoji": "\N{THUMBS UP SIGN}"}},
    )
    failure_messages += f
    _, f = await expect(
        bridged_msg_3,
        to={"get_reaction": {"emoji": "\N{THUMBS UP SIGN}"}},
    )
    failure_messages += f

    # React in channel_3 → bridge bot adds reaction in channels 1 and 2
    await bridged_msg_3.add_reaction("\N{THUMBS UP SIGN}")
    _, f = await expect(
        original_message,
        to={"get_reaction": {"emoji": "\N{THUMBS UP SIGN}"}},
    )
    failure_messages += f
    _, f = await expect(bridged_msg_2, to="have_no_new_reaction", timeout=5)
    failure_messages += f

    # Remove reaction from channel_1 only
    assert (tester_bot_user := tester_bot.user) and (bridge_bot_user := bridge_bot.user)
    await original_message.remove_reaction("\N{THUMBS UP SIGN}", tester_bot_user)

    # Channel 3: bridge bot should remove its reaction (originated from channel_1)
    _, f = await expect(
        bridged_msg_3,
        to={
            "have_reaction_removed": {
                "emoji": "\N{THUMBS UP SIGN}",
                "from_user": bridge_bot_user.id,
            }
        },
    )
    failure_messages += f

    # Wait for the removal to propagate, then verify channels 1 and 2 still have the reaction
    # (because channel_3's user reaction is still active)
    await asyncio.sleep(1)

    _, f = await expect(
        original_message,
        to={
            "still_have_reaction": {
                "emoji": "\N{THUMBS UP SIGN}",
                "from_user": bridge_bot_user.id,
            }
        },
    )
    failure_messages += f

    _, f = await expect(
        bridged_msg_2,
        to={
            "still_have_reaction": {
                "emoji": "\N{THUMBS UP SIGN}",
                "from_user": bridge_bot_user.id,
            }
        },
    )
    failure_messages += f

    _, f = await expect(
        bridged_msg_3,
        to={
            "still_have_reaction": {
                "emoji": "\N{THUMBS UP SIGN}",
                "from_user": tester_bot_user.id,
            }
        },
    )
    failure_messages += f

    return failure_messages


@reaction_bridging_tests.test
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
    original_message = await channel_1.send("react after demolish")
    bridged_message, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": "react after demolish", "be_from": bridge_bot},
    )
    if not bridged_message:
        return failure_messages

    # Demolish bridge
    await demolish_bridges(channel_1, channel_and_threads=True)

    # Add reaction — should not be bridged
    await original_message.add_reaction("\N{THUMBS UP SIGN}")
    _, f = await expect(bridged_message, to="have_no_new_reaction", timeout=5)
    failure_messages += f

    return failure_messages
