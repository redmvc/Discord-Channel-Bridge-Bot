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


class BridgingReactions(test_runner.TestCase):
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

    # Add reaction
    await original_message.add_reaction("\N{THUMBS UP SIGN}")
    _, f = await expect(
        bridged_message,
        to={"have_reaction": {"emoji": "\N{THUMBS UP SIGN}"}},
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
        to={"have_reaction": {"emoji": "\N{THUMBS UP SIGN}"}},
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
        to={"have_reaction": {"emoji": "\N{THUMBS UP SIGN}"}},
    )
    failure_messages += f

    await original_message.add_reaction("\N{HEAVY BLACK HEART}")
    _, f = await expect(
        bridged_message,
        to={"have_reaction": {"emoji": "\N{HEAVY BLACK HEART}"}},
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
        to={"have_reaction": {"emoji": "\N{THUMBS UP SIGN}"}},
    )
    failure_messages += f

    await original_message.add_reaction("\N{HEAVY BLACK HEART}")
    _, f = await expect(
        bridged_message,
        to={"have_reaction": {"emoji": "\N{HEAVY BLACK HEART}"}},
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
