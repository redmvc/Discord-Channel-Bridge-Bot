from unittest.mock import patch

import discord
import test_runner
from test_runner import (
    create_bridge,
    demolish_bridges,
    expect,
    give_manage_webhook_perms,
)

import common

FAKE_GUILD_ID = 99999


class ReplacingLinks(test_runner.TestCase):
    order = 45
    dependencies = ["CreatingBridges", "DemolishingBridges", "BridgingMessages"]

    def __init__(self):
        super().__init__(test_runner.test_runner)


replacing_links_tests = ReplacingLinks()


def _mock_guild_id_factory(channel_to_fake: discord.TextChannel):
    """Return a mock for common.get_channel_guild_id that makes one channel appear cross-server."""
    original = common.get_channel_guild_id

    async def mock_get_channel_guild_id(channel_or_id, **kwargs):
        if common.get_id_from_channel(channel_or_id) == channel_to_fake.id:
            return FAKE_GUILD_ID
        return await original(channel_or_id, **kwargs)

    return mock_get_channel_guild_id


@replacing_links_tests.test
async def replaces_message_link_to_bridged_message(
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
    await create_bridge(channel_1, channel_2.id)

    guild_id = testing_server.id

    # Send a seed message and capture its bridged copy
    msg_a = await channel_1.send("seed message")
    bridged_a, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"equal": "seed message", "be_from": bridge_bot},
    )
    if not bridged_a:
        return failure_messages

    # Send a message containing a link to msg_a
    link = f"https://discord.com/channels/{guild_id}/{channel_1.id}/{msg_a.id}"
    await channel_1.send(f"check this out: {link}")

    # The bridged copy should have the link rewritten to point to bridged_a in channel_2
    expected_fragment = f"{guild_id}/{channel_2.id}/{bridged_a.id}"
    _, f = await expect(
        "next_message",
        in_channel=channel_2,
        to={"contain": expected_fragment, "be_from": bridge_bot},
    )
    failure_messages += f

    return failure_messages


@replacing_links_tests.test
async def does_not_replace_unbridged_message_link(
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

    guild_id = testing_server.id

    # Send a message in unbridged channel_3
    msg_b = await channel_3.send("unbridged message")

    # Send a message in channel_1 containing a link to msg_b
    link = f"https://discord.com/channels/{guild_id}/{channel_3.id}/{msg_b.id}"
    await channel_1.send(f"see: {link}")

    # The link should be unchanged since msg_b has no bridge
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"contain": f"{channel_3.id}/{msg_b.id}", "be_from": bridge_bot},
    )

    return failure_messages


@replacing_links_tests.test
async def does_not_replace_link_already_in_target(
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
    await create_bridge(channel_1, channel_2.id)

    guild_id = testing_server.id

    # Send a message in channel_2 (will be bridged to channel_1)
    msg_in_ch2 = await channel_2.send("message in target")
    # Consume the bridged copy that arrives in channel_1
    await expect(
        "next_message",
        in_channel=channel_1,
        to={"equal": "message in target", "be_from": bridge_bot},
    )

    # Send a message in channel_1 containing a link to the ch2 message
    link = f"https://discord.com/channels/{guild_id}/{channel_2.id}/{msg_in_ch2.id}"
    await channel_1.send(f"see: {link}")

    # The link should be unchanged since it already points to the target channel
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"contain": f"{channel_2.id}/{msg_in_ch2.id}", "be_from": bridge_bot},
    )

    return failure_messages


@replacing_links_tests.test
async def replaces_cross_server_channel_mention(
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
    await demolish_bridges(channel_1, channel_and_threads=True)
    await demolish_bridges(channel_3, channel_and_threads=True)
    await create_bridge(channel_3, channel_4.id)
    await create_bridge(channel_1, channel_2.id)

    # Mock channel_3 as cross-server, then send a message mentioning it
    mock_fn = _mock_guild_id_factory(channel_3)
    with patch.object(common, "get_channel_guild_id", side_effect=mock_fn):
        await channel_1.send(f"go to <#{channel_3.id}>")

        # The mention should be replaced with channel_4 (bridged to ch3, in target guild)
        _, failure_messages = await expect(
            "next_message",
            in_channel=channel_2,
            to={
                "contain": f"<#{channel_4.id}>",
                "not_contain": f"<#{channel_3.id}>",
                "be_from": bridge_bot,
            },
        )

    return failure_messages


@replacing_links_tests.test
async def does_not_replace_same_server_channel_mention(
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
    await create_bridge(channel_1, channel_2.id)

    # No mock — channel_3 is genuinely in the same server
    await channel_1.send(f"check <#{channel_3.id}>")

    # The mention should be unchanged
    _, failure_messages = await expect(
        "next_message",
        in_channel=channel_2,
        to={"contain": f"<#{channel_3.id}>", "be_from": bridge_bot},
    )

    return failure_messages


@replacing_links_tests.test
async def replaces_cross_server_message_link(
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
    await demolish_bridges(channel_1, channel_and_threads=True)
    await demolish_bridges(channel_3, channel_and_threads=True)
    await create_bridge(channel_3, channel_4.id)
    await create_bridge(channel_1, channel_2.id)

    guild_id = testing_server.id
    mock_fn = _mock_guild_id_factory(channel_3)

    with patch.object(common, "get_channel_guild_id", side_effect=mock_fn):
        # Send a seed message in channel_3 and capture its bridged copy in channel_4
        msg_c = await channel_3.send("cross server seed")
        bridged_c, failure_messages = await expect(
            "next_message",
            in_channel=channel_4,
            to={"equal": "cross server seed", "be_from": bridge_bot},
        )
        if not bridged_c:
            return failure_messages

        # Send a message in channel_1 with a link to msg_c
        link = f"https://discord.com/channels/{guild_id}/{channel_3.id}/{msg_c.id}"
        await channel_1.send(f"see: {link}")

        # The link should be rewritten to point to bridged_c in channel_4
        expected_fragment = f"{guild_id}/{channel_4.id}/{bridged_c.id}"
        _, f = await expect(
            "next_message",
            in_channel=channel_2,
            to={
                "contain": expected_fragment,
                "not_contain": f"{channel_3.id}/{msg_c.id}",
                "be_from": bridge_bot,
            },
        )
        failure_messages += f

    return failure_messages
