from __future__ import annotations

import logging

import discord

# Objects to log events
logging.basicConfig(
    filename="logs.log", format="%(asctime)s %(levelname)s: %(message)s", filemode="w"
)
logger = logging.getLogger()
logger.setLevel(logging.WARNING)


class ChannelTypeError(ValueError):
    pass


class WebhookChannelError(ValueError):
    pass


class HTTPResponseError(Exception):
    pass


class ArgumentError(ValueError):
    pass


def validate_channels(
    **kwargs: (
        discord.guild.GuildChannel | discord.Thread | discord.abc.PrivateChannel | None
    ),
):
    """Raise `ChannelTypeError` if the channels passed as arguments are not the right channel types.

    #### Args:
        - `kwargs`: The channels to validate.
    """
    for channel_name, channel in kwargs.items():
        if (
            not isinstance(channel, discord.Thread)
            or not isinstance(channel.parent, discord.TextChannel)
        ) and not isinstance(channel, discord.TextChannel):
            raise ChannelTypeError(
                f"{channel_name} channel must be text channel or text channel thread, not {type(channel).__name__}."
            )


def validate_webhook(
    webhook: discord.Webhook, target_channel: discord.TextChannel | discord.Thread
):
    """Raise `WebhookChannelError` if the webhook is not attached to the target channel or, if it's a thread, its parent.

    #### Args:
        - `webhook`: A Discord webhook.
        - `target_channel`: The Discord text channel or thread to validate against.

    #### Raises:
        - `ChannelTypeError`: `target_channel` is a thread but not one that is off a text channel.
    """
    if isinstance(target_channel, discord.TextChannel):
        target_channel_id = target_channel.id
    else:
        try:
            assert isinstance(target_channel.parent, discord.TextChannel)
        except AssertionError:
            raise ChannelTypeError("Target thread is not a thread off a text channel.")
        target_channel_id = target_channel.parent.id

    if target_channel_id != webhook.channel_id:
        raise WebhookChannelError("webhook is not attached to Bridge's target channel.")
