from __future__ import annotations

from typing import Any, Sequence, Type

import discord


class ChannelTypeError(Exception):
    pass


class WebhookChannelError(Exception):
    pass


class HTTPResponseError(Exception):
    pass


class ArgumentError(ValueError):
    pass


def natural_language_concat(items: Sequence[str]) -> str:
    if len(items) == 2:
        return items[0] + " or " + items[1]
    else:
        return ", ".join(items[:-1]) + " or " + items[-1]


def validate_types(
    **kwargs: tuple[Any, type | Type[int | str] | tuple[type | Type[int | str], ...]]
):
    """Raise `TypeError` if the arguments passed are not the right type.

    #### Args:
        - `kwargs`: The arguments to validate and tuples with their values and types.
    """
    for arg_name, (arg_value, valid_type) in kwargs.items():
        if not isinstance(arg_value, valid_type):
            if isinstance(valid_type, type):
                raise TypeError(
                    f"{arg_name} must be {valid_type.__name__}, not "
                    + type(arg_value).__name__
                )
            else:
                raise TypeError(
                    f"{arg_name} must be {[t.__name__ for t in valid_type]}, not "
                    + type(arg_value).__name__
                )


def validate_channels(
    channels: dict[
        str,
        discord.guild.GuildChannel | discord.Thread | discord.abc.PrivateChannel | None,
    ]
):
    """Raise `ChannelTypeError` if the channels passed as arguments are not the right channel types.

    #### Args:
        - `channels`: A dictionary whose keys are channel names and whose values are the channels.
    """
    for channel_name, channel in channels.items():
        if (
            not isinstance(channel, discord.Thread)
            or not isinstance(channel.parent, discord.TextChannel)
        ) and not isinstance(channel, discord.TextChannel):
            raise ChannelTypeError(
                f"{channel_name} channel must be text channel or text channel thread, not "
                + type(channel).__name__
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
