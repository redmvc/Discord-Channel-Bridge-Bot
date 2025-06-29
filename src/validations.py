from __future__ import annotations

import inspect
import logging
from typing import Any, TypeVar

import discord

T = TypeVar("T", bound=Any)

# Objects to log events
logging.basicConfig(
    filename="logs.log", format="%(asctime)s %(levelname)s: %(message)s", filemode="w"
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ChannelTypeError(ValueError):
    pass


class WebhookChannelError(ValueError):
    pass


class HTTPResponseError(Exception):
    pass


class ArgumentError(ValueError):
    pass


def validate_channels(
    log_error: bool = True,
    **kwargs: Any,
) -> dict[str, discord.TextChannel | discord.Thread]:
    """Raise ChannelTypeError if any of the objects passed as arguments is not a `~discord.TextChannel` nor a `~discord.Thread` off one, and otherwise return a dictionary with the arguments cast to the right type.

    Parameters
    ----------
    log_error : bool, optional
        Whether to register this error in the logger. Defaults to True.
    **kwags
        The objects to validate.

    Returns
    -------
    dict[str, `~discord.TextChannel` | `~discord.Thread`]
    """
    cast_channels: dict[str, discord.TextChannel | discord.Thread] = {}
    for channel_name, channel in kwargs.items():
        if (
            not isinstance(channel, discord.Thread)
            or not isinstance(channel.parent, discord.TextChannel)
        ) and not isinstance(channel, discord.TextChannel):
            err = ChannelTypeError(
                f"Invalid channel '{channel_name}' passed to function {inspect.stack()[1][3]}(). It must be text channel or text channel thread, not {type(channel).__name__}."
            )
            if log_error:
                logger.error(err)
            raise err

        cast_channels[channel_name] = channel

    return cast_channels


def validate_webhook(
    webhook: discord.Webhook,
    target_channel: discord.TextChannel | discord.Thread,
):
    """Raise WebhookChannelError if the webhook is not attached to the target channel or, if it's a thread, its parent.

    Parameters
    ----------
    webhook : `~discord.Webhook`
        The webhook.
    target_channel : `~discord.TextChannel` | `~discord.Thread`
        The Discord text channel or thread to validate against.
    """
    if isinstance(target_channel, discord.TextChannel):
        target_channel_id = target_channel.id
    else:
        try:
            assert isinstance(target_channel.parent, discord.TextChannel)
        except AssertionError:
            err = ChannelTypeError(
                f"Error in function {inspect.stack()[1][3]}: webhook's target thread is not a thread off a text channel."
            )
            logger.error(err)
            raise err
        target_channel_id = target_channel.parent.id

    if target_channel_id != webhook.channel_id:
        err = WebhookChannelError(
            f"Error in function {inspect.stack()[1][3]}: webhook is not attached to Bridge's target channel."
        )
        logger.error(err)
        raise err
