from __future__ import annotations

from typing import Any, Sequence, Type

import discord


class ChannelTypeError(Exception):
    pass


def natural_language_concat(items: Sequence[str]) -> str:
    if len(items) == 2:
        return items[0] + " or " + items[1]
    else:
        return ", ".join(items[:-1]) + " or " + items[-1]


def validate_types(
    arguments: dict[
        str, tuple[Any, type | Type[int | str] | tuple[type | Type[int | str], ...]]
    ]
):
    """Raise `TypeError` if the arguments passed are not the right type.

    #### Args:
        - `arguments`: A dictionary whose keys are argument names and whose values are tuple with the argument value and its intended type.
    """
    for arg_name, (arg_value, valid_type) in arguments.items():
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
            isinstance(channel, discord.Thread)
            and not isinstance(channel.parent, discord.TextChannel)
        ) or not isinstance(channel, discord.TextChannel):
            raise ChannelTypeError(
                f"{channel_name} channel must be text channel or text channel thread, not "
                + type(channel).__name__
            )
