from __future__ import annotations

from typing import Any, Sequence, Type


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
