from typing import TYPE_CHECKING, Any, Coroutine, overload

from beartype import beartype

import events
import globals
from validations import logger

if TYPE_CHECKING:
    from typing import Literal


@overload
def start_client() -> None:
    """Start the client and connect to Discord. This function is blocking."""
    ...


@overload
def start_client(blocking: "Literal[True]") -> None:
    """Start the client and connect to Discord. This function is blocking."""
    ...


@overload
def start_client(blocking: "Literal[False]") -> Coroutine[Any, Any, None]:
    """Return a Coroutine that can be awaited or passed to an asyncio event loop which starts the bot client and connects to Discord without blocking execution.

    Returns
    -------
    Coroutine[Any, Any, None]
    """
    ...


@beartype
def start_client(blocking: bool = True) -> Coroutine[Any, Any, None] | None:
    """Start the client and connect to Discord. If `blocking` is set to False, this function will instead return a Coroutine that can be awaited or passed to an asyncio event loop with a non-blocking connection.

    Parameters
    ----------
    blocking : bool, optional
        Whether to run the blocking version of the connection. If set to False, this function will return a non-blocking Coroutine to connect to the servers. Defaults to True.

    Returns
    -------
    Coroutine[Any, Any, None] | None
    """
    events.register_events()
    app_token = globals.settings.get("app_token")
    if blocking:
        logger.info("Connecting client...")
        globals.client.run(app_token, reconnect=True)
    else:
        return globals.client.start(app_token, reconnect=True)


if __name__ == "__main__":
    start_client()
