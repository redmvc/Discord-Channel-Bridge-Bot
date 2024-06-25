from typing import cast

import discord

import globals
from validations import validate_channels, validate_types, validate_webhook


class Bridge:
    """
    A bridge from a source channel to a target channel.

    Attributes
    ----------
    source_id : int
        The ID of the source channel of the bridge.

    target_id : int
        The ID of the target channel of the bridge.

    webhook : discord.Webhook
        A webhook connecting those channels
    """

    @classmethod
    async def create(
        cls,
        source: discord.TextChannel | discord.Thread | int,
        target: discord.TextChannel | discord.Thread | int,
        webhook: discord.Webhook | None = None,
    ):
        """Create an outbound Bridge from source channel to target channel.

        #### Args:
            - `source`: Source channel or ID of same.
            - `target`: Target channel or ID of same.
            - `webhook`: Optionally, an existing webhook. Defaults to None, in which case a new one will be created.

        #### Raises:
            - `ChannelTypeError`: The source or target channels are not text channels nor threads off a text channel.
            - `WebhookChannelError`: `webhook` is not attached to target channel.
            - `HTTPException`: Creating the webhook failed.
            - `Forbidden`: You do not have permissions to create a webhook.
            - `ValueError`: Existing webhook does not have a token associated with it.

        #### Returns:
            - `Bridge`: The created Bridge.
        """

        self = Bridge()
        await self._add_source_and_target(source, target)
        await self._add_webhook(webhook)
        return self

    def __init__(self) -> None:
        """Construct a new empty Bridge. Should only be called from within class method Bridge.create()."""
        self._source_id: int | None = None
        self._target_id: int | None = None
        self._webhook: discord.Webhook | None = None

    async def _add_source_and_target(
        self,
        source: discord.TextChannel | discord.Thread | int,
        target: discord.TextChannel | discord.Thread | int,
    ) -> None:
        """Add a source and target to an empty Bridge. Should only be called from within class method Bridge.create().

        #### Args:
            - `source`: Source channel or ID of same.
            - `target`: Target channel or ID of same.

        #### Raises:
            - `AttributeError`: The Bridge isn't empty (i.e. it already has a source and a target).
            - `ChannelTypeError`: The source or target channels are not text channels nor threads off a text channel.
        """
        if self._source_id and self._target_id:
            raise AttributeError("Bridge is not empty.")

        validate_types(
            {
                "source": (source, (discord.TextChannel, discord.Thread, int)),
                "target": (target, (discord.TextChannel, discord.Thread, int)),
            }
        )
        validate_channels(
            {
                "source": await globals.get_channel_from_id(source),
                "target": await globals.get_channel_from_id(target),
            }
        )

        self._source_id = globals.get_id_from_channel(source)
        self._target_id = globals.get_id_from_channel(target)

    async def _add_webhook(self, webhook: discord.Webhook | None = None) -> None:
        """Add an existing webhook to this Bridge or create a new one for it.

        #### Args:
            - `webhook`: The webhook to add. Defaults to None, in which case a new one will be created.

        #### Raises:
            - `WebhookChannelError`: `webhook` is not attached to Bridge's target channel.
            - `HTTPException`: Deleting the existing webhook or creating a new webhook failed.
            - `Forbidden`: You do not have permissions to delete the existing webhook or create a new one.
            - `ValueError`: Existing webhook does not have a token associated with it.
        """
        target_channel = await self.target_channel

        if webhook:
            validate_types({"webhook": (webhook, discord.Webhook)})
            validate_webhook(webhook, target_channel)

        await self._destroy_webhook("Recycling webhook.")

        if webhook:
            self._webhook = webhook
        else:
            if isinstance(target_channel, discord.Thread):
                webhook_channel = target_channel.parent
            else:
                webhook_channel = target_channel

            assert isinstance(webhook_channel, discord.TextChannel)
            self._webhook = await webhook_channel.create_webhook(
                name=f":bridge: ({self._source_id} {self._target_id})"
            )

    async def update_webhook(self, webhook: discord.Webhook | None = None) -> None:
        """Replace this Bridge's webhook, destroying the existing one if possible.

        #### Args:
            - `webhook`: The webhook to replace this Bridge's webhook with. Defaults to None, in which case this function does nothing.

        #### Raises:
            - `WebhookChannelError`: `webhook` is not attached to Bridge's target channel.
            - `HTTPException`: Deleting the existing webhook failed.
            - `Forbidden`: You do not have permissions to delete the existing webhook.
            - `ValueError`: Existing webhook does not have a token associated with it.
        """
        if not webhook:
            return

        await self._add_webhook(webhook)

    async def _destroy_webhook(self, reason: str = "User request."):
        """Destroys the Bridge's webhook if it exists.

        #### Args:
            - `reason`: The reason to be stored in the Discord logs. Defaults to "User request.".

        #### Raises:
            - `HTTPException`: Deleting the webhook failed.
            - `Forbidden`: You do not have permissions to delete this webhook.
            - `ValueError`: This webhook does not have a token associated with it.
        """
        validate_types({"reason": (reason, str)})

        if self._webhook:
            try:
                await self._webhook.delete(reason=reason)
            except discord.NotFound:
                pass
            self._webhook = None

    @property
    def source_id(self) -> int:
        assert self._source_id
        return self._source_id

    @property
    async def source_channel(self) -> discord.TextChannel | discord.Thread:
        return cast(
            discord.TextChannel | discord.Thread,
            await globals.get_channel_from_id(self.source_id),
        )

    @property
    def target_id(self) -> int:
        assert self._target_id
        return self._target_id

    @property
    async def target_channel(self) -> discord.TextChannel | discord.Thread:
        return cast(
            discord.TextChannel | discord.Thread,
            await globals.get_channel_from_id(self.target_id),
        )

    @property
    def webhook(self) -> discord.Webhook:
        assert self._webhook
        return self._webhook


class Bridges:
    """
    A list of all bridges created.
    """

    def __init__(self) -> None:
        self._outbound_bridges: dict[int, dict[int, Bridge]] = {}
        self._inbound_bridges: dict[int, dict[int, Bridge]] = {}

    async def create_bridge(
        self,
        source: discord.TextChannel | discord.Thread | int,
        target: discord.TextChannel | discord.Thread | int,
        webhook: discord.Webhook | None = None,
    ) -> Bridge:
        """Create a new Bridge from source channel to target channel (and a new webhook if necessary) or update an existing Bridge with the webhook.

        #### Args:
            - `source`: Source channel or ID of same.
            - `target`: Target channel or ID of same.

        #### Raises:
            - `ChannelTypeError`: The source or target channels are not text channels nor threads off a text channel.
            - `WebhookChannelError`: `webhook` is not attached to Bridge's target channel.
            - `HTTPException`: Deleting an existing webhook or creating a new one failed.
            - `Forbidden`: You do not have permissions to create or delete webhooks.

        #### Returns:
            - `Bridge`: The created `Bridge`.
        """
        source_id = globals.get_id_from_channel(source)
        target_id = globals.get_id_from_channel(target)
        if self._outbound_bridges.get(source_id) and self._outbound_bridges[
            source_id
        ].get(target_id):
            bridge = self._outbound_bridges[source_id][target_id]
            await bridge.update_webhook(webhook)
        else:
            bridge = await Bridge.create(source_id, target_id, webhook)

            if not self._outbound_bridges.get(source_id):
                self._outbound_bridges[source_id] = {}
            self._outbound_bridges[source_id][target_id] = bridge

            if not self._inbound_bridges.get(target_id):
                self._inbound_bridges[target_id] = {}
            self._inbound_bridges[target_id][source_id] = bridge

        return bridge

    async def demolish_bridge(
        self,
        source: discord.TextChannel | discord.Thread | int,
        target: discord.TextChannel | discord.Thread | int,
    ) -> None:
        """Destroy the Bridge from source channel to target channel, deleting its associated webhook.

        #### Args:
            - `source`: Source channel or ID of same.
            - `target`: Target channel or ID of same.

        #### Raises:
            - `HTTPException`: Deleting the webhook failed.
            - `Forbidden`: You do not have permissions to delete the webhook.
            - `ValueError`: The webhook does not have a token associated with it.
        """
        validate_types(
            {
                "source": (source, (discord.TextChannel, discord.Thread, int)),
                "target": (target, (discord.TextChannel, discord.Thread, int)),
            }
        )

        source_id = globals.get_id_from_channel(source)
        target_id = globals.get_id_from_channel(target)

        if not self._outbound_bridges.get(source_id) or not self._outbound_bridges[
            source_id
        ].get(target_id):
            return

        await self._outbound_bridges[source_id][target_id]._destroy_webhook()

        del self._outbound_bridges[source_id][target_id]
        if len(self._outbound_bridges[source_id]) == 0:
            del self._outbound_bridges[source_id]

        del self._inbound_bridges[target_id][source_id]
        if len(self._inbound_bridges[target_id]) == 0:
            del self._inbound_bridges[target_id]

    def get_one_way_bridge(
        self,
        source: discord.TextChannel | discord.Thread | int,
        target: discord.TextChannel | discord.Thread | int,
    ) -> Bridge | None:
        """Return the Bridge from source channel to target channel.

        #### Args:
            - `source`: Source channel or ID of same.
            - `target`: Target channel or ID of same.
        """
        validate_types(
            {
                "source": (source, (discord.TextChannel, discord.Thread, int)),
                "target": (target, (discord.TextChannel, discord.Thread, int)),
            }
        )

        source_id = globals.get_id_from_channel(source)
        target_id = globals.get_id_from_channel(target)

        if not self._outbound_bridges.get(source_id) or not self._outbound_bridges[
            source_id
        ].get(target_id):
            return None

        return self._outbound_bridges[source_id][target_id]

    def get_two_way_bridge(
        self,
        source: discord.TextChannel | discord.Thread | int,
        target: discord.TextChannel | discord.Thread | int,
    ) -> tuple[Bridge | None, Bridge | None]:
        """Return a tuple of Bridges, the first element of which is the Bridge from source to target and the second of which is the Bridge from target to source.

        #### Args:
            - `source`: Source channel or ID of same.
            - `target`: Target channel or ID of same.
        """
        validate_types(
            {
                "source": (source, (discord.TextChannel, discord.Thread, int)),
                "target": (target, (discord.TextChannel, discord.Thread, int)),
            }
        )

        return (
            self.get_one_way_bridge(source, target),
            self.get_one_way_bridge(target, source),
        )

    def get_outbound_bridges(
        self, source: discord.TextChannel | discord.Thread | int
    ) -> dict[int, Bridge] | None:
        """Return a dict with all Bridges from source channel, identified by the target channel id.

        #### Args:
            - `source`: Source channel or ID of same.
        """
        validate_types({"source": (source, (discord.TextChannel, discord.Thread, int))})

        return self._outbound_bridges.get(globals.get_id_from_channel(source))

    def get_inbound_bridges(
        self, target: discord.TextChannel | discord.Thread | int
    ) -> dict[int, Bridge] | None:
        """Return a dict with all Bridges to target channel, identified by the source channel id.

        #### Args:
            - `target`: Target channel or ID of same.
        """
        validate_types({"target": (target, (discord.TextChannel, discord.Thread, int))})

        return self._inbound_bridges.get(globals.get_id_from_channel(target))


bridges = Bridges()
