from typing import Callable, Literal, cast, overload

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

    source_channel : TextChannel | Thread
        The source channel of the bridge.

    target_id : int
        The ID of the target channel of the bridge.

    target_channel : TextChannel | Thread
        The target channel of the bridge.

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
        validate_channels(
            source=(
                await globals.get_channel_from_id(source)
                if isinstance(source, int)
                else source
            ),
            target=(
                await globals.get_channel_from_id(target)
                if isinstance(target, int)
                else target
            ),
        )

        self = Bridge()
        self._source_id = globals.get_id_from_channel(source)
        self._target_id = globals.get_id_from_channel(target)
        await self._add_webhook(webhook)

        return self

    def __init__(self) -> None:
        """Construct a new empty Bridge. Should only be called from within class method Bridge.create()."""
        self._source_id: int | None = None
        self._target_id: int | None = None
        self._webhook: discord.Webhook | None = None

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
            validate_types(webhook=(webhook, discord.Webhook))
            validate_webhook(webhook, target_channel)

        # If I already have a webhook, I'll destroy it and replace it with a new one
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
        validate_types(reason=(reason, str))

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
            source=(source, (discord.TextChannel, discord.Thread, int)),
            target=(target, (discord.TextChannel, discord.Thread, int)),
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
            source=(source, (discord.TextChannel, discord.Thread, int)),
            target=(target, (discord.TextChannel, discord.Thread, int)),
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
            source=(source, (discord.TextChannel, discord.Thread, int)),
            target=(target, (discord.TextChannel, discord.Thread, int)),
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
        validate_types(source=(source, (discord.TextChannel, discord.Thread, int)))

        return self._outbound_bridges.get(globals.get_id_from_channel(source))

    def get_inbound_bridges(
        self, target: discord.TextChannel | discord.Thread | int
    ) -> dict[int, Bridge] | None:
        """Return a dict with all Bridges to target channel, identified by the source channel id.

        #### Args:
            - `target`: Target channel or ID of same.
        """
        validate_types(target=(target, (discord.TextChannel, discord.Thread, int)))

        return self._inbound_bridges.get(globals.get_id_from_channel(target))

    @overload
    def get_reachable_channels(
        self,
        starting_channel: discord.TextChannel | discord.Thread | int,
        direction: Literal["outbound", "inbound"],
        *,
        include_webhooks: Literal[True],
        include_starting: bool = False,
    ) -> dict[int, discord.Webhook]:
        ...

    @overload
    def get_reachable_channels(
        self,
        starting_channel: discord.TextChannel | discord.Thread | int,
        direction: Literal["outbound", "inbound"],
        *,
        include_webhooks: Literal[False] | None = None,
        include_starting: bool = False,
    ) -> set[int]:
        ...

    def get_reachable_channels(
        self,
        starting_channel: discord.TextChannel | discord.Thread | int,
        direction: Literal["outbound", "inbound"],
        *,
        include_webhooks: bool | None = False,
        include_starting: bool = False,
    ) -> set[int] | dict[int, discord.Webhook]:
        """If `include_webhooks` is `False` (default), return a set with all channel IDs reachable from a given source channel down an unbroken series of outbound or inbound bridges; if it's `True`, return a dictionary with those channels as keys and one webhook attached to each of those channels as values.

        #### Args:
            - `starting_channel`: The channel other channels must be reachable from.
            - `direction`: Whether to go down outbound or inbound bridges.
            - `include_webhooks`: Whether to include a list of webhooks attached to the reachable channels in the output. Will only include one webhook per channel. Defaults to False.
            - `include_starting`: Whether to include the starting channel ID in the list. Defaults to False.

        #### Raises:
            - `ValueError`: The `direction` variable has a value other than `"outbound"` and `"inbound"`.
        """
        if include_webhooks is not None:
            validate_include_webhooks = {"include_webhooks": (include_webhooks, bool)}
        else:
            validate_include_webhooks = {}
        validate_types(
            starting_channel=(
                starting_channel,
                (discord.TextChannel, discord.Thread, int),
            ),
            direction=(direction, str),
            include_starting=(include_starting, bool),
            **validate_include_webhooks,
        )

        if direction not in {"outbound", "inbound"}:
            raise ValueError(
                'direction argument to get_reachable_channels() must be either "outbound" or "inbound".'
            )

        get_bridges: Callable[
            [discord.TextChannel | discord.Thread | int], dict[int, Bridge] | None
        ]
        if direction == "outbound":
            get_bridges = self.get_outbound_bridges
        else:
            get_bridges = self.get_inbound_bridges

        starting_channel_id = globals.get_id_from_channel(starting_channel)
        channel_ids_to_check: set[int] = {starting_channel_id}
        channel_ids_checked: set[int] = set()

        reachable_channel_ids: set[int] | dict[int, discord.Webhook]
        if include_webhooks:
            reachable_channel_ids = {}
        else:
            reachable_channel_ids = set()

        while len(channel_ids_to_check) > 0:
            channel_id_to_check = channel_ids_to_check.pop()
            if channel_id_to_check in channel_ids_checked:
                continue

            channel_ids_checked.add(channel_id_to_check)
            bridges_to_check = get_bridges(channel_id_to_check)
            if not bridges_to_check:
                continue

            newly_reachable_ids = set(bridges_to_check.keys())
            if isinstance(reachable_channel_ids, dict):
                reachable_channel_ids = {
                    channel_id: bridge.webhook
                    for channel_id, bridge in bridges_to_check.items()
                } | reachable_channel_ids
            else:
                reachable_channel_ids = reachable_channel_ids.union(newly_reachable_ids)
            channel_ids_to_check = (
                channel_ids_to_check.union(newly_reachable_ids) - channel_ids_checked
            )

        if not include_starting:
            if isinstance(reachable_channel_ids, dict):
                reachable_channel_ids.pop(starting_channel_id, None)
            else:
                reachable_channel_ids.discard(starting_channel_id)

        return reachable_channel_ids


bridges = Bridges()
