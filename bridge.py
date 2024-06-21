import discord

import globals


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
        self = cls(source, target)
        await self.add_webhook(webhook)
        return self

    def __init__(
        self,
        source: discord.TextChannel | discord.Thread | int,
        target: discord.TextChannel | discord.Thread | int,
    ) -> None:
        self._source_id = globals.get_id_from_channel(source)
        self._target_id = globals.get_id_from_channel(target)
        self._webhook: discord.Webhook | None = None

    async def add_webhook(
        self,
        webhook: discord.Webhook | None = None,
    ) -> None:
        if self._webhook:
            await self._webhook.delete(reason="Recycling webhook.")
            self._webhook = None

        if webhook:
            self._webhook = webhook
        else:
            target_channel = globals.get_channel_from_id(self._target_id)
            assert isinstance(target_channel, discord.TextChannel | discord.Thread)
            if isinstance(target_channel, discord.Thread):
                webhook_channel = target_channel.parent
            else:
                webhook_channel = target_channel

            assert isinstance(webhook_channel, discord.TextChannel)
            self._webhook = await webhook_channel.create_webhook(
                name=f":bridge: ({self._source_id} {self._target_id})"
            )

    async def update_webhook(
        self,
        webhook: discord.Webhook | None = None,
    ) -> None:
        if not webhook:
            return

        await self.add_webhook(webhook)

    async def __del__(self):
        if self._webhook:
            await self._webhook.delete(reason="Recycling webhook.")

    @property
    def source_id(self) -> int:
        return self._source_id

    @property
    def target_id(self) -> int:
        return self._target_id

    @property
    def webhook(self) -> discord.Webhook | None:
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
        """

        source_id = globals.get_id_from_channel(source)
        target_id = globals.get_id_from_channel(target)

        if not self._outbound_bridges.get(source_id) or not self._outbound_bridges[
            source_id
        ].get(target_id):
            return

        del self._outbound_bridges[source_id][target_id]
        del self._inbound_bridges[target_id][source_id]

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

        return (
            self.get_one_way_bridge(source, target),
            self.get_one_way_bridge(target, source),
        )

    def get_outbound_bridges(
        self,
        source: discord.TextChannel | discord.Thread | int,
    ) -> dict[int, Bridge] | None:
        """Return a dict with all Bridges from source channel, identified by the target channel id.

        #### Args:
            - `source`: Source channel or ID of same.
        """

        return self._outbound_bridges.get(globals.get_id_from_channel(source))

    def get_inbound_bridges(
        self,
        target: discord.TextChannel | discord.Thread | int,
    ) -> dict[int, Bridge] | None:
        """Return a dict with all Bridges to target channel, identified by the source channel id.

        #### Args:
            - `target`: Target channel or ID of same.
        """

        return self._inbound_bridges.get(globals.get_id_from_channel(target))
