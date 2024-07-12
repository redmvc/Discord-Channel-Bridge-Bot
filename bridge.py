import asyncio
from typing import Callable, Literal, cast, overload

import discord
import sqlalchemy
from sqlalchemy import Delete as SQLDelete
from sqlalchemy import and_ as sql_and
from sqlalchemy import or_ as sql_or
from sqlalchemy.exc import StatementError as SQLError
from sqlalchemy.orm import Session as SQLSession

import globals
from database import (
    DBBridge,
    DBMessageMap,
    DBWebhook,
    engine,
    sql_insert_ignore_duplicate,
    sql_retry,
    sql_upsert,
)
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
        target_channel = (
            await globals.get_channel_from_id(target)
            if isinstance(target, int)
            else target
        )
        validate_channels(
            source=(
                await globals.get_channel_from_id(source)
                if isinstance(source, int)
                else source
            ),
            target=target_channel,
        )

        self = Bridge()
        self._source_id = globals.get_id_from_channel(source)
        self._target_id = globals.get_id_from_channel(target)

        return self

    def __init__(self) -> None:
        """Construct a new empty Bridge. Should only be called from within class method Bridge.create()."""
        self._source_id: int | None = None
        self._target_id: int | None = None

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
        webhook = bridges.webhooks.get(self.target_id)
        assert webhook
        return webhook


class Bridges:
    """
    A list of all bridges created.

    Attributes
    ----------
    webhook : dict[int, discord.Webhook]
        The webhooks associated with each target channel.
    """

    def __init__(self) -> None:
        self._outbound_bridges: dict[int, dict[int, Bridge]] = {}
        self._inbound_bridges: dict[int, dict[int, Bridge]] = {}
        self.webhooks: dict[int, discord.Webhook] = {}

    async def create_bridge(
        self,
        *,
        source: discord.TextChannel | discord.Thread | int,
        target: discord.TextChannel | discord.Thread | int,
        webhook: discord.Webhook | None = None,
        update_db: bool = True,
        session: SQLSession | None = None,
    ) -> Bridge:
        """Create a new Bridge from source channel to target channel (and a new webhook if necessary).

        #### Args:
            - `source`: Source channel or ID of same.
            - `target`: Target channel or ID of same.
            - `webhook`: Optionally, an already-existing webhook connecting these channels. Defaults to None, in which case a new one will be created.
            - `update_db`: Whether to update the database when creating the Bridge. Defaults to True.
            - `session`: Optionally, a session with the connection to the database. Defaults to None, in which case creates and closes a new one locally. Only used if `update_db` is True.

        #### Raises:
            - `ChannelTypeError`: The source or target channels are not text channels nor threads off a text channel.
            - `WebhookChannelError`: `webhook` is not attached to Bridge's target channel.
            - `HTTPException`: Deleting an existing webhook or creating a new one failed.
            - `Forbidden`: You do not have permissions to create or delete webhooks.

        #### Returns:
            - `Bridge`: The created `Bridge`.
        """
        types_to_validate: dict[str, tuple] = {}
        if update_db and session:
            types_to_validate["session"] = (session, SQLSession)
        if webhook:
            types_to_validate["webhook"] = (webhook, discord.Webhook)
        if len(types_to_validate) > 0:
            validate_types(**types_to_validate)

        # First I create the Bridge in memory
        source_id = globals.get_id_from_channel(source)
        target_id = globals.get_id_from_channel(target)
        if self._outbound_bridges.get(source_id) and self._outbound_bridges[
            source_id
        ].get(target_id):
            # This bridge already exists, I won't create a new one
            bridge = self._outbound_bridges[source_id][target_id]

            if webhook:
                if not self.webhooks.get(target_id):
                    # I don't already have a webhook registered for the target channel, I'll add one now
                    self.webhooks[target_id] = webhook
                elif self.webhooks[target_id].id != webhook.id:
                    # I already have a webhook registered for the target channel and it is not this one, I'll delete this one
                    try:
                        await webhook.delete()
                    except Exception:
                        pass
        else:
            # Need to create a new bridge
            bridge = await Bridge.create(source_id, target_id)

            if not self._outbound_bridges.get(source_id):
                self._outbound_bridges[source_id] = {}
            self._outbound_bridges[source_id][target_id] = bridge

            if not self._inbound_bridges.get(target_id):
                self._inbound_bridges[target_id] = {}
            self._inbound_bridges[target_id][source_id] = bridge

            target_channel = await globals.get_channel_from_id(target)
            assert isinstance(target_channel, (discord.TextChannel, discord.Thread))

            if webhook:
                validate_webhook(webhook, target_channel)
                if (
                    self.webhooks.get(target_id)
                    and self.webhooks[target_id].id != webhook.id
                ):
                    # If I already have a webhook, I'll destroy the one being passed
                    try:
                        await webhook.delete()
                    except Exception:
                        pass
                else:
                    # Otherwise, I'll register the one being passed to my target channel
                    self.webhooks[target_id] = webhook
            elif not self.webhooks.get(target_id):
                # Target channel does not already have a webhook, create one
                if isinstance(target_channel, discord.Thread):
                    webhook_channel = target_channel.parent
                else:
                    webhook_channel = target_channel

                assert isinstance(webhook_channel, discord.TextChannel)
                self.webhooks[target_id] = await webhook_channel.create_webhook(
                    name=f":bridge: ({source_id} {target_id})"
                )

        # If I don't need to update the database I end here
        if not update_db:
            return bridge

        # Add this Bridge and webhook to the DB
        close_after = False
        try:
            if not session:
                close_after = True
                session = SQLSession(engine)

            target_id_str = str(target_id)
            insert_bridge_row = await sql_insert_ignore_duplicate(
                table=DBBridge,
                indices={"source", "target"},
                source=str(source_id),
                target=target_id_str,
            )
            insert_webhook_row = await sql_upsert(
                table=DBWebhook,
                indices={"channel"},
                channel=target_id_str,
                webhook=str(bridge.webhook.id),
            )

            def execute_query():
                session.execute(insert_bridge_row)
                session.execute(insert_webhook_row)

            await sql_retry(execute_query)
        except Exception as e:
            if close_after and session:
                session.rollback()
                session.close()

            if isinstance(e, SQLError):
                await bridges.demolish_bridges(
                    source, target, one_sided=True, update_db=False
                )

            raise e

        if close_after:
            session.commit()
            session.close()

        return bridge

    async def demolish_bridges(
        self,
        source: discord.TextChannel | discord.Thread | int,
        target: discord.TextChannel | discord.Thread | int,
        *,
        update_db: bool = True,
        session: SQLSession | None = None,
        one_sided: bool = False,
    ) -> None:
        """Destroy Bridges between source and target channel, deleting their associated webhooks.

        #### Args:
            - `source`: Source channel or ID of same.
            - `target`: Target channel or ID of same.
            - `update_db`: Whether to update the database when creating the Bridge. Defaults to True.
            - `session`: A connection to the database. Defaults to None, in which case a new one will be created to be used. Only used if `update_db` is True.
            - `one_sided`: Whether to demolish only the bridge going from `source` to `target`, rather than both. Defaults to False.

        #### Raises:
            - `HTTPException`: Deleting the webhook failed.
            - `Forbidden`: You do not have permissions to delete the webhook.
            - `ValueError`: The webhook does not have a token associated with it.
        """
        if update_db and session:
            validate_session = {"session": (session, SQLSession)}
        else:
            validate_session = {}
        validate_types(
            source=(source, (discord.TextChannel, discord.Thread, int)),
            target=(target, (discord.TextChannel, discord.Thread, int)),
            **validate_session,
        )
        one_sided = not not one_sided

        # If the command is called one-sidedly, there should be a bridge from source to target
        # otherwise, there should be at least one bridge between source and target
        source_id = globals.get_id_from_channel(source)
        target_id = globals.get_id_from_channel(target)
        if (
            not self._outbound_bridges.get(source_id)
            or not self._outbound_bridges[source_id].get(target_id)
        ) and (
            one_sided
            or (
                not self._outbound_bridges.get(target_id)
                or not self._outbound_bridges[target_id].get(source_id)
            )
        ):
            return

        # First we delete the Bridges from memory
        if self._outbound_bridges.get(source_id) and self._outbound_bridges[
            source_id
        ].get(target_id):
            # If there is a bridge from source to target, destroy it
            del self._outbound_bridges[source_id][target_id]
            if len(self._outbound_bridges[source_id]) == 0:
                del self._outbound_bridges[source_id]

            del self._inbound_bridges[target_id][source_id]
            if len(self._inbound_bridges[target_id]) == 0:
                del self._inbound_bridges[target_id]

        if (
            not one_sided
            and self._outbound_bridges.get(target_id)
            and self._outbound_bridges[target_id].get(source_id)
        ):
            # If the command was not called one-sidedly and there is a bridge from target to source, destroy it
            del self._inbound_bridges[source_id][target_id]
            if len(self._inbound_bridges[source_id]) == 0:
                del self._inbound_bridges[source_id]

            del self._outbound_bridges[target_id][source_id]
            if len(self._outbound_bridges[target_id]) == 0:
                del self._outbound_bridges[target_id]

        # If any channels no longer have bridges pointing to them, delete their webhooks
        async def delete_webhook(channel_id: int):
            webhook = self.webhooks[channel_id]
            try:
                await webhook.delete()
            except Exception:
                pass

            del self.webhooks[channel_id]

        async_delete_webhooks = []
        webhooks_deleted: set[str] = set()
        if not self._inbound_bridges.get(source_id) and self.webhooks.get(source_id):
            webhooks_deleted.add(str(self.webhooks[source_id].id))
            async_delete_webhooks.append(delete_webhook(source_id))

        if not self._inbound_bridges.get(target_id) and self.webhooks.get(target_id):
            webhooks_deleted.add(str(self.webhooks[target_id].id))
            async_delete_webhooks.append(delete_webhook(target_id))

        if len(async_delete_webhooks) > 0:
            await asyncio.gather(*async_delete_webhooks)

        # Return if we're not meant to update the DB
        if not update_db:
            return

        # Update the DB
        close_after = False
        try:
            if not session:
                session = SQLSession(engine)
                close_after = True

            source_id_str = str(source_id)
            target_id_str = str(target_id)
            delete_demolished_bridges = SQLDelete(DBBridge).where(
                sql_or(
                    sql_and(
                        DBBridge.source == source_id_str,
                        DBBridge.target == target_id_str,
                    ),
                    sql_and(
                        sqlalchemy.sql.expression.literal(not one_sided),
                        DBBridge.source == target_id_str,
                        DBBridge.target == source_id_str,
                    ),
                )
            )
            delete_demolished_messages = SQLDelete(DBMessageMap).where(
                sql_or(
                    sql_and(
                        DBMessageMap.source_channel == source_id_str,
                        DBMessageMap.target_channel == target_id_str,
                    ),
                    sql_and(
                        sqlalchemy.sql.expression.literal(not one_sided),
                        DBMessageMap.source_channel == target_id_str,
                        DBMessageMap.target_channel == source_id_str,
                    ),
                )
            )

            if len(webhooks_deleted) > 0:
                delete_invalid_webhooks = SQLDelete(DBWebhook).where(
                    DBWebhook.webhook.in_(webhooks_deleted)
                )
            else:
                delete_invalid_webhooks = None

            def execute_queries():
                session.execute(delete_demolished_bridges)
                session.execute(delete_demolished_messages)
                if delete_invalid_webhooks is not None:
                    session.execute(delete_invalid_webhooks)

            await sql_retry(execute_queries)
        except Exception as e:
            if close_after and session:
                session.rollback()
                session.close()

            raise e

        if close_after:
            session.commit()
            session.close()

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
