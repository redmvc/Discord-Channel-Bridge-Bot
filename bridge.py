import asyncio
import inspect
from copy import deepcopy
from typing import Any, Callable, Coroutine, Literal, cast, overload

import discord
from beartype import beartype
from sqlalchemy import Delete as SQLDelete
from sqlalchemy import ScalarResult
from sqlalchemy import Select as SQLSelect
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
from validations import ArgumentError, logger, validate_channels, validate_webhook


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

    @beartype
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
        logger.debug("Creating bridge from %s to %s.", source, target)

        if isinstance(source, int):
            validate_channels(source=await globals.get_channel_from_id(source))
        if isinstance(target, int):
            validate_channels(target=await globals.get_channel_from_id(target))

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
    async def webhook(self) -> discord.Webhook:
        webhook = await bridges.webhooks.get_webhook(self.target_id)
        assert webhook
        return webhook


class Bridges:
    """
    A list of all bridges created.

    Attributes
    ----------
    webhooks : Webhooks
        The webhooks associated with each target channel.
    """

    def __init__(self) -> None:
        self._outbound_bridges: dict[int, dict[int, Bridge]] = {}
        self._inbound_bridges: dict[int, dict[int, Bridge]] = {}
        self.webhooks = Webhooks()

    @beartype
    async def load_from_database(self, session: SQLSession | None = None):
        logger.info("Loading bridges from database...")

        if not session:
            session = SQLSession(engine)
            close_after = True
        else:
            close_after = False

        self._outbound_bridges: dict[int, dict[int, Bridge]] = {}
        self._inbound_bridges: dict[int, dict[int, Bridge]] = {}
        self.webhooks = Webhooks()

        # I am going to try to identify all existing bridges and webhooks and add them to my tracking
        # and also delete the ones that aren't valid or accessible
        invalid_channel_ids: set[str] = set()
        invalid_webhook_ids: set[str] = set()

        select_all_webhooks: SQLSelect[tuple[DBWebhook]] = SQLSelect(DBWebhook)
        webhook_query_result: ScalarResult[DBWebhook] = session.scalars(
            select_all_webhooks
        )
        add_webhook_async: list[Coroutine[Any, Any, discord.Webhook]] = []
        for channel_webhook in webhook_query_result:
            channel_id = int(channel_webhook.channel)
            webhook_id = int(channel_webhook.webhook)

            channel = await globals.get_channel_from_id(channel_id)
            if not channel or not isinstance(
                channel, (discord.TextChannel, discord.Thread)
            ):
                # If I don't have access to the channel, delete bridges from and to it
                logger.debug(
                    "Couldn't find channel with ID %s when loading webhooks from database.",
                    channel_id,
                )
                invalid_channel_ids.add(channel_webhook.channel)
                continue

            if channel_webhook.webhook in invalid_webhook_ids:
                # If I noticed that I can't fetch this webhook I add its channel to the list of invalid channels
                invalid_channel_ids.add(channel_webhook.channel)
                continue

            try:
                webhook = await globals.client.fetch_webhook(webhook_id)
            except Exception:
                # If I have access to the channel but not the webhook I remove that channel from targets
                invalid_channel_ids.add(channel_webhook.channel)
                invalid_webhook_ids.add(channel_webhook.webhook)
                logger.debug(
                    "Couldn't find webhook attached to channel #%s (ID: %s) when loading bridges from database.",
                    channel.name,
                    channel.id,
                )
                continue

            # Webhook and channel are valid
            add_webhook_async.append(self.webhooks.add_webhook(channel_id, webhook))
        await asyncio.gather(*add_webhook_async)

        # I will make a list of all target channels that have at least one source and delete the ones that don't
        all_target_channels: set[str] = set()
        targets_with_sources: set[str] = set()

        async_create_bridges: list[Coroutine[Any, Any, Bridge]] = []
        select_all_bridges: SQLSelect[tuple[DBBridge]] = SQLSelect(DBBridge)
        bridge_query_result: ScalarResult[DBBridge] = session.scalars(
            select_all_bridges
        )
        for bridge in bridge_query_result:
            target_id_str = bridge.target
            if target_id_str in invalid_channel_ids:
                continue

            target_id = int(target_id_str)
            target_webhook = await self.webhooks.get_webhook(target_id)
            if not target_webhook:
                # This target channel is not in my list of webhooks fetched from earlier, destroy this bridge
                logger.debug(
                    "Target channel with ID %s was in the bridges table but not in the webhooks table.",
                    target_id_str,
                )
                invalid_channel_ids.add(target_id_str)
                continue

            webhook_id_str = str(target_webhook.id)
            if webhook_id_str in invalid_webhook_ids:
                # This should almost certainly never happen
                logger.warning(
                    f"Webhook ID {webhook_id_str} successfully fetched with get_webhook() was somehow in invalid_webhook_ids."
                )
                invalid_channel_ids.add(target_id_str)
                if deleted_webhook_id := await self.webhooks.delete_channel(target_id):
                    # After deleting this channel there were no longer any channels attached to this webhook
                    invalid_webhook_ids.add(str(deleted_webhook_id))
                continue

            source_id_str = bridge.source
            source_id = int(source_id_str)
            source_channel = await globals.get_channel_from_id(source_id)
            if not source_channel:
                # If I don't have access to the source channel, delete bridges from and to it
                logger.debug(
                    "Couldn't find source channel with ID %s when loading webhooks from database.",
                    source_id_str,
                )
                invalid_channel_ids.add(source_id_str)
                if deleted_webhook_id := await self.webhooks.delete_channel(source_id):
                    # After deleting this channel there were no longer any channels attached to this webhook
                    invalid_webhook_ids.add(str(deleted_webhook_id))
            else:
                # I have access to both the source and target channels and to the webhook
                # so I can add this channel to my list of Bridges
                targets_with_sources.add(target_id_str)
                try:
                    async_create_bridges.append(
                        self.create_bridge(
                            source=source_id,
                            target=target_id,
                            webhook=target_webhook,
                            update_db=False,
                        )
                    )
                except Exception as e:
                    logger.error(
                        "Exception occurred when calling create_bridge() from load_from_database() with arguments (source=%s, target=%s, webhook=%s): %s",
                        source_id,
                        target_id,
                        target_webhook,
                        e,
                    )
                    raise e

        # Any target channels that don't have valid source channels attached to them should be deleted
        invalid_channel_ids = invalid_channel_ids.union(
            all_target_channels - targets_with_sources
        )

        # I'm going to delete all webhooks attached to invalid channels or to channels that aren't target channels
        channel_ids_with_webhooks_to_delete = {
            channel_id
            for channel_id_str in invalid_channel_ids
            if (channel_id := int(channel_id_str))
            and (await self.webhooks.get_webhook(channel_id))
        }.union(
            {
                channel_id
                for channel_id, webhook_id in self.webhooks.webhook_by_channel.items()
                if str(channel_id) not in targets_with_sources
                or str(webhook_id) in invalid_webhook_ids
            }
        )
        for channel_id in channel_ids_with_webhooks_to_delete:
            await self.webhooks.delete_channel(channel_id)

        # Gather bridge creation and webhook deletion
        await asyncio.gather(*async_create_bridges)

        # And update the database with any necessary deletions
        if (
            len(invalid_channel_ids) > 0
            or len(channel_ids_with_webhooks_to_delete) > 0
            or len(invalid_webhook_ids) > 0
        ):
            # First fetch the full list of channel IDs to delete
            channel_ids_to_delete = invalid_channel_ids.union(
                {str(channel_id) for channel_id in channel_ids_with_webhooks_to_delete}
            )

            if len(channel_ids_to_delete) > 0:
                delete_invalid_bridges = SQLDelete(DBBridge).where(
                    sql_or(
                        DBBridge.source.in_(channel_ids_to_delete),
                        DBBridge.target.in_(channel_ids_to_delete),
                    )
                )
                session.execute(delete_invalid_bridges)

                delete_invalid_messages = SQLDelete(DBMessageMap).where(
                    sql_or(
                        DBMessageMap.source_channel.in_(channel_ids_to_delete),
                        DBMessageMap.target_channel.in_(channel_ids_to_delete),
                    )
                )
                session.execute(delete_invalid_messages)

            delete_invalid_webhooks = SQLDelete(DBWebhook).where(
                sql_or(
                    DBWebhook.channel.in_(channel_ids_to_delete),
                    DBWebhook.webhook.in_(invalid_webhook_ids),
                )
            )
            session.execute(delete_invalid_webhooks)

        if close_after:
            session.commit()
            session.close()

        logger.info("Bridges successfully loaded from database!")

    @beartype
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
        source_channel = await globals.get_channel_from_id(source)
        if isinstance(target, int):
            target_channel = await globals.get_channel_from_id(target)
            validate_channels(
                target_channel=target_channel, source_channel=source_channel
            )
            target_channel = cast(discord.TextChannel | discord.Thread, target_channel)
            source_channel = cast(discord.TextChannel | discord.Thread, source_channel)
        else:
            target_channel = target
            validate_channels(source_channel=source_channel)
            source_channel = cast(discord.TextChannel | discord.Thread, source_channel)

        logger.debug(
            "Creating bridge from #%s:%s (ID: %s) to #%s:%s (ID: %s)...",
            source_channel.guild.name,
            (
                source_channel.parent.name + ":"
                if isinstance(source_channel, discord.Thread) and source_channel.parent
                else ""
            )
            + source_channel.name,
            source_channel.id,
            target_channel.guild.name,
            (
                target_channel.parent.name + ":"
                if isinstance(target_channel, discord.Thread) and target_channel.parent
                else ""
            )
            + target_channel.name,
            target_channel.id,
        )

        # First I create the Bridge in memory
        source_id = globals.get_id_from_channel(source)
        target_id = globals.get_id_from_channel(target)
        if self._outbound_bridges.get(source_id) and self._outbound_bridges[
            source_id
        ].get(target_id):
            # This bridge already exists, I won't create a new one
            bridge = self._outbound_bridges[source_id][target_id]

            if webhook:
                existing_webhook = await self.webhooks.get_webhook(target_id)
                if not existing_webhook:
                    # I don't already have a webhook registered for the target channel, I'll add one now
                    logger.debug(
                        "A bridge to channel #%s (ID: %s) already existed but it somehow did not have a webhook.",
                        target_channel.name,
                        target_id,
                    )
                    await self.webhooks.add_webhook(target_id, webhook)
                elif existing_webhook.id != webhook.id:
                    # I already have a webhook registered for the target channel and it is not this one, I'll delete this one
                    logger.debug(
                        "Tried to add webhook with ID %s to channel #%s (ID: %s) in a previously-existing bridge but the channel already had an associated webhook with ID %s.",
                        webhook.id,
                        target_channel.name,
                        target_id,
                        existing_webhook.id,
                    )
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

            existing_webhook = await self.webhooks.get_webhook(target_id)
            if webhook:
                validate_webhook(webhook, target_channel)
                if existing_webhook and existing_webhook.id != webhook.id:
                    # If I already have a webhook, I'll destroy the one being passed
                    logger.debug(
                        "Tried to add webhook with ID %s to channel #%s (ID: %s) when creating a new bridge but it already had an associated webhook with ID %s.",
                        webhook.id,
                        target_channel.name,
                        target_id,
                        existing_webhook.id,
                    )
                    try:
                        await webhook.delete()
                    except Exception:
                        pass
                else:
                    # Otherwise, I'll register the one being passed to my target channel
                    await self.webhooks.add_webhook(target, webhook)
            elif not existing_webhook:
                # Target channel does not already have a webhook, create one
                await self.webhooks.add_webhook(target_channel)

        # If I don't need to update the database I end here
        if not update_db:
            logger.debug(
                "Bridge from #%s to #%s created!",
                source_channel.name,
                target_channel.name,
            )
            return bridge

        # Add this Bridge and webhook to the DB
        logger.debug(
            "Inserting bridge from #%s to #%s into database...",
            source_channel.name,
            target_channel.name,
        )
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

            bridge_webhook = await bridge.webhook
            insert_webhook_row = await sql_upsert(
                table=DBWebhook,
                indices={"channel"},
                channel=target_id_str,
                webhook=str(bridge_webhook.id),
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
                await self.demolish_bridges(
                    source_channel=source,
                    target_channel=target,
                    one_sided=True,
                    update_db=False,
                )

            raise

        if close_after:
            session.commit()
            session.close()

        logger.debug(
            "Bridge from #%s to #%s created.", source_channel.name, target_channel.name
        )
        return bridge

    @beartype
    async def demolish_bridges(
        self,
        *,
        source_channel: discord.TextChannel | discord.Thread | int | None = None,
        target_channel: discord.TextChannel | discord.Thread | int | None = None,
        update_db: bool = True,
        session: SQLSession | None = None,
        one_sided: bool = False,
    ) -> None:
        """Destroy Bridges from source and/or to target channel.

        #### Args:
            - `source_channel`: Source channel or ID of same. Defaults to None, in which case will demolish all inbound bridges to `target_channel`.
            - `target_channel`: Target channel or ID of same. Defaults to None, in which case will demolish all outbound bridges from `source_channel`.
            - `update_db`: Whether to update the database when creating the Bridge. Defaults to True.
            - `session`: A connection to the database. Defaults to None, in which case a new one will be created to be used. Only used if `update_db` is True.
            - `one_sided`: Whether to demolish only the bridge going from `source_channel` to `target_channel`, rather than both. Defaults to False. Only used if both `source_channel` and `target_channel` are present.

        #### Raises:
            - `ArgumentError`: Neither `source_channel` nor `target_channel` were passed.
            - `HTTPException`: Deleting the webhook failed.
            - `Forbidden`: You do not have permissions to delete the webhook.
            - `ValueError`: The webhook does not have a token associated with it.
        """
        if not source_channel and not target_channel:
            err = ArgumentError(
                f"Error in function {inspect.stack()[1][3]}(): at least one of source_channel or target_channel needs to be passed as argument to demolish_bridges()."
            )
            logger.error(err)
            raise err

        # Now let's check that all relevant bridges exist
        if target_channel:
            target_id = globals.get_id_from_channel(target_channel)
            inbound_bridges_to_target = self._inbound_bridges.get(target_id)
            outbound_bridges_from_target = self._outbound_bridges.get(target_id)
        else:
            target_id = None
            inbound_bridges_to_target = None
            outbound_bridges_from_target = None

        if source_channel:
            source_id = globals.get_id_from_channel(source_channel)
            outbound_bridges_from_source = self._outbound_bridges.get(source_id)
            if outbound_bridges_from_source and target_id:
                bridge_source_to_target = outbound_bridges_from_source.get(target_id)
            else:
                bridge_source_to_target = None
        else:
            source_id = None
            outbound_bridges_from_source = None
            bridge_source_to_target = None

        if (
            source_id
            and (not target_id or one_sided)
            and (
                not outbound_bridges_from_source
                or (target_id and not bridge_source_to_target)
            )
        ):
            return

        if target_id and not source_id and not inbound_bridges_to_target:
            return

        if (
            source_id
            and target_id
            and not one_sided
            and (not outbound_bridges_from_source or not bridge_source_to_target)
            and (
                not outbound_bridges_from_target
                or not outbound_bridges_from_target.get(source_id)
            )
        ):
            return

        if source_id:
            if target_id:
                if one_sided:
                    logger.debug(
                        "Demolishing bridge from channel ID %s to channel ID %s...",
                        source_id,
                        target_id,
                    )
                else:
                    logger.debug(
                        "Demolishing bridges between channel ID %s and channel ID %s...",
                        source_id,
                        target_id,
                    )
            else:
                logger.debug("Demolishing bridges from channel ID %s...", source_id)
        else:
            logger.debug("Demolishing bridges to channel ID %s...", target_id)

        # And then we list all of the bridges we want to demolish
        if source_id:
            if target_id:
                bridges_to_demolish = [(source_id, target_id)]
                if not one_sided:
                    bridges_to_demolish.append((target_id, source_id))
            else:
                bridges_to_demolish = [
                    (source_id, tid) for tid in self._outbound_bridges[source_id].keys()
                ]
        else:
            assert target_id
            bridges_to_demolish = [
                (sid, target_id) for sid in self._inbound_bridges[target_id].keys()
            ]

        # First we delete the Bridges from memory, and webhooks if necessary
        webhooks_deleted: set[str] = set()
        for sid, tid in bridges_to_demolish:
            if from_source := self._outbound_bridges.get(sid):
                if from_source.get(tid):
                    del self._outbound_bridges[sid][tid]
                else:
                    logger.debug(
                        "Tried to demolish bridge from channel with ID %s to channel with ID %s but it was not in the list of outbound bridges.",
                        sid,
                        tid,
                    )

                if len(self._outbound_bridges[sid]) == 0:
                    del self._outbound_bridges[sid]
            else:
                logger.debug(
                    "Tried to demolish bridge from channel with ID %s but it was not in the list of outbound bridges.",
                    sid,
                )

            if to_target := self._inbound_bridges.get(tid):
                if to_target.get(sid):
                    del self._inbound_bridges[tid][sid]
                else:
                    logger.debug(
                        "Tried to demolish bridge to channel with ID %s from channel with ID %s but it was not in the list of inbound bridges.",
                        tid,
                        sid,
                    )

                if len(self._inbound_bridges[tid]) == 0:
                    del self._inbound_bridges[tid]
                    if deleted_webhook_id := await self.webhooks.delete_channel(tid):
                        webhooks_deleted.add(str(deleted_webhook_id))
            else:
                logger.debug(
                    "Tried to demolish bridge to channel with ID %s but it was not in the list of inbound bridges.",
                    tid,
                )

        # Return if we're not meant to update the DB
        if not update_db:
            logger.debug("Bridge(s) demolished.")
            return

        # Update the DB
        logger.debug("Removing bridge(s) from database...")
        close_after = False
        try:
            if not session:
                session = SQLSession(engine)
                close_after = True

            delete_demolished_bridges_and_messages: list[SQLDelete] = []
            for sid, tid in bridges_to_demolish:
                source_id_str = str(sid)
                target_id_str = str(tid)
                delete_demolished_bridges_and_messages.append(
                    SQLDelete(DBBridge).where(
                        sql_and(
                            DBBridge.source == source_id_str,
                            DBBridge.target == target_id_str,
                        )
                    )
                )
                delete_demolished_bridges_and_messages.append(
                    SQLDelete(DBMessageMap).where(
                        sql_and(
                            DBMessageMap.source_channel == source_id_str,
                            DBMessageMap.target_channel == target_id_str,
                        )
                    )
                )

            if len(webhooks_deleted) > 0:
                delete_invalid_webhooks = SQLDelete(DBWebhook).where(
                    DBWebhook.webhook.in_(webhooks_deleted)
                )
            else:
                delete_invalid_webhooks = None

            def execute_queries():
                for delete_query in delete_demolished_bridges_and_messages:
                    session.execute(delete_query)
                if delete_invalid_webhooks is not None:
                    session.execute(delete_invalid_webhooks)

            await sql_retry(execute_queries)
        except Exception:
            if close_after and session:
                session.rollback()
                session.close()

            raise

        if close_after:
            session.commit()
            session.close()

        logger.debug("Bridge(s) removed from database.")

    @beartype
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
        logger.debug("Fetching one-way bridge from %s to %s.", source, target)
        source_id = globals.get_id_from_channel(source)
        target_id = globals.get_id_from_channel(target)

        if not self._outbound_bridges.get(source_id) or not self._outbound_bridges[
            source_id
        ].get(target_id):
            return None

        return self._outbound_bridges[source_id][target_id]

    @beartype
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
        logger.debug("Fetching bridges between %s and %s.", source, target)
        return (
            self.get_one_way_bridge(source, target),
            self.get_one_way_bridge(target, source),
        )

    @beartype
    def get_outbound_bridges(
        self, source: discord.TextChannel | discord.Thread | int
    ) -> dict[int, Bridge] | None:
        """Return a dict with all Bridges from source channel, identified by the target channel id.

        #### Args:
            - `source`: Source channel or ID of same.
        """
        logger.debug("Fetching outbound bridges from %s.", source)
        return self._outbound_bridges.get(globals.get_id_from_channel(source))

    @beartype
    def get_inbound_bridges(
        self, target: discord.TextChannel | discord.Thread | int
    ) -> dict[int, Bridge] | None:
        """Return a dict with all Bridges to target channel, identified by the source channel id.

        #### Args:
            - `target`: Target channel or ID of same.
        """
        logger.debug("Fetching inbound bridges to %s.", target)
        return self._inbound_bridges.get(globals.get_id_from_channel(target))

    @overload
    async def get_reachable_channels(
        self,
        starting_channel: discord.TextChannel | discord.Thread | int,
        direction: Literal["outbound", "inbound"],
        *,
        include_webhooks: Literal[True],
        include_starting: bool = False,
    ) -> dict[int, discord.Webhook]: ...

    @overload
    async def get_reachable_channels(
        self,
        starting_channel: discord.TextChannel | discord.Thread | int,
        direction: Literal["outbound", "inbound"],
        *,
        include_webhooks: Literal[False] | None = None,
        include_starting: bool = False,
    ) -> set[int]: ...

    @beartype
    async def get_reachable_channels(
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
        logger.debug(
            "Fetching all channels reachable from %s through chains of %s bridges.",
            starting_channel,
            direction,
        )

        if direction not in {"outbound", "inbound"}:
            err = ValueError(
                f'Error in function {inspect.stack()[1][3]}(): direction argument to get_reachable_channels() must be either "outbound" or "inbound".'
            )
            logger.error(err)
            raise err

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

        reachable_channel_ids_dict: dict[int, discord.Webhook] = {}
        reachable_channel_ids_set: set[int] = set()

        while len(channel_ids_to_check) > 0:
            channel_id_to_check = channel_ids_to_check.pop()
            if channel_id_to_check in channel_ids_checked:
                continue

            channel_ids_checked.add(channel_id_to_check)
            bridges_to_check = get_bridges(channel_id_to_check)
            if not bridges_to_check:
                continue

            newly_reachable_ids = set(bridges_to_check.keys())
            if include_webhooks:
                reachable_channel_ids_dict = {
                    channel_id: await bridge.webhook
                    for channel_id, bridge in bridges_to_check.items()
                } | reachable_channel_ids_dict
            else:
                reachable_channel_ids_set = reachable_channel_ids_set.union(
                    newly_reachable_ids
                )
            channel_ids_to_check = (
                channel_ids_to_check.union(newly_reachable_ids) - channel_ids_checked
            )

        if not include_starting:
            if include_webhooks:
                reachable_channel_ids_dict.pop(starting_channel_id, None)
            else:
                reachable_channel_ids_set.discard(starting_channel_id)

        if include_webhooks:
            return reachable_channel_ids_dict
        else:
            return reachable_channel_ids_set


class Webhooks:
    """A class for keeping track of all webhooks available."""

    def __init__(self) -> None:
        # A list of webhooks by ID
        self._webhooks: dict[int, discord.Webhook] = {}

        # All channels using a given webhook
        self._channels_per_webhook: dict[int, set[int]] = {}

        # The webhook used by a given channel
        self._webhook_by_channel: dict[int, int] = {}

        # The webhook used by a parent channel
        self._webhook_by_parent: dict[int, int] = {}

    @beartype
    async def add_webhook(
        self,
        channel_or_id: discord.TextChannel | discord.Thread | int,
        webhook: discord.Webhook | None = None,
    ) -> discord.Webhook:
        """Add a webhook to my list of webhooks. If no webhook is provided and the channel is not a thread whose parent already has a webhook, a new webhook is created.

        #### Args:
            - `channel_or_id`: The channel or ID of a channel to add a webhook to.
            - `webhook`: The webhook to add, or None to try to find one or create one. Defaults to None.
        """
        channel_id = globals.get_id_from_channel(channel_or_id)

        if existing_webhook := self._webhooks.get(channel_id):
            # if I already have a webhook associated with this channel I'm gucci
            return existing_webhook

        if not webhook or not webhook.channel_id:
            # Webhook wasn't given or wasn't valid
            channel = await globals.get_channel_from_id(channel_or_id)
            validate_channels(channel=channel)
            channel = cast(discord.TextChannel | discord.Thread, channel)

            logger.debug(
                "Adding new webhook to #%s:%s (ID: %s)...",
                channel.guild.name,
                (
                    channel.parent.name + ":"
                    if isinstance(channel, discord.Thread) and channel.parent
                    else ""
                )
                + channel.name,
                channel.id,
            )

            webhook_owner = channel
            if isinstance(channel, discord.Thread) and isinstance(
                channel.parent, discord.TextChannel
            ):
                # Try to get the webhook associated with the parent channel
                webhook_owner = channel.parent
                if (webhook_id := self._webhook_by_channel.get(webhook_owner.id)) or (
                    webhook_id := self._webhook_by_parent.get(webhook_owner.id)
                ):
                    webhook = self._webhooks.get(webhook_id)
            elif webhook_id := self._webhook_by_parent.get(channel_id):
                # I am the parent of some thread that had already created a webhook
                webhook = self._webhooks.get(webhook_id)

            if not webhook or not webhook.channel_id:
                # Webhook still doesn't exist, going to create it
                webhook = await cast(discord.TextChannel, webhook_owner).create_webhook(
                    name="Channel Bridge Bot"
                )
        else:
            logger.debug(
                "Adding webhook with ID %s to channel with ID %s...",
                webhook.id,
                channel_id,
            )

        assert webhook.channel_id
        webhook_id = webhook.id

        if not self._webhooks.get(webhook_id):
            self._webhooks[webhook_id] = webhook
            self._channels_per_webhook[webhook_id] = set()
            self._webhook_by_parent[webhook.channel_id] = webhook_id
        self._channels_per_webhook[webhook_id].add(channel_id)
        self._webhook_by_channel[channel_id] = webhook_id

        logger.debug("Webhook added to channel with ID %s.", channel_id)
        return webhook

    @beartype
    async def get_webhook(
        self, channel_or_id: discord.TextChannel | discord.Thread | int
    ) -> discord.Webhook | None:
        """Return a webhook associated with a channel (or a thread's parent) or None if there isn't one.

        #### Args:
            - `channel_or_id`: The channel or ID to find a webhook for.
        """
        logger.debug("Fetching webhook associated with channel %s.", channel_or_id)

        channel_id = globals.get_id_from_channel(channel_or_id)
        if (webhook_id := self._webhook_by_channel.get(channel_id)) and (
            webhook := self._webhooks.get(webhook_id)
        ):
            return webhook

        if (webhook_id := self._webhook_by_parent.get(channel_id)) and (
            webhook := self._webhooks.get(webhook_id)
        ):
            # This thread is the owner of a webhook added by a thread
            return await self.add_webhook(channel_id, webhook)

        channel = await globals.get_channel_from_id(channel_or_id)
        if (
            isinstance(channel, discord.Thread)
            and isinstance(channel.parent, discord.TextChannel)
            and (
                (webhook_id := self._webhook_by_channel.get(channel.parent.id))
                or (webhook_id := self._webhook_by_parent.get(channel.parent.id))
            )
            and (webhook := self._webhooks.get(webhook_id))
        ):
            # This thread's parent has a webhook
            return await self.add_webhook(channel_id, webhook)

        # The channel doesn't have its own webhook associated, nor is it a thread so we can't find its parent
        return None

    @beartype
    async def delete_channel(
        self, channel_or_id: discord.TextChannel | discord.Thread | int
    ) -> int | None:
        """Delete a channel from the list of webhooks and, if there are no longer any channels associated with its webhook, delete it and return its ID.

        #### Args:
            - `channel_or_id`: The channel or ID to delete.
        """
        channel_id = globals.get_id_from_channel(channel_or_id)
        logger.debug("Deleting channel with ID %s from list of webhooks...", channel_id)

        webhook_id = self._webhook_by_channel.get(channel_id)
        if not webhook_id:
            return None

        del self._webhook_by_channel[channel_id]
        try:
            if self._channels_per_webhook.get(webhook_id):
                self._channels_per_webhook[webhook_id].remove(channel_id)
            else:
                self._channels_per_webhook[webhook_id] = set()
        except KeyError:
            pass

        logger.debug("Channel with ID %s deleted from list of webhooks.", channel_id)
        if (channels := self._channels_per_webhook.get(webhook_id)) and len(
            channels
        ) > 0:
            # There are still channels associated with this webhook
            return None

        webhook = self._webhooks.get(webhook_id)
        if not webhook:
            # The webhook doesn't exist somehow
            logger.debug("Couldn't find webhook being deleted.")
            return webhook_id

        logger.debug("Webhook associated with it no longer necessary, deleting it...")
        try:
            await webhook.delete(reason="Bridge demolition.")
        except discord.NotFound:
            pass

        if webhook.channel_id:
            del self._webhook_by_parent[webhook.channel_id]
        else:
            # Can't find the channel ID directly, webhook doesn't exist or something
            logger.debug("Couldn't find webhook by parent ID when trying to delete it.")
            for parent_id, parented_webhook_id in self._webhook_by_parent.items():
                if webhook_id == parented_webhook_id:
                    del self._webhook_by_parent[parent_id]
                    break
        del self._channels_per_webhook[webhook_id]
        del self._webhooks[webhook_id]

        logger.debug("Webhook with ID %s deleted.", webhook_id)
        return webhook_id

    @property
    def webhook_by_channel(self):
        return deepcopy(self._webhook_by_channel)


bridges = Bridges()
