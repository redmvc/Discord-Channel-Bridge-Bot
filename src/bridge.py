import asyncio
import inspect
from copy import deepcopy
from typing import TYPE_CHECKING, overload

import discord
from sqlalchemy.exc import StatementError
from sqlalchemy.ext.asyncio import AsyncSession as SQLSession

import common
from database import (
    _MISSING_SESSION,
    DBBridge,
    DBMessageMap,
    DBWebhook,
    get_sql_insert_ignore_duplicate_query,
    get_sql_upsert_query,
    register_observed_event,
    sql_command,
)
from database import functions as F
from validations import (
    ArgumentError,
    TextChannelOrThread,
    WebhookChannelError,
    logger,
    validate_channels,
    validate_webhook,
)

if TYPE_CHECKING:
    from typing import Callable, Literal


class Bridge:
    """A bridge from a source channel to a target channel.

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
        source: TextChannelOrThread | int,
        target: TextChannelOrThread | int,
    ) -> "Bridge":
        """Create and return an outbound Bridge from source channel to target channel.

        Parameters
        ----------
        source : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
            Source channel or ID of same.
        target : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
            Target channel or ID of same.

        Raises
        ------
        ChannelTypeError
            Either the source or the target channel is not a text channel nor a thread off a text channel.

        Returns
        -------
        :class:`~bridge.Bridge`
        """
        logger.debug("Creating bridge from %s to %s.", source, target)

        if isinstance(source, int):
            validate_channels(source=await common.get_channel_from_id(source))
        if isinstance(target, int):
            validate_channels(target=await common.get_channel_from_id(target))

        self = Bridge()
        self._source_id = common.get_id_from_channel(source)
        self._target_id = common.get_id_from_channel(target)

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
    async def source_channel(self) -> TextChannelOrThread:
        return await common.get_channel_from_id(
            self.source_id,
            ensure_text_or_thread=True,
        )

    @property
    def target_id(self) -> int:
        assert self._target_id
        return self._target_id

    @property
    async def target_channel(self) -> TextChannelOrThread:
        return await common.get_channel_from_id(
            self.target_id,
            ensure_text_or_thread=True,
        )

    @property
    async def webhook(self) -> discord.Webhook:
        webhook = await bridges.webhooks.get_webhook(self.target_id)
        assert webhook
        return webhook


class Bridges:
    """A list of all bridges created.

    Attributes
    ----------
    webhooks : :class:`~bridge.Webhooks`
        The webhooks associated with each target channel.
    """

    def __init__(self) -> None:
        self._outbound_bridges: dict[int, dict[int, Bridge]] = {}
        self._inbound_bridges: dict[int, dict[int, Bridge]] = {}
        self.webhooks = Webhooks()

    @sql_command
    async def load_from_database(self, *, session: SQLSession = _MISSING_SESSION):
        """Load all bridges saved in the bot's connected database.

        Parameters
        ----------
        session : :class:`~sqlalchemy.ext.asyncio.AsyncSession`, optional
            An async SQLAlchemy Session connecting to the database. If it's not present, a new one will be created.
        """
        self._outbound_bridges: dict[int, dict[int, Bridge]] = {}
        self._inbound_bridges: dict[int, dict[int, Bridge]] = {}
        self.webhooks = Webhooks()

        # I am going to try to identify all existing bridges and webhooks and add them to my tracking
        # and also delete the ones that aren't valid or accessible
        invalid_channel_ids: set[str] = set()
        invalid_webhook_ids: set[str] = set()
        fetched_webhooks: dict[int, discord.Webhook] = {}

        webhook_query_result = await DBWebhook(session).collect()
        for channel_webhook in webhook_query_result:
            channel_id = int(channel_webhook.channel)
            webhook_id = int(channel_webhook.webhook)

            channel = await common.get_channel_from_id(channel_id)
            if not (channel and isinstance(channel, TextChannelOrThread)):
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
                if webhook_id in fetched_webhooks:
                    webhook = fetched_webhooks[webhook_id]
                else:
                    webhook = await common.client.fetch_webhook(webhook_id)
                    fetched_webhooks[webhook_id] = webhook
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
            await self.webhooks.add_webhook(channel_id, webhook)

        # I will make a list of all target channels that have at least one source and delete the ones that don't
        all_target_channels: set[str] = set()
        targets_with_sources: set[str] = set()

        bridge_query_result = await DBBridge(session).collect()
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
            source_channel = await common.get_channel_from_id(source_id)
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
                    await self.create_bridge(
                        source=source_id,
                        target=target_id,
                        webhook=target_webhook,
                        update_db=False,
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
        invalid_channel_ids = invalid_channel_ids | (
            all_target_channels - targets_with_sources
        )

        # I'm going to delete all webhooks attached to invalid channels or to channels that aren't target channels
        channel_ids_with_webhooks_to_delete = {
            channel_id
            for channel_id_str in invalid_channel_ids
            if (
                (channel_id := int(channel_id_str))
                and (await self.webhooks.get_webhook(channel_id))
            )
        } | {
            channel_id
            for channel_id, webhook_id in self.webhooks.webhook_by_channel.items()
            if (
                (str(channel_id) not in targets_with_sources)
                or (str(webhook_id) in invalid_webhook_ids)
            )
        }
        for channel_id in channel_ids_with_webhooks_to_delete:
            await self.webhooks.delete_channel(channel_id)

        # And update the database with any necessary deletions
        if (
            (len(invalid_channel_ids) > 0)
            or (len(channel_ids_with_webhooks_to_delete) > 0)
            or (len(invalid_webhook_ids) > 0)
        ):
            # First fetch the full list of channel IDs to delete
            channel_ids_to_delete = invalid_channel_ids | {
                str(channel_id) for channel_id in channel_ids_with_webhooks_to_delete
            }

            if len(channel_ids_to_delete) > 0:
                await (
                    DBBridge(session)
                    .where(
                        F.col("source").isin(channel_ids_to_delete)
                        | F.col("target").isin(channel_ids_to_delete)
                    )
                    .delete()
                )

            await (
                DBWebhook(session)
                .where(
                    F.col("channel").isin(channel_ids_to_delete)
                    | F.col("webhook").isin(invalid_webhook_ids)
                )
                .delete()
            )

        logger.info("Bridges successfully loaded from database!")

    @sql_command
    async def create_bridge(
        self,
        *,
        source: TextChannelOrThread | int,
        target: TextChannelOrThread | int,
        webhook: discord.Webhook | None = None,
        update_db: bool = True,
        session: SQLSession = _MISSING_SESSION,
    ) -> Bridge:
        """Create a new Bridge from source channel to target channel (and a new webhook if necessary).

        Parameters
        ----------
        source : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
            Source channel or ID of same.
        target : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
            Target channel or ID of same.
        webhook : :class:`~discord.Webhook` | None, optional
            An already-existing webhook connecting these channels. Defaults to None, in which case a new one will be created.
        update_db : bool, optional
            Whether to update the database when creating the Bridge. Defaults to True.
        session : :class:`~sqlalchemy.ext.asyncio.AsyncSession`, optional
            An async SQLAlchemy Session connecting to the database. If it's not present, a new one will be created.

        Returns
        -------
        :class:`~bridge.Bridge`

        Raises
        ------
        ChannelTypeError
            Either the source or the target channel is not a text channel nor a thread off a text channel.
        HTTPException
            Deleting an existing webhook or creating a new one failed.
        Forbidden
            You do not have permissions to create or delete webhooks.
        """
        validated_channels = validate_channels(
            source=await common.get_channel_from_id(source),
            target=await common.get_channel_from_id(target),
        )
        source_channel = validated_channels["source"]
        target_channel = validated_channels["target"]

        logger.debug(
            "Creating bridge from #%s:%s (ID: %s) to #%s:%s (ID: %s)...",
            source_channel.guild.name,
            (
                (
                    source_channel.parent.name + ":"
                    if (
                        isinstance(source_channel, discord.Thread)
                        and source_channel.parent
                    )
                    else ""
                )
                + source_channel.name
            ),
            source_channel.id,
            target_channel.guild.name,
            (
                (
                    target_channel.parent.name + ":"
                    if (
                        isinstance(target_channel, discord.Thread)
                        and target_channel.parent
                    )
                    else ""
                )
                + target_channel.name
            ),
            target_channel.id,
        )

        # Register the bridge creation event
        await register_observed_event(source_channel.id, session=session)

        # First I create the Bridge in memory
        source_id = common.get_id_from_channel(source)
        target_id = common.get_id_from_channel(target)
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
                try:
                    validate_webhook(webhook, target_channel)
                except WebhookChannelError:
                    # Failed to find a webhook in the target channel, recreate one if it doesn't already exist
                    try:
                        await webhook.delete()
                    except Exception:
                        pass
                    webhook = None

            if webhook:
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

        if source_id not in common.pinned_messages_cache:
            logger.debug(
                "Loading pinned messages from <#%s> into local cache...", source_id
            )
            await toggle_pins_helper(source_channel, session=session)
            logger.debug("<#%s>'s pinned messages loaded.", source_id)

        # If I don't need to update the database I end here
        if not update_db:
            logger.debug(
                "Bridge from #%s to #%s created!",
                source_channel.name,
                target_channel.name,
            )
            return bridge

        try:
            return await self._add_bridge_to_db(
                source_channel=source_channel,
                source_id=source_id,
                target_channel=target_channel,
                target_id=target_id,
                bridge=bridge,
                session=session,
            )
        except Exception as e:
            if isinstance(e, StatementError):
                await self.demolish_bridges(
                    source_channel=source_id,
                    target_channel=target_id,
                    one_sided=True,
                    update_db=False,
                )

            raise

    @sql_command
    async def _add_bridge_to_db(
        self,
        *,
        source_channel: TextChannelOrThread,
        source_id: int,
        target_channel: TextChannelOrThread,
        target_id: int,
        bridge: Bridge,
        session: SQLSession = _MISSING_SESSION,
    ) -> Bridge:
        """Add the bridge from source channel to target channel to the database."""
        logger.debug(
            "Inserting bridge from #%s to #%s into database...",
            source_channel.name,
            target_channel.name,
        )

        target_id_str = str(target_id)
        insert_bridge_row = await get_sql_insert_ignore_duplicate_query(
            DBBridge,
            indices={"source", "target"},
            source=str(source_id),
            target=target_id_str,
        )

        bridge_webhook = await bridge.webhook
        insert_webhook_row = await get_sql_upsert_query(
            DBWebhook,
            indices={"channel"},
            channel=target_id_str,
            webhook=str(bridge_webhook.id),
        )

        await session.execute(insert_bridge_row)
        await session.execute(insert_webhook_row)

        logger.debug(
            "Bridge from #%s to #%s inserted into database.",
            source_channel.name,
            target_channel.name,
        )
        return bridge

    async def demolish_bridges(
        self,
        *,
        source_channel: TextChannelOrThread | int | None = None,
        target_channel: TextChannelOrThread | int | None = None,
        update_db: bool = True,
        session: SQLSession = _MISSING_SESSION,
        one_sided: bool = False,
    ):
        """Destroy Bridges from source and/or to target channel.

        Parameters
        ----------
        source_channel : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int | None, optional
            Source channel or ID of same. Defaults to None, in which case will demolish all inbound bridges to `target_channel`.
        target_channel : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int | None, optional
            Target channel or ID of same. Defaults to None, in which case will demolish all outbound bridges from `source_channel`.
        update_db : bool, optional
            Whether to update the database when creating the Bridge. Defaults to True.
        session : :class:`~sqlalchemy.ext.asyncio.AsyncSession`, optional
            An async SQLAlchemy Session connecting to the database. If it's not present, a new one will be created. Only used if `update_db` is True.
        one_sided : bool, optional
            Whether to demolish only the bridge going from `source_channel` to `target_channel`, rather than both. Defaults to False. Only used if both `source_channel` and `target_channel` are present.

        Raises
        ------
        ArgumentError
            Neither `source_channel` nor `target_channel` were passed.
        HTTPException
            Deleting the webhook failed.
        Forbidden
            You do not have permissions to delete the webhook.
        ValueError
            The webhook does not have a token associated with it.
        """
        if (not source_channel) and (not target_channel):
            err = ArgumentError(
                f"Error in function {inspect.stack()[1][3]}(): at least one of source_channel or target_channel needs to be passed as argument to demolish_bridges()."
            )
            logger.error(err)
            raise err

        # Now let's check that all relevant bridges exist
        if target_channel:
            target_id = common.get_id_from_channel(target_channel)
            inbound_bridges_to_target = self._inbound_bridges.get(target_id)
            outbound_bridges_from_target = self._outbound_bridges.get(target_id)
        else:
            target_id = None
            inbound_bridges_to_target = None
            outbound_bridges_from_target = None

        if source_channel:
            source_id = common.get_id_from_channel(source_channel)
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
            and ((not target_id) or one_sided)
            and (
                (not outbound_bridges_from_source)
                or (target_id and (not bridge_source_to_target))
            )
        ):
            return

        if target_id and (not source_id) and (not inbound_bridges_to_target):
            return

        if (
            source_id
            and target_id
            and (not one_sided)
            and ((not outbound_bridges_from_source) or (not bridge_source_to_target))
            and (
                (not outbound_bridges_from_target)
                or (not outbound_bridges_from_target.get(source_id))
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
            elif outbound_bridges_from_source:
                bridges_to_demolish = [
                    (source_id, tid) for tid in outbound_bridges_from_source.keys()
                ]
            else:
                bridges_to_demolish = []
        elif target_id and inbound_bridges_to_target:
            bridges_to_demolish = [
                (sid, target_id) for sid in inbound_bridges_to_target.keys()
            ]
        else:
            bridges_to_demolish = []

        # First we delete the Bridges from memory, and webhooks if necessary
        webhooks_deleted: set[str] = set()
        for sid, tid in bridges_to_demolish:
            if from_source := self._outbound_bridges.get(sid):
                if from_source.get(tid):
                    try:
                        del self._outbound_bridges[sid][tid]
                    except Exception:
                        pass
                else:
                    logger.debug(
                        "Tried to demolish bridge from channel with ID %s to channel with ID %s but it was not in the list of outbound bridges.",
                        sid,
                        tid,
                    )

                if (bridges_from_source := self._outbound_bridges.get(sid)) and (
                    len(bridges_from_source) == 0
                ):
                    try:
                        del self._outbound_bridges[sid]
                    except Exception:
                        pass
            else:
                logger.debug(
                    "Tried to demolish bridge from channel with ID %s but it was not in the list of outbound bridges.",
                    sid,
                )

            if to_target := self._inbound_bridges.get(tid):
                if to_target.get(sid):
                    try:
                        del self._inbound_bridges[tid][sid]
                    except Exception:
                        pass
                else:
                    logger.debug(
                        "Tried to demolish bridge to channel with ID %s from channel with ID %s but it was not in the list of inbound bridges.",
                        tid,
                        sid,
                    )

                if (bridges_to_target := self._inbound_bridges.get(tid)) and (
                    len(bridges_to_target) == 0
                ):
                    try:
                        del self._inbound_bridges[tid]
                    except Exception:
                        pass
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

        await self._remove_bridges_from_db(
            bridges_to_demolish=bridges_to_demolish,
            webhooks_deleted=webhooks_deleted,
            session=session,
        )

    @sql_command
    async def _remove_bridges_from_db(
        self,
        *,
        bridges_to_demolish: list[tuple[int, int]],
        webhooks_deleted: set[str],
        session: SQLSession = _MISSING_SESSION,
    ):
        """Remove bridges from database."""
        logger.debug("Removing bridge(s) from database...")

        for sid, tid in bridges_to_demolish:
            source_id_str = str(sid)
            target_id_str = str(tid)
            await (
                DBBridge(session)
                .where(
                    (F.col("source") == F.lit(source_id_str))
                    & (F.col("target") == F.lit(target_id_str))
                )
                .delete()
            )

        if len(webhooks_deleted) > 0:
            await (
                DBWebhook(session)
                .where(F.col("webhook").isin(webhooks_deleted))
                .delete()
            )

        logger.debug("Bridge(s) removed from database.")

    def get_one_way_bridge(
        self,
        source: TextChannelOrThread | int,
        target: TextChannelOrThread | int,
    ) -> Bridge | None:
        """Return the Bridge from source channel to target channel if it exists, and None otherwise.

        Parameters
        ----------
        source : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
            Source channel or ID of same.
        target : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
            Target channel or ID of same.

        Returns
        -------
        :class:`~bridge.Bridge` | None
        """
        logger.debug("Fetching one-way bridge from %s to %s.", source, target)
        source_id = common.get_id_from_channel(source)
        target_id = common.get_id_from_channel(target)

        if not (bridges_from_source := self._outbound_bridges.get(source_id)):
            return None

        return bridges_from_source.get(target_id)

    def get_two_way_bridge(
        self,
        source: TextChannelOrThread | int,
        target: TextChannelOrThread | int,
    ) -> tuple[Bridge | None, Bridge | None]:
        """Return a tuple of Bridges, the first element of which is the Bridge from source to target and the second of which is the Bridge from target to source.

        Parameters
        ----------
        source : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
            Source channel or ID of same.
        target : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
            Target channel or ID of same.

        Returns
        -------
        tuple[:class:`~bridge.Bridge` | None, :class:`~bridge.Bridge` | None]
        """
        logger.debug("Fetching bridges between %s and %s.", source, target)
        return (
            self.get_one_way_bridge(source, target),
            self.get_one_way_bridge(target, source),
        )

    def get_outbound_bridges(
        self,
        source: TextChannelOrThread | int,
    ) -> dict[int, Bridge] | None:
        """Return a dict with all Bridges from source channel, identified by the target channel id.

        Parameters
        ----------
        source : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
            Source channel or ID of same.

        Returns
        -------
        dict[int, :class:`~bridge.Bridge`] | None
        """
        logger.debug("Fetching outbound bridges from %s.", source)
        return self._outbound_bridges.get(common.get_id_from_channel(source))

    def get_channels_with_outbound_bridges(self) -> set[int]:
        """Return a set with the IDs of all channels that have outbound bridges coming from them.

        Returns
        -------
        set[int]
        """
        return set(self._outbound_bridges.keys())

    def get_inbound_bridges(
        self,
        target: TextChannelOrThread | int,
    ) -> dict[int, Bridge] | None:
        """Return a dict with all Bridges to target channel, identified by the source channel id.

        Parameters
        ----------
        target : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
            Target channel or ID of same.

        Returns
        -------
        dict[int, :class:`~bridge.Bridge`] | None
        """
        logger.debug("Fetching inbound bridges to %s.", target)
        return self._inbound_bridges.get(common.get_id_from_channel(target))

    @overload
    async def get_reachable_channels(
        self,
        starting_channel: TextChannelOrThread | int,
        direction: "Literal['outbound', 'inbound'] | None" = None,
        *,
        include_webhooks: "Literal[True]",
        include_starting: bool = False,
    ) -> dict[int, discord.Webhook]:
        """Return a dictionary with those channels as keys and one webhook attached to each of those channels as values.

        Parameters
        ----------
        starting_channel : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
            The channel other channels must be reachable from, or ID of same.
        direction : Literal["outbound", "inbound"] | None, optional
            Whether to look down an outbound chain or up an inbound chain. Defaults to None, in which case both will be checked.
        include_starting : bool, optional
            Whether to include the starting channel ID in the list. Defaults to False.

        Returns
        -------
        dict[int, :class:`~discord.Webhook`]
        """
        pass

    @overload
    async def get_reachable_channels(
        self,
        starting_channel: TextChannelOrThread | int,
        direction: "Literal['outbound', 'inbound'] | None" = None,
        *,
        include_webhooks: "Literal[False]",
        include_starting: bool = False,
    ) -> set[int]:
        """Return a set with all channel IDs reachable from a given source channel down an unbroken series of outbound or inbound bridges.

        Parameters
        ----------
        starting_channel : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
            The channel other channels must be reachable from, or ID of same.
        direction : Literal["outbound", "inbound"] | None, optional
            Whether to look down an outbound chain or up an inbound chain. Defaults to None, in which case both will be checked.
        include_starting : bool, optional
            Whether to include the starting channel ID in the list. Defaults to False.

        Returns
        -------
        set[int]
        """
        pass

    @overload
    async def get_reachable_channels(
        self,
        starting_channel: TextChannelOrThread | int,
        direction: "Literal['outbound', 'inbound'] | None" = None,
        *,
        include_starting: bool = False,
    ) -> set[int]:
        """Return a set with all channel IDs reachable from a given source channel down an unbroken series of outbound or inbound bridges.

        Parameters
        ----------
        starting_channel : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
            The channel other channels must be reachable from, or ID of same.
        direction : Literal["outbound", "inbound"] | None, optional
            Whether to look down an outbound chain or up an inbound chain. Defaults to None, in which case both will be checked.
        include_starting : bool, optional
            Whether to include the starting channel ID in the list. Defaults to False.

        Returns
        -------
        set[int]
        """
        pass

    async def get_reachable_channels(
        self,
        starting_channel: TextChannelOrThread | int,
        direction: "Literal['outbound', 'inbound'] | None" = None,
        *,
        include_webhooks: bool = False,
        include_starting: bool = False,
    ) -> set[int] | dict[int, discord.Webhook]:
        """If `include_webhooks` is `False` (default), return a set with all channel IDs reachable from a given source channel down an unbroken series of outbound or inbound bridges; if it's `True`, return a dictionary with those channels as keys and one webhook attached to each of those channels as values.

        Parameters
        ----------
        starting_channel : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
            The channel other channels must be reachable from, or ID of same.
        direction : Literal["outbound", "inbound"] | None, optional
            Whether to look down an outbound chain or up an inbound chain. Defaults to None, in which case both will be checked.
        include_webhooks : bool, optional
            Whether to include a list of webhooks attached to the reachable channels in the output. Will only include one webhook per channel. Defaults to False.
        include_starting : bool, optional
            Whether to include the starting channel ID in the list. Defaults to False.

        Returns
        -------
        set[int] | dict[int, :class:`~discord.Webhook`]
        """
        if direction is None:
            # Both directions
            outbound_result = await self.get_reachable_channels(
                starting_channel,
                "outbound",
                include_webhooks=include_webhooks,
                include_starting=include_starting,
            )
            inbound_result = await self.get_reachable_channels(
                starting_channel,
                "inbound",
                include_webhooks=include_webhooks,
                include_starting=include_starting,
            )

            assert (
                isinstance(outbound_result, set) and isinstance(inbound_result, set)
            ) or (
                isinstance(outbound_result, dict) and isinstance(inbound_result, dict)
            )
            return outbound_result | inbound_result  # pyright: ignore[reportOperatorIssue]

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

        get_bridges: "Callable[[TextChannelOrThread | int], dict[int, Bridge] | None]"
        if direction == "outbound":
            get_bridges = self.get_outbound_bridges
        else:
            get_bridges = self.get_inbound_bridges

        starting_channel_id = common.get_id_from_channel(starting_channel)
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
                reachable_channel_ids_set = (
                    reachable_channel_ids_set | newly_reachable_ids
                )
            channel_ids_to_check = (
                channel_ids_to_check | newly_reachable_ids
            ) - channel_ids_checked

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

    def __init__(self):
        # A list of webhooks by ID
        self._webhooks: dict[int, discord.Webhook] = {}

        # All channels using a given webhook
        self._channels_per_webhook: dict[int, set[int]] = {}

        # The webhook used by a given channel
        self._webhook_by_channel: dict[int, int] = {}

        # The webhook used by a parent channel
        self._webhook_by_parent: dict[int, int] = {}

    async def add_webhook(
        self,
        channel_or_id: TextChannelOrThread | int,
        webhook: discord.Webhook | None = None,
    ) -> discord.Webhook:
        """Add a webhook to my list of webhooks. If no webhook is provided and the channel is not a thread whose parent already has a webhook, a new webhook is created.

        Parameters
        ----------
        channel_or_id : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
            The channel or ID of a channel to add a webhook to.
        webhook : :class:`~discord.Webhook` | None, optional
            The webhook to add, or None to try to find one or create one. Defaults to None.

        Returns
        -------
        :class:`~discord.Webhook`
        """
        channel_id = common.get_id_from_channel(channel_or_id)

        if existing_webhook := self._webhooks.get(channel_id):
            # if I already have a webhook associated with this channel I'm gucci
            return existing_webhook

        if (not webhook) or (not webhook.channel_id):
            # Webhook wasn't given or wasn't valid
            channel = validate_channels(
                channel=await common.get_channel_from_id(channel_or_id)
            )["channel"]

            logger.debug(
                "Adding new webhook to #%s:%s (ID: %s)...",
                channel.guild.name,
                (
                    (
                        channel.parent.name + ":"
                        if isinstance(channel, discord.Thread) and channel.parent
                        else ""
                    )
                    + channel.name
                ),
                channel.id,
            )

            webhook_owner = await common.get_channel_parent(channel)
            if (webhook_id := self._webhook_by_channel.get(webhook_owner.id)) or (
                webhook_id := self._webhook_by_parent.get(webhook_owner.id)
            ):
                # Try to get the webhook associated with a thread or its parent
                webhook = self._webhooks.get(webhook_id)

            if (not webhook) or (not webhook.channel_id):
                # Webhook still doesn't exist, going to create it
                webhook = await webhook_owner.create_webhook(name="Channel Bridge Bot")
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

    async def get_webhook(
        self,
        channel_or_id: TextChannelOrThread | int,
    ) -> discord.Webhook | None:
        """Return a webhook associated with a channel (or a thread's parent) or None if there isn't one.

        Parameters
        ----------
        channel_or_id : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
            The channel or ID to find a webhook for.

        Returns
        -------
        :class:`~discord.Webhook` | None
        """
        logger.debug("Fetching webhook associated with channel %s.", channel_or_id)

        channel_id = common.get_id_from_channel(channel_or_id)
        if (webhook_id := self._webhook_by_channel.get(channel_id)) and (
            webhook := self._webhooks.get(webhook_id)
        ):
            return webhook

        if (webhook_id := self._webhook_by_parent.get(channel_id)) and (
            webhook := self._webhooks.get(webhook_id)
        ):
            # This thread is the owner of a webhook added by a thread
            return await self.add_webhook(channel_id, webhook)

        channel = await common.get_channel_from_id(channel_or_id)
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

    async def delete_channel(
        self,
        channel_or_id: TextChannelOrThread | int,
    ) -> int | None:
        """Delete a channel from the list of webhooks and, if there are no longer any channels associated with its webhook, delete it and return its ID.

        Parameters
        ----------
        channel_or_id : :class:`~discord.TextChannel` | :class:`~discord.Thread` | int
            The channel or ID to delete.

        Returns
        -------
        int | None
        """
        channel_id = common.get_id_from_channel(channel_or_id)
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
        if (channels := self._channels_per_webhook.get(webhook_id)) and (
            len(channels) > 0
        ):
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


@sql_command(commit_results=False)
async def toggle_pins_helper(
    channel: TextChannelOrThread,
    *,
    session: SQLSession = _MISSING_SESSION,
):
    """Bridge message pins and unpins.

    Parameters
    ----------
    channel : :class:`~discord.abc.GuildChannel` | :class:`~discord.Thread`
        The guild channel that had its pins updated.
    session : :class:`~sqlalchemy.ext.asyncio.AsyncSession`, optional
        An async SQLAlchemy Session connecting to the database. If it's not present, a new one will be created.
    """
    channel_id = channel.id

    # Fetch current pins and build set of IDs, with retry for Cloudflare 429s
    current_pin_ids: set[int] = set()
    max_attempts = 1
    for attempt in range(max_attempts + 1):
        try:
            current_pin_ids = {msg.id async for msg in channel.pins()}
            break
        except discord.HTTPException as e:
            if (e.status == 429) and (attempt < max_attempts):
                delay = 5 * (2**attempt)
                logger.warning(
                    "Cloudflare rate limited when fetching pins for <#%s>. Retrying in %ss (attempt %s/%s)...",
                    channel_id,
                    delay,
                    attempt + 1,
                    max_attempts,
                )
                await asyncio.sleep(delay)
            else:
                logger.error(
                    "Failed to fetch pins for <#%s> after retries: %s",
                    channel_id,
                    e,
                )
                return

    # Get previous state from cache
    previous_pin_ids = common.pinned_messages_cache.get(channel_id)
    if previous_pin_ids is None:
        # First event for a channel not in cache (e.g. bridge created after startup)
        common.pinned_messages_cache[channel_id] = current_pin_ids
        return

    # Compute diff
    newly_pinned = current_pin_ids - previous_pin_ids
    newly_unpinned = previous_pin_ids - current_pin_ids

    # Update cache
    common.pinned_messages_cache[channel_id] = current_pin_ids

    if not (newly_pinned or newly_unpinned):
        return

    # Get reachable destination channels
    reachable_channels = await bridges.get_reachable_channels(channel_id, "outbound")
    if not reachable_channels:
        return

    # Identify who pinned by looking for the pins_add system message
    pinner: discord.Member | discord.User | None = None
    if newly_pinned:
        try:
            async for msg in channel.history(limit=5):
                if msg.type == discord.MessageType.pins_add:
                    pinner = msg.author
                    break
        except discord.HTTPException:
            pass

    for pin_toggled_msg_id, pin in [
        *((msg_id, True) for msg_id in newly_pinned),
        *((msg_id, False) for msg_id in newly_unpinned),
    ]:
        pin_toggled_msg_src = await (
            DBMessageMap(session)
            .where(
                (F.col("source_message") == F.lit(pin_toggled_msg_id))
                | (F.col("target_message") == F.lit(pin_toggled_msg_id))
            )
            .limit(1)
            .collect()
        )
        if not pin_toggled_msg_src:
            continue

        source_msg_id = int(pin_toggled_msg_src[0].source_message)
        bridged_messages = await (
            DBMessageMap(session)
            .where(
                (F.col("source_message") == F.lit(source_msg_id))
                & (
                    F.col("source_channel").isin(reachable_channels)
                    | F.col("target_channel").isin(reachable_channels)
                )
            )
            .collect()
        )

        if (int(bridged_messages[0].source_message) != pin_toggled_msg_id) and (
            int(bridged_messages[0].source_channel) in reachable_channels
        ):
            await _toggle_pin_message(
                int(bridged_messages[0].source_channel),
                int(bridged_messages[0].source_message),
                pin,
                pinner=pinner if pin else None,
            )

        for message_row in bridged_messages:
            target_channel_id = int(message_row.target_channel)
            target_message_id = int(message_row.target_message)
            if (target_channel_id not in reachable_channels) or (
                target_message_id == pin_toggled_msg_id
            ):
                continue

            # Only pin the primary message (order 0 or NULL), not split parts
            if message_row.target_message_order and (
                int(message_row.target_message_order) != 0
            ):
                continue

            await _toggle_pin_message(
                target_channel_id,
                target_message_id,
                pin,
                pinner=pinner if pin else None,
            )


async def _toggle_pin_message(
    channel_id: int,
    target_msg_id: int,
    pin: bool,
    *,
    pinner: "discord.Member | discord.User | None" = None,
):
    """Pin or unpin a message in a channel, and send a notification embed on pin."""
    target_channel = await common.get_channel_from_id(channel_id)
    if not isinstance(target_channel, TextChannelOrThread):
        return

    if not target_channel.permissions_for(target_channel.guild.me).pin_messages:
        return

    partial_message = target_channel.get_partial_message(target_msg_id)
    try:
        common.expected_pin_changes[channel_id] += 1
        if pin:
            await partial_message.pin(
                reason=f"Bridging pin{f' by user with ID {pinner.id}' if pinner else ''}."
            )
        else:
            await partial_message.unpin(reason="Bridging unpin.")
    except discord.HTTPException as e:
        common.expected_pin_changes[channel_id] -= 1
        if common.expected_pin_changes[channel_id] == 0:
            del common.expected_pin_changes[channel_id]
        logger.warning(
            "Failed to %s message %s in channel <#%s>: %s",
            "pin" if pin else "unpin",
            target_msg_id,
            channel_id,
            e,
        )
        return

    if not (pin and pinner):
        return

    # Send a notification embed for pins
    embed = discord.Embed.from_dict(
        {
            "description": f"-# \U0001f4cc The above message was pinned by <@{pinner.id}>.",
            "type": "rich",
        }
    )

    try:
        await target_channel.send(embed=embed)
    except discord.HTTPException as e:
        logger.warning(
            "Failed to send pin notification in channel <#%s>: %s",
            channel_id,
            e,
        )
