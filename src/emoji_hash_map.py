import asyncio
import random
from typing import Any, Coroutine, Literal, Sequence, overload

import discord
from beartype import beartype
from sqlalchemy import Delete as SQLDelete
from sqlalchemy import ScalarResult
from sqlalchemy import Select as SQLSelect
from sqlalchemy import Update as SQLUpdate
from sqlalchemy import UpdateBase
from sqlalchemy import not_ as sql_not
from sqlalchemy.orm import Session as SQLSession

import globals
from database import DBEmoji, sql_command, sql_retry, sql_upsert
from validations import ArgumentError, logger


class EmojiHashMap:
    """A mapping between emoji IDs and hashes of their images."""

    @overload
    def __init__(self, session: SQLSession): ...

    @overload
    def __init__(self, session: SQLSession | None): ...

    @overload
    def __init__(self): ...

    @sql_command
    @beartype
    def __init__(self, session: SQLSession):
        """Initialise the emoji hash map from the emoji table.

        Parameters
        ----------
        session : :class:`~sqlalchemy.orm.Session` | None, optional
            An SQLAlchemy ORM Session connecting to the database. Defaults to None, in which case a new one will be created.
        """
        logger.info("Initialising emoji hash map...")

        self._emoji_to_hash: dict[int, str] = {}
        self._hash_to_emoji: dict[str, set[int]] = {}
        self._hash_to_available_emoji: dict[str, set[int]] = {}
        self._hash_to_internal_emoji: dict[str, int] = {}

        try:
            select_hashed_emoji: SQLSelect[tuple[DBEmoji]] = SQLSelect(DBEmoji)
            hashed_emoji_query_result: ScalarResult[DBEmoji] = session.scalars(
                select_hashed_emoji
            )
            emoji_ids_to_delete: set[str] = set()
            accessibility_flips: set[str] = set()
            for row in hashed_emoji_query_result:
                emoji_id_str = row.id
                emoji_id = int(emoji_id_str)

                emoji_hash = row.image_hash

                emoji_actually_accessible = not not globals.client.get_emoji(emoji_id)
                if (
                    row.server_id
                    and (server_id := int(row.server_id))
                    and globals.client.get_guild(server_id)
                    and not emoji_actually_accessible
                ):
                    # Emoji isn't accessible despite me being in its guild, it was probably deleted
                    logger.debug("Emoji with ID %s was not found.", emoji_id_str)
                    emoji_ids_to_delete.add(emoji_id_str)
                    continue

                emoji_registered_as_accessible = row.accessible
                if emoji_registered_as_accessible != emoji_actually_accessible:
                    accessibility_flips.add(emoji_id_str)

                self._add_emoji_to_map(
                    emoji_id,
                    emoji_hash,
                    accessible=emoji_actually_accessible,
                    server_id=row.server_id,
                )

            if len(emoji_ids_to_delete) > 0:
                session.execute(
                    SQLDelete(DBEmoji).where(DBEmoji.id.in_(emoji_ids_to_delete))
                )

            if len(accessibility_flips) > 0:
                session.execute(
                    SQLUpdate(DBEmoji)
                    .where(DBEmoji.id.in_(accessibility_flips))
                    .values(accessible=sql_not(DBEmoji.accessible))
                )
        except Exception as e:
            logger.error("An error occurred while creating an EmojiHashMap: %s", e)
            raise

        logger.info("Emoji hash map initialised.")

    @beartype
    def _add_emoji_to_map(
        self,
        emoji_id: int,
        image_hash: str,
        *,
        accessible: bool | None = None,
        server_id: int | str | None = None,
        is_internal: bool = False,
    ):
        """Add an emoji to the hash map and return its ID.

        Parameters
        ----------
        emoji_id : int
            The ID of the emoji.
        image_hash : str
            The hash of its image.
        accessible : bool | None, optional
            Whether the emoji is accessible by the bot. Defaults to None, in which case will try to figure it out from the other arguments.
        server_id : int | str | None, optional
            The ID of the server this emoji is in. If included and equal to the bot's emoji server, will set `accessible` to True and add the emoji to the internal emoji hash map. Defaults to None.
        is_internal : bool, optional
            If set to True, will set `accessible` to True and add the emoji to the internal emoji hash map. Defaults to False.
        """
        logger.debug("Adding emoji with ID %s to emoji hash map...", emoji_id)

        if not is_internal and server_id:
            server_id = int(server_id)
            if (
                emoji_server_id := globals.settings.get("emoji_server_id")
            ) and server_id == int(emoji_server_id):
                is_internal = True
                accessible = True
            elif globals.client.get_guild(server_id):
                accessible = True

        emoji_id = int(emoji_id)

        self._emoji_to_hash[emoji_id] = image_hash
        if accessible is None:
            accessible = not not globals.client.get_emoji(emoji_id)

        if not self._hash_to_emoji.get(image_hash):
            self._hash_to_emoji[image_hash] = set()
        self._hash_to_emoji[image_hash].add(emoji_id)

        if accessible:
            if not self._hash_to_available_emoji.get(image_hash):
                self._hash_to_available_emoji[image_hash] = set()
            self._hash_to_available_emoji[image_hash].add(emoji_id)
        elif (
            self._hash_to_available_emoji.get(image_hash)
            and emoji_id in self._hash_to_available_emoji[image_hash]
        ):
            self._hash_to_available_emoji[image_hash].remove(emoji_id)

        if is_internal:
            self._hash_to_internal_emoji[image_hash] = emoji_id

        logger.debug("Emoji with ID %s added to map.", emoji_id)

    @overload
    async def _add_emoji_to_database(
        self,
        *,
        emoji: discord.PartialEmoji | discord.Emoji | None = None,
        emoji_id: int | str | None = None,
        emoji_name: str | None = None,
        emoji_server_id: int | str | None = None,
        emoji_animated: bool | None = None,
        image: bytes | None = None,
        image_hash: str | None = None,
        accessible: bool = False,
        session: SQLSession,
    ): ...

    @overload
    async def _add_emoji_to_database(
        self,
        *,
        emoji: discord.PartialEmoji | discord.Emoji | None = None,
        emoji_id: int | str | None = None,
        emoji_name: str | None = None,
        emoji_server_id: int | str | None = None,
        emoji_animated: bool | None = None,
        image: bytes | None = None,
        image_hash: str | None = None,
        accessible: bool = False,
        session: SQLSession | None = None,
    ): ...

    @sql_command
    @beartype
    async def _add_emoji_to_database(
        self,
        *,
        emoji: discord.PartialEmoji | discord.Emoji | None = None,
        emoji_id: int | str | None = None,
        emoji_name: str | None = None,
        emoji_server_id: int | str | None = None,
        emoji_animated: bool | None = None,
        image: bytes | None = None,
        image_hash: str | None = None,
        accessible: bool = False,
        session: SQLSession,
    ):
        """Inserts an emoji into the `emoji` database table.

        Parameters
        ----------
        emoji : :class:`~discord.PartialEmoji` | :class:`~discord.Emoji` | None, optional
            The Discord emoji to insert. Defaults to None, in which case `emoji_id` and `emoji_name` will be used instead.
        emoji_id : int | str | None, optional
            The ID of the emoji to insert. Defaults to None. Only used if `emoji` is not present.
        emoji_name : str | None, optional
            The name of the emoji. Defaults to None, in which case the client will try to find an emoji with ID `emoji_id`. If it's included, it must start with the string "a:" if the emoji animated. Only used if `emoji` is not present.
        emoji_server_id : int | str | None, optional
            The ID of the server this emoji is from. Defaults to None.
        emoji_animated : bool | None, optional
            Whether the emoji is animated. Defaults to None, in which case its value will be inferred from the other arguments.
        image : bytes | None, optional
            The emoji image to extract a hash from. Defaults to None, in which case the hash will be calculated from the other arguments.
        image_hash : str | None, optional
            The hash of the emoji image. Defaults to None, in which case it will be calculated from the other arguments.
        accessible : bool, optional
            Whether the bot can access the emoji. Defaults to False.
        session : :class:`~sqlalchemy.orm.Session` | None, optional
            An SQLAlchemy ORM Session connecting to the database. Defaults to None, in which case a new one will be created.

        Raises
        ------
        ArgumentError
            Neither `emoji` nor `emoji_id` were passed, or `emoji_id` was passed, `emoji` and `emoji_name` weren't, and the client couldn't find an accessible emoji with ID `emoji_id`.
        ValueError
            `emoji` argument was passed and had type `PartialEmoji` but it was not a custom emoji, or `emoji_id` argument was passed and had type `str` but it was not a valid numerical ID.
        `sqlalchemy.exc.StatementError`
            An error occurred while connecting to the database.
        HTTPResponseError
            HTTP request to fetch image returned a status other than 200.
        InvalidURL
            URL generated from emoji was not valid.
        RuntimeError
            Session connection failed.
        ServerTimeoutError
            Connection to server timed out.
        """
        (
            emoji_id,
            emoji_name,
            emoji_animated_inferred,
            emoji_url,
        ) = await globals.get_emoji_information(emoji, emoji_id, emoji_name)
        logger.debug("Adding emoji with ID %s to database...", emoji_id)

        if emoji_animated is None:
            emoji_animated = emoji_animated_inferred

        if not image_hash:
            if not image:
                logger.debug("Getting image for emoji with ID %s from URL...", emoji_id)
                image = await globals.get_image_from_URL(emoji_url)
                logger.debug(
                    "Image for emoji with ID %s successfully loaded from URL.", emoji_id
                )
            image_hash = globals.hash_image(image)

        upsert_emoji = await self.upsert_emoji(
            emoji_id=emoji_id,
            emoji_name=emoji_name,
            emoji_server_id=emoji_server_id,
            emoji_animated=emoji_animated,
            image_hash=image_hash,
            accessible=accessible,
        )
        await sql_retry(lambda: session.execute(upsert_emoji))

        logger.debug("Emoji with ID %s added to database.", emoji_id)

    @beartype
    async def add_emoji(
        self,
        *,
        emoji: discord.PartialEmoji | discord.Emoji | None = None,
        emoji_id: int | str | None = None,
        emoji_name: str | None = None,
        emoji_server_id: int | str | None = None,
        emoji_animated: bool | None = None,
        image: bytes | None = None,
        image_hash: str | None = None,
        accessible: bool = False,
        is_internal: bool = False,
        update_db: bool = False,
        session: SQLSession | None = None,
    ) -> tuple[int, str]:
        """Insert an emoji into the hash map and, optionally, into the emoji database table, then return a tuple with the emoji ID and the hash of its image.

        Parameters
        ----------
        emoji : :class:`~discord.PartialEmoji` | :class:`~discord.Emoji` | None, optional
            The Discord emoji to insert. Defaults to None, in which case `emoji_id` and `emoji_name` will be used instead.
        emoji_id : int | str | None, optional
            The ID of the emoji to insert. Defaults to None. Only used if `emoji` is not present.
        emoji_name : str | None, optional
            The name of the emoji. Defaults to None, in which case the client will try to find an emoji with ID `emoji_id`. If it's included, it must start with the string "a:" if the emoji animated. Only used if `emoji` is not present.
        emoji_server_id : int | str | None, optional
            The ID of the server this emoji is from. Defaults to None.
        emoji_animated : bool | None, optional
            Whether the emoji is animated. Defaults to None, in which case its value will be inferred from the other arguments.
        image : bytes | None, optional
            The emoji image to extract a hash from. Defaults to None, in which case the hash will be calculated from the other arguments.
        image_hash : str | None, optional
            The hash of the emoji image. Defaults to None, in which case it will be calculated from the other arguments.
        accessible : bool, optional
            Whether the bot can access the emoji. Defaults to False.
        is_internal : bool, optional
            If set to True, will set `accessible` to True and add the emoji to the internal emoji hash map. Defaults to False.
        update_db : bool, optional
            Whether the emoji should be inserted into the database. Defaults to False. Including `session` is equivalent to setting this variable to True.
        session : :class:`~sqlalchemy.orm.Session` | None, optional
            An SQLAlchemy ORM Session connecting to the database. Defaults to None, but if `update_db` is set to True a new one will be created.

        Returns
        -------
        tuple[int, str]
        """
        logger.debug("Adding %s to hash map.", emoji if emoji else emoji_id)
        if not emoji_id or not image_hash:
            emoji_id, emoji_name, _, emoji_url = await globals.get_emoji_information(
                emoji,
                emoji_id,
                emoji_name,
            )
            if not image_hash:
                if not image:
                    logger.debug(
                        "Getting image for emoji with ID %s from URL...", emoji_id
                    )
                    image = await globals.get_image_from_URL(emoji_url)
                    logger.debug(
                        "Image for emoji with ID %s successfully loaded from URL.",
                        emoji_id,
                    )
                image_hash = globals.hash_image(image)
        else:
            emoji_id = int(emoji_id)

        if is_internal:
            assert (emoji_server_id_raw := globals.settings.get("emoji_server_id"))
            emoji_server_id = int(emoji_server_id_raw)

        self._add_emoji_to_map(
            emoji_id,
            image_hash,
            accessible=accessible,
            server_id=emoji_server_id,
            is_internal=is_internal,
        )

        if update_db or session:
            await self._add_emoji_to_database(
                emoji_id=emoji_id,
                emoji_name=emoji_name,
                emoji_server_id=emoji_server_id,
                emoji_animated=emoji_animated,
                image=image,
                image_hash=image_hash,
                accessible=accessible,
                session=session,
            )

        return (emoji_id, image_hash)

    @beartype
    async def upsert_emoji(
        self,
        *,
        emoji_id: int | str,
        emoji_name: str | None = None,
        emoji_server_id: int | str | None = None,
        emoji_animated: bool = False,
        image_hash: str,
        accessible: bool = False,
    ) -> UpdateBase:
        """Return an :class:`~sqlalchemy.UpdateBase` for upserting an emoji into the database.

        Parameters
        ----------
        emoji_id : int | str
            The emoji ID.
        emoji_name : str | None, optional
            The name of the emoji. Defaults to None.
        emoji_server_id : int | str | None, optional
            The ID of the server the emoji is stored in. Defaults to None.
        emoji_animated : bool, optional
            Whether the emoji is animated. Defaults to False.
        image_hash : str
            The hash of the emoji's image.
        accessible : bool, optional
            Whether the emoji is accessible by the bot. Defaults to False.

        Returns
        -------
        :class:`~sqlalchemy.UpdateBase`
        """
        if emoji_server_id:
            upsert_server_id = {"server_id": str(emoji_server_id)}
        else:
            upsert_server_id = {}

        return await sql_upsert(
            table=DBEmoji,
            indices={"id"},
            ignored_cols={"animated"},
            id=str(emoji_id),
            name=emoji_name,
            animated=emoji_animated,
            image_hash=image_hash,
            accessible=accessible,
            **upsert_server_id,
        )

    @beartype
    async def delete_emoji(
        self,
        emoji_id: int,
        update_db: bool = False,
        session: SQLSession | None = None,
    ):
        """Delete an emoji from the hash map. If `session` is included, delete it from the database also.

        Parameters
        ----------
        emoji_id : int
            The ID of the emoji to delete.
        update_db : bool, optional
            Whether the emoji should be deleted from the database. Defaults to False. Including `session` is equivalent to setting this variable to True.
        session : :class:`~sqlalchemy.orm.Session` | None, optional
            An SQLAlchemy ORM Session connecting to the database. If set to None and `update_db` is True a new one will be created. Defaults to None
        """
        if not self._emoji_to_hash.get(emoji_id):
            logger.debug(
                "Attempted to delete emoji with ID %s but it was not in the emoji hash map.",
                emoji_id,
            )
            return

        logger.debug("Deleting emoji with ID %s from hash map...", emoji_id)

        image_hash = self._emoji_to_hash[emoji_id]
        del self._emoji_to_hash[emoji_id]

        if (
            self._hash_to_emoji.get(image_hash)
            and emoji_id in self._hash_to_emoji[image_hash]
        ):
            self._hash_to_emoji[image_hash].remove(emoji_id)

        if (
            self._hash_to_available_emoji.get(image_hash)
            and emoji_id in self._hash_to_available_emoji[image_hash]
        ):
            self._hash_to_available_emoji[image_hash].remove(emoji_id)

        if self._hash_to_internal_emoji.get(image_hash):
            del self._hash_to_internal_emoji[image_hash]

        logger.debug("Emoji with ID %s deleted from map.", emoji_id)

        if update_db or session:
            await self._delete_emoji_from_db(emoji_id, session)

    @overload
    async def _delete_emoji_from_db(
        self,
        emoji_id: int,
        session: SQLSession,
    ): ...

    @overload
    async def _delete_emoji_from_db(
        self,
        emoji_id: int,
        session: SQLSession | None = None,
    ): ...

    @sql_command
    async def _delete_emoji_from_db(
        self,
        emoji_id: int,
        session: SQLSession,
    ):
        logger.debug("Deleting emoji with ID %s from database...", emoji_id)

        await sql_retry(
            lambda: session.execute(
                SQLDelete(DBEmoji).where(DBEmoji.id == str(emoji_id))
            )
        )

        logger.debug("Emoji with ID %s deleted from database.", emoji_id)

    @overload
    async def load_server_emoji(self, *, session: SQLSession | None = None):
        """Load all emoji in all servers the bot is connected to into the hash map.

        Raises
        ------
        HTTPResponseError
            HTTP request to fetch image returned a status other than 200.
        InvalidURL
            URL generated from emoji was not valid.
        RuntimeError
            Session connection failed.
        ServerTimeoutError
            Connection to server timed out.
        """
        ...

    @overload
    async def load_server_emoji(
        self,
        server_id: int,
        *,
        session: SQLSession | None = None,
    ):
        """Load all emoji in a server into the hash map.

        Parameters
        ----------
        server_id : int
            The ID of the server to load.

        Raises
        ------
        ValueError
            The server ID passed as argument does not belong to a server the bot is in.
        HTTPResponseError
            HTTP request to fetch image returned a status other than 200.
        InvalidURL
            URL generated from emoji was not valid.
        RuntimeError
            Session connection failed.
        ServerTimeoutError
            Connection to server timed out.
        """
        ...

    @overload
    async def load_server_emoji(
        self,
        server_id: int | None = None,
        *,
        session: SQLSession | None = None,
    ): ...

    @sql_command
    @beartype
    async def load_server_emoji(
        self,
        server_id: int | None = None,
        *,
        session: SQLSession,
    ):
        """Load all emoji in a server or in all servers the bot is connected to into the hash map.

        Parameters
        ----------
        server_id : int | None, optional
            The ID of the server to load. Defaults to None, in which case will load the emoji from all servers the bot is connected to.
        session : :class:`~sqlalchemy.orm.Session` | None, optional
            An SQLAlchemy ORM Session connecting to the database. Defaults to None, in which case a new one will be created.

        Raises
        ------
        ValueError
            The server ID passed as argument does not belong to a server the bot is in.
        HTTPResponseError
            HTTP request to fetch image returned a status other than 200.
        InvalidURL
            URL generated from emoji was not valid.
        RuntimeError
            Session connection failed.
        ServerTimeoutError
            Connection to server timed out.
        """
        if server_id:
            server = globals.client.get_guild(server_id)
            if not server:
                raise ValueError("Bot is not in server.")

            servers: Sequence[discord.Guild] = [server]
            logger.info("Loading emoji from server %s into hash map...", server.name)
            ending_info_message = "Emoji from server %s loaded."
        else:
            servers = globals.client.guilds
            logger.info("Loading emoji from all available servers into hash map...")
            ending_info_message = "Emoji from all available servers loaded."

        async def update_emoji(
            server_id: int | str,
            is_internal: bool,
            emoji: discord.Emoji,
        ):
            await self.delete_emoji(emoji.id)

            image = await globals.get_image_from_URL(emoji.url)
            image_hash = globals.hash_image(image)
            self._add_emoji_to_map(
                emoji.id, image_hash, accessible=True, is_internal=is_internal
            )

            return await self.upsert_emoji(
                emoji_id=emoji.id,
                emoji_name=emoji.name,
                emoji_server_id=server_id,
                emoji_animated=emoji.animated,
                image_hash=image_hash,
                accessible=True,
            )

        for server in servers:
            logger.debug("Loading server %s...", server.name)

            update_emoji_async: list[Coroutine[Any, Any, UpdateBase]] = []

            is_internal = (
                globals.emoji_server is not None
                and server.id == globals.emoji_server.id
            )
            for emoji in server.emojis:
                update_emoji_async.append(update_emoji(server.id, is_internal, emoji))

            # I'll gather the requests one server at a time
            upserts = await asyncio.gather(*update_emoji_async)
            for upsert in upserts:
                session.execute(upsert)

            logger.debug("Server %s loaded.", server.name)

        logger.info(ending_info_message)

    @beartype
    async def copy_emoji_into_server(
        self,
        *,
        emoji_to_copy: discord.PartialEmoji | None = None,
        emoji_to_copy_id: str | int | None = None,
        emoji_image: bytes | None = None,
        emoji_image_hash: str | None = None,
        emoji_to_copy_name: str | None = None,
        session: SQLSession | None = None,
    ) -> discord.Emoji | None:
        """Try to create an emoji in the emoji server and, if successful, return it.

        Parameters
        ----------
        emoji_to_copy : :class:`~discord.PartialEmoji` | None, optional
            The emoji to copy into the emoji server. Defaults to None, in which case `emoji_to_copy_name` and either `emoji_to_copy_id` or `emoji_image` are used instead.
        emoji_to_copy_id : str | int | None, optional
            The ID of the missing emoji. Defaults to None, in which case `emoji_image` is used instead. Only used if `emoji_to_copy` is None.
        emoji_image : bytes | None, optional
            An image to be directly loaded into the server. Defaults to None. Only used if `emoji_to_copy` and `emoji_to_copy_id` are None.
        emoji_image_hash : str | None, optional
            The hash of `emoji_image`. Defaults to None, in which case it will be calculated from `emoji_image`.
        emoji_to_copy_name : str | None, optional
            The name of a missing emoji, optionally preceded by an "a:" in case it's animated. Defaults to None, but must be included if either `emoji_to_copy_id` or `emoji_image` is.
        session : :class:`~sqlalchemy.orm.Session` | None, optional
            An SQLAlchemy ORM Session connecting to the database. Defaults to None, in which case a new one will be created.

        Returns
        -------
        :class:`~discord.Emoji` | None

        Raises
        ------
        ArgumentError
            The number of arguments passed is incorrect.
        ValueError
            `emoji_to_copy` argument was passed and had type `PartialEmoji` but it was not a custom emoji, or `emoji_to_copy_id` argument was passed and had type `str` but it was not a valid numerical ID.
        :class:`~discord.Forbidden`
            Emoji server permissions not set correctly.
        HTTPResponseError
            HTTP request to fetch emoji image returned a status other than 200.
        InvalidURL
            URL generated from emoji ID was not valid.
        RuntimeError
            Session connection to the server to fetch image from URL failed.
        ServerTimeoutError
            Connection to server to fetch image from URL timed out.
        """
        if not globals.emoji_server:
            return None
        emoji_server_id = globals.emoji_server.id

        if not emoji_image:
            logger.debug(
                "Copying emoji %s into emoji server.",
                emoji_to_copy if emoji_to_copy else emoji_to_copy_id,
            )

            (
                emoji_to_copy_id,
                emoji_to_copy_name,
                _,
                emoji_to_copy_url,
            ) = await globals.get_emoji_information(
                emoji_to_copy,
                emoji_to_copy_id,
                emoji_to_copy_name,
            )

            emoji_image = await globals.get_image_from_URL(emoji_to_copy_url)
        else:
            if not emoji_to_copy_name:
                raise ArgumentError(
                    "emoji_image was passed as argument to copy_emoji_into_server() but emoji_to_copy_name was not."
                )

            logger.debug("Inserting emoji from image directly into emoji server.")

        if not emoji_image_hash:
            emoji_image_hash = globals.hash_image(emoji_image)

        emoji_to_delete_id = None
        try:
            emoji = await globals.emoji_server.create_custom_emoji(
                name=emoji_to_copy_name,
                image=emoji_image,
                reason="Bridging reaction.",
            )
        except discord.Forbidden:
            logger.warning("Emoji server permissions not set correctly.")
            raise
        except discord.HTTPException:
            loaded_emojis = await globals.emoji_server.fetch_emojis()
            if len(loaded_emojis) < 50:
                # Something weird happened, the error was not due to a full server
                raise

            # Try to delete an emoji from the server and then add this again.
            emoji_to_delete: discord.Emoji | None = random.choice(loaded_emojis)
            emoji_to_delete_id = emoji_to_delete.id
            if not emoji_to_delete:
                raise Exception("emoji_to_delete failed to be fetched somehow.")

            await emoji_to_delete.delete()

            try:
                emoji = await globals.emoji_server.create_custom_emoji(
                    name=emoji_to_copy_name,
                    image=emoji_image,
                    reason="Bridging reaction.",
                )
            except discord.Forbidden:
                logger.warning("Emoji server permissions not set correctly.")
                raise

        return await self._copy_emoji_into_db(
            emoji_to_copy,
            emoji_to_copy_id,
            emoji_image_hash,
            emoji_to_copy_name,
            emoji_to_delete_id,
            emoji,
            emoji_server_id,
            session,
        )

    @overload
    async def _copy_emoji_into_db(
        self,
        emoji_to_copy: discord.PartialEmoji | None,
        emoji_to_copy_id: str | int | None,
        emoji_image_hash: str,
        emoji_to_copy_name: str,
        emoji_to_delete_id: int | None,
        emoji: discord.Emoji,
        emoji_server_id: int,
        session: SQLSession,
    ) -> discord.Emoji | None: ...

    @overload
    async def _copy_emoji_into_db(
        self,
        emoji_to_copy: discord.PartialEmoji | None,
        emoji_to_copy_id: str | int | None,
        emoji_image_hash: str,
        emoji_to_copy_name: str,
        emoji_to_delete_id: int | None,
        emoji: discord.Emoji,
        emoji_server_id: int,
        session: SQLSession | None,
    ) -> discord.Emoji | None: ...

    @sql_command
    async def _copy_emoji_into_db(
        self,
        emoji_to_copy: discord.PartialEmoji | None,
        emoji_to_copy_id: str | int | None,
        emoji_image_hash: str,
        emoji_to_copy_name: str,
        emoji_to_delete_id: int | None,
        emoji: discord.Emoji,
        emoji_server_id: int,
        session: SQLSession,
    ) -> discord.Emoji | None:
        # Copied the emoji, going to update my table
        if emoji_to_delete_id is not None:
            try:
                await self.delete_emoji(emoji_to_delete_id, session=session)
            except Exception as e:
                logger.error(
                    "An error occurred when trying to delete an emoji from the hash map while running copy_emoji_into_server(): %s",
                    e,
                )
                raise

        try:
            await self.add_emoji(
                emoji=emoji,
                emoji_server_id=emoji_server_id,
                image_hash=emoji_image_hash,
                is_internal=True,
                session=session,
            )

            if emoji_to_copy:
                await self.map_emoji(
                    external_emoji=emoji_to_copy,
                    internal_emoji=emoji,
                    image_hash=emoji_image_hash,
                    session=session,
                )
            elif emoji_to_copy_id:
                await self.map_emoji(
                    external_emoji_id=emoji_to_copy_id,
                    external_emoji_name=emoji_to_copy_name,
                    internal_emoji=emoji,
                    image_hash=emoji_image_hash,
                    session=session,
                )
        except Exception as e:
            logger.error(
                "An SQL error occurred while trying to copy an emoji into the emoji server: %s",
                e,
            )
            raise

        logger.debug("%s added to emoji server.", emoji)
        return emoji

    @beartype
    async def map_emoji(
        self,
        *,
        external_emoji: discord.PartialEmoji | None = None,
        external_emoji_id: int | str | None = None,
        external_emoji_name: str | None = None,
        internal_emoji: discord.Emoji,
        image_hash: str | None = None,
        session: SQLSession | None = None,
    ) -> bool:
        """Attempt to create a mapping between external and internal emoji, recording it locally and saving it in the emoji table. Return True if creating the map succeeded and False otherwise.

        Parameters
        ----------
        external_emoji : :class:`~discord.PartialEmoji` | None, optional
            The custom emoji that is not present in any servers the bot is in. Defaults to None, in which case `external_emoji_id` will be used to fetch it instead.
        external_emoji_id : int | str | None, optional
            The ID of a custom emoji. Defaults to None. Only used if `external_emoji` is not present.
        external_emoji_name : str | None, optional
            The name of the emoji. Defaults to None, in which case the client will try to find an emoji with ID `external_emoji_id`. If it's included, it must start with the string "a:" if the emoji animated. Only used if `external_emoji` is not present.
        internal_emoji : :class:`~discord.Emoji`
            An emoji the bot has in its emoji server.
        image_hash : str | None, optional
            The hash of the image associated with this emoji. Defaults to None, in which case will use the hash associated with `internal_emoji`.
        session : :class:`~sqlalchemy.orm.Session` | None, optional
            An SQLAlchemy ORM Session connecting to the database. Defaults to None, in which case a new one will be created.

        Returns
        -------
        bool

        Raises
        ------
        ValueError
            Incorrect number of arguments passed.
        `sqlalchemy.exc.StatementError`
            SQL statement inferred from arguments was invalid or database connection failed.
        HTTPResponseError
            HTTP request to fetch image returned a status other than 200.
        InvalidURL
            URL generated from emoji was not valid.
        RuntimeError
            Session connection failed.
        ServerTimeoutError
            Connection to server timed out.
        """
        (
            external_emoji_id,
            external_emoji_name,
            external_emoji_animated,
            _,
        ) = await globals.get_emoji_information(
            external_emoji,
            external_emoji_id,
            external_emoji_name,
        )

        full_emoji = globals.client.get_emoji(external_emoji_id)
        if full_emoji and full_emoji.guild:
            external_emoji_server_id = full_emoji.guild_id
        else:
            external_emoji_server_id = None

        try:
            if not image_hash:
                if partial_or_full_emoji := (external_emoji or full_emoji):
                    # Get the hash of the external emoji's image if we have access to it
                    image = await globals.get_image_from_URL(partial_or_full_emoji.url)
                else:
                    image = await globals.get_image_from_URL(internal_emoji.url)

                image_hash = globals.hash_image(image)

            external_emoji_accessible = not not full_emoji
            await self.add_emoji(
                emoji_id=external_emoji_id,
                emoji_name=external_emoji_name,
                emoji_server_id=external_emoji_server_id,
                emoji_animated=external_emoji_animated,
                image_hash=image_hash,
                accessible=external_emoji_accessible,
                session=session,
            )
        except Exception as e:
            logger.error(e)
            return False

        return True

    @overload
    def get_matches(
        self,
        emoji: discord.PartialEmoji | int | str,
        *,
        only_accessible: bool = False,
    ) -> frozenset[int] | None:
        """Return a frozenset with the emoji IDs of emoji available to the bot that match the emoji passed as argument, if there are any.

        Parameters
        ----------
        emoji : :class:`~discord.PartialEmoji` | int | str
            The emoji to find matches for or ID of same.
        only_accessible : bool, optional
            If set to True will return only emoji that are accessible by the bot. Defaults to False.

        Returns
        -------
        frozenset[int] | None
        """
        ...

    @overload
    def get_matches(
        self,
        emoji: discord.PartialEmoji | int | str,
        *,
        only_accessible: bool = False,
        return_str: Literal[False],
    ) -> frozenset[int] | None:
        """Return a frozenset with the emoji IDs of emoji available to the bot that match the emoji passed as argument, if there are any.

        Parameters
        ----------
        emoji : :class:`~discord.PartialEmoji` | int | str
            The emoji to find matches for or ID of same.
        only_accessible : bool, optional
            If set to True will return only emoji that are accessible by the bot. Defaults to False.
        return_str : bool, optional
            If set to True will return a frozenset of stringified IDs. Defaults to False.

        Returns
        -------
        frozenset[int] | None
        """
        ...

    @overload
    def get_matches(
        self,
        emoji: discord.PartialEmoji | int | str,
        *,
        only_accessible: bool = False,
        return_str: Literal[True],
    ) -> frozenset[str] | None:
        """Return a frozenset with the emoji IDs, converted to strings, of emoji available to the bot that match the emoji passed as argument, if there are any.

        Parameters
        ----------
        emoji : :class:`~discord.PartialEmoji` | int | str
            The emoji to find matches for or ID of same.
        only_accessible : bool, optional
            If set to True will return only emoji that are accessible by the bot. Defaults to False.
        return_str : bool, optional
            If set to True will return a frozenset of stringified IDs. Defaults to False.

        Returns
        -------
        frozenset[str] | None
        """
        ...

    @beartype
    def get_matches(
        self,
        emoji: discord.PartialEmoji | int | str,
        *,
        only_accessible: bool = False,
        return_str: bool = False,
    ) -> frozenset[int] | frozenset[str] | None:
        """Return a frozenset with the emoji IDs of emoji available to the bot that match the emoji passed as argument, if there are any.

        Parameters
        ----------
        emoji : :class:`~discord.PartialEmoji` | int | str
            The emoji to find matches for or ID of same.
        only_accessible : bool, optional
            If set to True will return only emoji that are accessible by the bot. Defaults to False.
        return_str : bool, optional
            If set to True will return a frozenset of stringified IDs. Defaults to False.

        Returns
        -------
        frozenset[int] | frozenset[str] | None
        """
        logger.debug("Fetching matches for emoji %s.", emoji)

        if isinstance(emoji, discord.PartialEmoji):
            if not emoji.id:
                logger.debug(
                    "PartialEmoji passed as argument to get_matches() was not a custom emoji."
                )
                return None
            emoji_id = emoji.id
        else:
            try:
                emoji_id = int(emoji)
            except ValueError:
                logger.debug(
                    "ID passed to get_matches() was a string that could not be converted into an integer."
                )
                return None

        if not (image_hash := self._emoji_to_hash.get(emoji_id)):
            logger.debug("No matches found for emoji with ID %s.", emoji_id)
            return None

        if only_accessible:
            hash_to_emoji = self._hash_to_available_emoji.get(image_hash)
        else:
            hash_to_emoji = self._hash_to_emoji.get(image_hash)
        if not hash_to_emoji:
            return None

        if not return_str:
            emoji_set = frozenset(hash_to_emoji)
            return emoji_set

        emoji_set = frozenset({str(id) for id in hash_to_emoji})
        return emoji_set

    @beartype
    def get_internal_equivalent(self, emoji_id: int) -> int | None:
        """Return the ID of an internal emoji matching the one passed, if available.

        Parameters
        ----------
        emoji_id : int
            The ID of the emoji to check.

        Returns
        -------
        int | None
        """
        logger.debug("Fetching internal equivalent to emoji with ID %s.", emoji_id)

        if not (image_hash := self._emoji_to_hash.get(emoji_id)):
            return None

        return self._hash_to_internal_emoji.get(image_hash)

    @overload
    def get_accessible_emoji(
        self,
        emoji_id: int,
    ) -> discord.Emoji | None:
        """Return an emoji matching the ID passed. First tries to return the one matching the ID itself, then an internal equivalent, and finally any accessible ones from other servers.

        Parameters
        ----------
        emoji_id : int
            The ID of the emoji to get.

        Returns
        -------
        :class:`~discord.Emoji` | None
        """
        ...

    @overload
    def get_accessible_emoji(
        self,
        emoji_id: int,
        *,
        skip_self: Literal[False],
    ) -> discord.Emoji | None:
        """Return an emoji matching the ID passed. First tries to return the one matching the ID itself, then an internal equivalent, and finally any accessible ones from other servers.

        Parameters
        ----------
        emoji_id : int
            The ID of the emoji to get.
        skip_self : bool, optional
            Whether the function should ignore the attempt to get an emoji associated with the ID itself and skip straight to looking for an internal equivalent. Defaults to False.

        Returns
        -------
        :class:`~discord.Emoji` | None
        """
        ...

    @overload
    def get_accessible_emoji(
        self,
        emoji_id: int,
        *,
        skip_self: Literal[True],
    ) -> discord.Emoji | None:
        """Return an emoji matching the ID passed. First tries to find an internal equivalent to the emoji, and if it can it tries to look for any accessible ones from other servers.

        Parameters
        ----------
        emoji_id : int
            The ID of the emoji to get.
        skip_self : bool, optional
            Whether the function should ignore the attempt to get an emoji associated with the ID itself and skip straight to looking for an internal equivalent. Defaults to False.

        Returns
        -------
        :class:`~discord.Emoji` | None
        """
        ...

    @beartype
    def get_accessible_emoji(
        self,
        emoji_id: int,
        *,
        skip_self: bool = False,
    ) -> discord.Emoji | None:
        """Return an emoji matching the ID passed. First tries to return the one matching the ID itself, then an internal equivalent, and finally any accessible ones.

        Parameters
        ----------
        emoji_id : int
            The ID of the emoji to get.
        skip_self : bool, optional
            Whether the function should ignore the attempt to get an emoji associated with the ID itself and skip straight to looking for an internal equivalent. Defaults to False.

        Returns
        -------
        :class:`~discord.Emoji` | None
        """
        logger.debug(
            "Fetching accessible emoji matching ID %s with skip_self = %s.",
            emoji_id,
            skip_self,
        )

        if (
            not skip_self
            and (emoji := globals.client.get_emoji(emoji_id))
            and emoji.is_usable()
        ):
            return emoji

        if (internal_emoji_id := self.get_internal_equivalent(emoji_id)) and (
            emoji := globals.client.get_emoji(internal_emoji_id)
        ):
            return emoji

        if (
            (matching_emoji_ids := self.get_matches(emoji_id))
            and (matching_emoji_id := set(matching_emoji_ids).pop())
            and (emoji := globals.client.get_emoji(matching_emoji_id))
        ):
            return emoji

        return None

    @overload
    async def get_hash(
        self,
        *,
        emoji: discord.PartialEmoji | discord.Emoji,
        session: SQLSession | None = None,
    ) -> str:
        """Return the hash of an emoji. Will ensure the emoji is in the hash map by adding it if it's not.

        Parameters
        ----------
        emoji : :class:`~discord.PartialEmoji` | :class:`~discord.Emoji`
            The emoji to get a hash for.
        session : :class:`~sqlalchemy.orm.Session` | None, optional
            An SQLAlchemy ORM Session connecting to the database. Defaults to None, in which case a new one will be created.

        Returns
        -------
        str

        Raises
        ------
        ValueError
            `emoji` had type `PartialEmoji` but it was not a custom emoji.
        """
        ...

    @overload
    async def get_hash(
        self,
        *,
        emoji_id: int | str,
        session: SQLSession | None = None,
    ) -> str | None:
        """Return the hash of an emoji. If an emoji with this ID can't be found in our existing hash map nor fetched from the client, returns None; otherwise will ensure the emoji is in the hash map by adding it if it's not.

        Parameters
        ----------
        emoji_id : int | str
            The ID of the emoji to get a hash for.
        session : :class:`~sqlalchemy.orm.Session` | None, optional
            An SQLAlchemy ORM Session connecting to the database. Defaults to None, in which case a new one will be created.

        Returns
        -------
        str | None

        Raises
        ------
        ValueError
            `emoji_id` had type `str` but it was not a valid numerical ID.
        HTTPResponseError
            HTTP request to fetch image returned a status other than 200.
        InvalidURL
            URL generated from emoji was not valid.
        RuntimeError
            Session connection failed.
        ServerTimeoutError
            Connection to server timed out.
        """
        ...

    @overload
    async def get_hash(
        self,
        *,
        emoji: None,
        emoji_id: int | str,
        emoji_name: None,
        session: SQLSession | None = None,
    ) -> str | None:
        """Return the hash of an emoji. If an emoji with this ID can't be found in our existing hash map nor fetched from the client, returns None; otherwise will ensure the emoji is in the hash map by adding it if it's not.

        Parameters
        ----------
        emoji_id : int | str
            The ID of the emoji to get a hash for.
        session : :class:`~sqlalchemy.orm.Session` | None, optional
            An SQLAlchemy ORM Session connecting to the database. Defaults to None, in which case a new one will be created.

        Returns
        -------
        str | None

        Raises
        ------
        ValueError
            `emoji_id` had type `str` but it was not a valid numerical ID.
        HTTPResponseError
            HTTP request to fetch image returned a status other than 200.
        InvalidURL
            URL generated from emoji was not valid.
        RuntimeError
            Session connection failed.
        ServerTimeoutError
            Connection to server timed out.
        """
        ...

    @overload
    async def get_hash(
        self,
        *,
        emoji_id: int | str,
        emoji_name: str,
        session: SQLSession | None = None,
    ) -> str:
        """Return the hash of an emoji. Will ensure the emoji is in the hash map by adding it if it's not.

        Parameters
        ----------
        emoji_id : int | str
            The ID of the emoji to get a hash for.
        emoji_name : str
            The name of the emoji. It must start with the string "a:" if the emoji animated.
        session : :class:`~sqlalchemy.orm.Session` | None, optional
            An SQLAlchemy ORM Session connecting to the database. Defaults to None, in which case a new one will be created.

        Returns
        -------
        str

        Raises
        ------
        ValueError
            `emoji_id` had type `str` but it was not a valid numerical ID.
        """
        ...

    @beartype
    async def get_hash(
        self,
        *,
        emoji: discord.PartialEmoji | discord.Emoji | None = None,
        emoji_id: int | str | None = None,
        emoji_name: str | None = None,
        session: SQLSession | None = None,
    ) -> str | None:
        """Return the hash of an emoji.

        If only `emoji_id` is passed and the emoji can't be found in our existing hash map nor inferred from the ID itself, returns None; otherwise will ensure the emoji is in the hash map by adding it if it's not.

        Parameters
        ----------
        emoji : :class:`~discord.PartialEmoji` | :class:`~discord.Emoji` | None, optional
            The emoji to get a hash for. Defaults to None, in which case `emoji_id` is used instead.
        emoji_id : int | str | None, optional
            The ID of the emoji to get a hash for. Defaults to None. Only used if `emoji` isn't present.
        emoji_name : str | None, optional
            The name of the emoji. Defaults to None, in which case the client will try to find an emoji with ID `emoji_id`. If it's included, it must start with the string "a:" if the emoji animated. Only used if `emoji` is not present.
        session : :class:`~sqlalchemy.orm.Session` | None, optional
            An SQLAlchemy ORM Session connecting to the database. Defaults to None, in which case a new one will be created.

        Returns
        -------
        str | None

        Raises
        ------
        ValueError
            `emoji` argument was passed and had type `PartialEmoji` but it was not a custom emoji, or `emoji_id` argument was passed and had type `str` but it was not a valid numerical ID.
        HTTPResponseError
            HTTP request to fetch image returned a status other than 200.
        InvalidURL
            URL generated from emoji was not valid.
        RuntimeError
            Session connection failed.
        ServerTimeoutError
            Connection to server timed out.
        """
        logger.debug("Getting hash for emoji %s.", emoji if emoji else emoji_id)

        if (
            (not emoji)
            and emoji_id
            and (not emoji_name)
            and (emoji_hash := self._emoji_to_hash.get(int(emoji_id)))
        ):
            return emoji_hash

        try:
            return await self.ensure_hash_map(
                emoji=emoji,
                emoji_id=emoji_id,
                emoji_name=emoji_name,
                session=session,
            )
        except ArgumentError:
            return None

    @beartype
    async def ensure_hash_map(
        self,
        *,
        emoji: discord.Emoji | discord.PartialEmoji | None = None,
        emoji_id: int | str | None = None,
        emoji_name: str | None = None,
        session: SQLSession | None = None,
    ) -> str:
        """Check that the emoji is in the hash map and, if not, add it to the map and to the database, then return the hash.

        Parameters
        ----------
        emoji : :class:`~discord.PartialEmoji` | :class:`~discord.Emoji` | None, optional
            The emoji to get a hash for. Defaults to None, in which case `emoji_id` is used instead.
        emoji_id : int | str | None, optional
            The ID of the emoji to get a hash for. Defaults to None. Only used if `emoji` isn't present.
        emoji_name : str | None, optional
            The name of the emoji. Defaults to None, in which case the client will try to find an emoji with ID `emoji_id`. If it's included, it must start with the string "a:" if the emoji animated. Only used if `emoji` is not present.
        session : :class:`~sqlalchemy.orm.Session` | None, optional
            An SQLAlchemy ORM Session connecting to the database. Defaults to None, in which case a new one will be created.

        Returns
        -------
        str

        Raises
        ------
        ArgumentError
            The number of arguments passed is incorrect.
        ValueError
            `emoji` argument was passed and had type `PartialEmoji` but it was not a custom emoji, or `emoji_id` argument was passed and had type `str` but it was not a valid numerical ID.
        HTTPResponseError
            HTTP request to fetch image returned a status other than 200.
        InvalidURL
            URL generated from emoji was not valid.
        RuntimeError
            Session connection failed.
        ServerTimeoutError
            Connection to server timed out.
        """
        logger.debug(
            "Ensuring that emoji %s is in hash map.", emoji if emoji else emoji_id
        )

        emoji_id, emoji_name, _, _ = await globals.get_emoji_information(
            emoji,
            emoji_id,
            emoji_name,
        )

        if already_existing_hash := self._emoji_to_hash.get(emoji_id):
            return already_existing_hash

        _, image_hash = await self.add_emoji(
            emoji=emoji,
            emoji_id=emoji_id,
            emoji_name=emoji_name,
            update_db=True,
            session=session,
        )

        return image_hash


map: EmojiHashMap
