import asyncio
from typing import Any, Coroutine, Literal, Sequence, overload

import discord
from beartype import beartype
from sqlalchemy import Delete as SQLDelete
from sqlalchemy import ScalarResult
from sqlalchemy import Select as SQLSelect
from sqlalchemy import Update as SQLUpdate
from sqlalchemy import UpdateBase
from sqlalchemy import not_ as sql_not
from sqlalchemy.exc import StatementError as SQLError
from sqlalchemy.orm import Session as SQLSession

import globals
from database import DBEmoji, engine, sql_retry, sql_upsert


class EmojiHashMap:
    """
    A mapping between emoji IDs and hashes of their images.
    """

    @beartype
    def __init__(self, session: SQLSession | None = None):
        """Initialise the emoji hash map from the emoji table.

        #### Args:
            - `session`: A connection to the database. Defaults to None.
        """
        self._emoji_to_hash: dict[int, str] = {}
        self._hash_to_emoji: dict[str, set[int]] = {}
        self._hash_to_available_emoji: dict[str, set[int]] = {}
        self._hash_to_internal_emoji: dict[str, int] = {}

        close_after = False
        try:
            if not session:
                session = SQLSession(engine)
                close_after = True

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
        except SQLError as e:
            if close_after and session:
                session.rollback()
                session.close()

            raise e

        if close_after:
            session.commit()
            session.close()

    @beartype
    def _add_emoji_to_map(
        self,
        emoji_id: int | str,
        image_hash: str,
        *,
        accessible: bool | None = None,
        server_id: int | str | None = None,
        is_internal: bool | None = None,
    ) -> tuple[int, str]:
        """Add an emoji to the hash map.

        #### Args:
            - `emoji_id`: The ID of the emoji.
            - `image_hash`: The hash of its image.
            - `accessible`: Whether the emoji is accessible to the bot. Defaults to None, in which case will try to figure it out from the other arguments.
            - `server_id`: The ID of the server this emoji is in. If included and equal to the bot's emoji server, will set `accessible` to True and add the emoji to the internal emoji hash map.
            - `is_internal`: If set to True, will set `accessible` to True and add the emoji to the internal emoji hash map.

        #### Raises:
            - `ValueError`: `emoji_id` or `server_id` were not valid numerical IDs.

        #### Returns:
            - `tuple[int, str]`: A tuple with the emoji ID and the hash of its image.
        """
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

        return (emoji_id, image_hash)

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
        session: SQLSession | None = None,
    ):
        """Inserts an emoji into the `emoji` database table.

        #### Args:
            - `emoji`: The Discord emoji to insert. Defaults to None, in which case `emoji_id` and `emoji_name` will be used instead.
            - `emoji_id`: The ID of the emoji to insert. Defaults to None, in which case `emoji` will be used instead.
            - `emoji_name`: The name of the emoji. Defaults to None, but must be included if `emoji_id` is. If it starts with `"a:"` the emoji will be marked as animated.
            - `emoji_server_id`: The ID of the server this emoji is from. Defaults to None.
            - `emoji_animated`: Whether the emoji is animated. Defaults to None, in which case its value will be inferred from the other arguments.
            - `image`: The emoji image to extract a hash from. Defaults to None, in which case the hash will be calculated from the other arguments.
            - `image_hash`: The hash of the emoji image. Defaults to None, in which case it will be calculated from the other arguments.
            - `accessible`: Whether the bot can access the emoji. Defaults to False.
            - `session`: A connection to the database. Defaults to None, in which case a new one will be created to be used.

        #### Raises:
            - `ArgumentError`: The number of arguments passed is incorrect.
            - `ValueError`: `emoji` argument was passed and had type `PartialEmoji` but it was not a custom emoji, or `emoji_id` argument was passed and had type `str` but it was not a valid numerical ID.
            - `SQLError`: An error occurred while connecting to the database.
            - `HTTPResponseError`: HTTP request to fetch image returned a status other than 200.
            - `InvalidURL`: URL generated from emoji was not valid.
            - `RuntimeError`: Session connection failed.
            - `ServerTimeoutError`: Connection to server timed out.
        """
        emoji_id, emoji_name, emoji_animated_inferred, emoji_url = (
            globals.get_emoji_information(emoji, emoji_id, emoji_name)
        )

        if emoji_animated is None:
            emoji_animated = emoji_animated_inferred

        if not image_hash:
            if not image:
                image = await globals.get_image_from_URL(emoji_url)
            image_hash = globals.hash_image(image)

        close_after = False
        try:
            if not session:
                session = SQLSession(engine)
                close_after = True

            upsert_emoji = await self.upsert_emoji(
                emoji_id=emoji_id,
                emoji_name=emoji_name,
                emoji_server_id=emoji_server_id,
                emoji_animated=emoji_animated,
                image_hash=image_hash,
                accessible=accessible,
            )
            await sql_retry(lambda: session.execute(upsert_emoji))
        except SQLError as e:
            if close_after and session:
                session.rollback()
                session.close()

            raise e

        if close_after:
            session.commit()
            session.close()

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
        is_internal: bool | None = None,
        session: SQLSession | Literal[True] | None = None,
    ) -> tuple[int, str]:
        """Insert an emoji into the hash map and, optionally, into the `emoji` database table.

        #### Args:
            - `emoji`: The Discord emoji to insert. Defaults to None, in which case `emoji_id` and `emoji_name` will be used instead.
            - `emoji_id`: The ID of the emoji to insert. Defaults to None, in which case `emoji` will be used instead.
            - `emoji_name`: The name of the emoji. Defaults to None, but must be included if `emoji_id` is. If it starts with `"a:"` the emoji will be marked as animated.
            - `emoji_server_id`: The ID of the server this emoji is from. Defaults to None.
            - `emoji_animated`: Whether the emoji is animated. Defaults to None, in which case its value will be inferred from the other arguments.
            - `image`: The emoji image to extract a hash from. Defaults to None, in which case the hash will be calculated from the other arguments.
            - `image_hash`: The hash of the emoji image. Defaults to None, in which case it will be calculated from the other arguments.
            - `accessible`: Whether the bot can access the emoji. Defaults to False.
            - `is_internal`: If set to True, will set `accessible` to True and add the emoji to the internal emoji hash map.
            - `session`: A connection to the database. If set to None, the emoji will not be inserted into the database; if set to True, a new session will be created to perform the database operations.

        #### Raises:
            - `ArgumentError`: The number of arguments passed is incorrect.
            - `ValueError`: `emoji` argument was passed and had type `PartialEmoji` but it was not a custom emoji, or `emoji_id` argument was passed and had type `str` but it was not a valid numerical ID.
            - `SQLError`: An error occurred while connecting to the database.
            - `HTTPResponseError`: HTTP request to fetch image returned a status other than 200.
            - `InvalidURL`: URL generated from emoji was not valid.
            - `RuntimeError`: Session connection failed.
            - `ServerTimeoutError`: Connection to server timed out.

        #### Returns:
            - `tuple[int, str]`: A tuple with the emoji ID and the hash of its image.
        """
        if not emoji_id or not image_hash:
            emoji_id, emoji_name, _, emoji_url = globals.get_emoji_information(
                emoji, emoji_id, emoji_name
            )
            if not image_hash:
                if not image:
                    image = await globals.get_image_from_URL(emoji_url)
                image_hash = globals.hash_image(image)

        if is_internal:
            assert (emoji_server_id_raw := globals.settings.get("emoji_server_id"))
            emoji_server_id = int(emoji_server_id_raw)

        emoji_id, image_hash = self._add_emoji_to_map(
            emoji_id,
            image_hash,
            accessible=accessible,
            server_id=emoji_server_id,
            is_internal=is_internal,
        )

        if session:
            close_after = False
            try:
                if isinstance(session, bool):
                    session = SQLSession(engine)
                    close_after = True

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
            except SQLError as e:
                if close_after and isinstance(session, SQLSession):
                    session.rollback()
                    session.close()

                raise e

            if close_after:
                session.commit()
                session.close()

        return (emoji_id, image_hash)

    @beartype
    async def upsert_emoji(
        self,
        *,
        emoji_id: int | str,
        emoji_name: str | None = None,
        emoji_server_id: int | str | None = None,
        emoji_animated: bool | None = None,
        image_hash: str,
        accessible: bool | None = None,
    ) -> UpdateBase:
        """Return an `UpdateBase` for upserting an emoji into the database.

        #### Args:
            - `emoji_id`: The emoji ID.
            - `emoji_name`: The name of the emoji. Defaults to None.
            - `emoji_server_id`: The ID of the server the emoji is stored in. Defaults to None.
            - `emoji_animated`: Whether the emoji is animated. Defaults to None, in which case it will be considered False.
            - `image_hash`: The hash of the emoji's image.
            - `accessible`: Whether the emoji is accessible by the bot. Defaults to None, in which case it will be considered False.
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
            animated=not not emoji_animated,
            image_hash=image_hash,
            accessible=not not accessible,
            **upsert_server_id,
        )

    @beartype
    async def delete_emoji(
        self, emoji_id: int, session: SQLSession | Literal[True] | None = None
    ):
        """Delete an emoji from the hash map. If `session` is included, delete it from the database also.

        #### Args:
            - `emoji_id`: The ID of the emoji to delete.
            - `session`: A connection to the database, or True in case a new one should be created.
        """
        if not self._emoji_to_hash.get(emoji_id):
            return

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

        if session:
            close_after = False
            try:
                if isinstance(session, bool):
                    session = SQLSession(engine)
                    close_after = True

                await sql_retry(
                    lambda: session.execute(
                        SQLDelete(DBEmoji).where(DBEmoji.id == str(emoji_id))
                    )
                )
            except SQLError as e:
                if close_after and isinstance(session, SQLSession):
                    session.rollback()
                    session.close()

                raise e

            if close_after:
                session.commit()
                session.close()

    @beartype
    async def load_server_emoji(self, server_id: int | None = None):
        """Load all emoji in a server (or in all servers the bot is connected to) into the hash map.

        #### Args:
            - `server_id`: The ID of the server to load. Defaults to None, in which case will load the emoji from all servers the bot is connected to.

        #### Raises:
            - `ValueError`: The server ID passed as argument does not belong to a server the bot is in.
            - `HTTPResponseError`: HTTP request to fetch image returned a status other than 200.
            - `InvalidURL`: URL generated from emoji was not valid.
            - `RuntimeError`: Session connection failed.
            - `ServerTimeoutError`: Connection to server timed out.
        """
        if server_id:
            server = globals.client.get_guild(server_id)
            if not server:
                raise ValueError("Bot is not in server.")

            servers: Sequence[discord.Guild] = [server]
        else:
            servers = globals.client.guilds

        async def update_emoji(
            server_id: int | str, is_internal: bool, emoji: discord.Emoji
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

        session = None
        try:
            with SQLSession(engine) as session:
                for server in servers:
                    update_emoji_async: list[Coroutine[Any, Any, UpdateBase]] = []

                    is_internal = (
                        globals.emoji_server is not None
                        and server.id == globals.emoji_server.id
                    )
                    for emoji in server.emojis:
                        update_emoji_async.append(
                            update_emoji(server.id, is_internal, emoji)
                        )

                    # I'll gather the requests one server at a time
                    upserts = await asyncio.gather(*update_emoji_async)
                    for upsert in upserts:
                        session.execute(upsert)

                session.commit()
        except SQLError as e:
            if session:
                session.rollback()
                session.close()

            raise e

    @overload
    def get_matches(
        self,
        emoji: discord.PartialEmoji | int | str,
        *,
        only_accessible: bool | None = None,
    ) -> frozenset[int] | None: ...

    @overload
    def get_matches(
        self,
        emoji: discord.PartialEmoji | int | str,
        *,
        only_accessible: bool | None = None,
        return_str: Literal[False],
    ) -> frozenset[int] | None: ...

    @overload
    def get_matches(
        self,
        emoji: discord.PartialEmoji | int | str,
        *,
        only_accessible: bool | None = None,
        return_str: Literal[True],
    ) -> frozenset[str] | None: ...

    @beartype
    def get_matches(
        self,
        emoji: discord.PartialEmoji | int | str,
        *,
        only_accessible: bool | None = None,
        return_str: bool | None = False,
    ) -> frozenset[int] | frozenset[str] | None:
        """Return a frozenset with the emoji IDs of emoji available to the bot that match the emoji passed as argument.

        #### Args:
            - `emoji`: The emoji to find matches for or ID of same.
            - `only_accessible`: If set to True will return only emoji that are accessible by the bot. Defaults to False.
            - `return_str`: If set to True will return a frozenset of stringified IDs. Defaults to False.
        """
        if isinstance(emoji, discord.PartialEmoji):
            if not emoji.id:
                return None
            emoji_id = emoji.id
        else:
            try:
                emoji_id = int(emoji)
            except ValueError:
                return None

        if not self._emoji_to_hash.get(emoji_id):
            return None

        image_hash = self._emoji_to_hash[emoji_id]
        if only_accessible:
            hash_to_emoji = self._hash_to_available_emoji
        else:
            hash_to_emoji = self._hash_to_emoji
        if not hash_to_emoji.get(image_hash):
            return None

        if not return_str:
            emoji_set = frozenset(hash_to_emoji[image_hash])
            return emoji_set

        return frozenset({str(id) for id in hash_to_emoji[image_hash]})

    @beartype
    def get_internal_equivalent(self, emoji_id: int) -> int | None:
        """Return the ID of an internal emoji matching the one passed, if available.

        #### Args:
            - `emoji_id`: The ID of the emoji to check.
        """
        if not self._emoji_to_hash.get(emoji_id):
            return None

        image_hash = self._emoji_to_hash[emoji_id]
        if not self._hash_to_internal_emoji.get(image_hash):
            return None

        return self._hash_to_internal_emoji[image_hash]

    @beartype
    def get_accessible_emoji(
        self, emoji_id: int, *, skip_self: bool = False
    ) -> discord.Emoji | None:
        """Return an emoji matching the ID passed. First tries to return the one matching the ID itself, then an internal equivalent, and finally any accessible ones.

        #### Args:
            - `emoji_id`: The ID of the emoji to get.
            - `skip_self`: Whether the function should ignore the attempt to get an emoji associated with the ID itself. Defaults to False.
        """
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

        If only `emoji_id` is passed and the emoji can't be found in our existing hash map, returns None; otherwise will ensure the emoji is in the hash map.

        #### Args:
            - `emoji`: The emoji to get a hash for. Defaults to None, in which case `emoji_id` is used instead.
            - `emoji_id`: The ID of the emoji to get a hash for. Defaults to None, in which case `emoji` is used instead.
            - `emoji_name`: The name of the emoji. Defaults to None, but must be included if `emoji_id` is. If it starts with `"a:"` the emoji will be marked as animated.
            - `session`: A connection to the database. Defaults to None, in which case a new one will be created for any necessary DB operations.

        #### Raises:
            - `ArgumentError`: The number of arguments passed is incorrect.
            - `ValueError`: `emoji` argument was passed and had type `PartialEmoji` but it was not a custom emoji, or `emoji_id` argument was passed and had type `str` but it was not a valid numerical ID.
            - `HTTPResponseError`: HTTP request to fetch image returned a status other than 200.
            - `InvalidURL`: URL generated from emoji was not valid.
            - `RuntimeError`: Session connection failed.
            - `ServerTimeoutError`: Connection to server timed out.
        """
        if not emoji and emoji_id and not emoji_name:
            emoji_id = int(emoji_id)
            if self._emoji_to_hash.get(emoji_id):
                return self._emoji_to_hash[emoji_id]
            return None

        return await self.ensure_hash_map(
            emoji=emoji, emoji_id=emoji_id, emoji_name=emoji_name, session=session
        )

    @beartype
    async def ensure_hash_map(
        self,
        *,
        emoji: discord.Emoji | discord.PartialEmoji | None = None,
        emoji_id: int | str | None = None,
        emoji_name: str | None = None,
        session: SQLSession | None = None,
    ) -> str | None:
        """Check that the emoji is in the hash map and, if not, add it to the map and to the database, then return the hash.

        #### Args:
            - `emoji`: A Discord emoji. Defaults to None, in which case the values below will be used instead.
            - `emoji_id`: The ID of an emoji. Defaults to None, in which case the value above will be used instead.
            - `emoji_name`: The name of the emoji. Defaults to None, but must be included if `emoji_id` is. If it starts with `"a:"` the emoji will be marked as animated.
            - `session`: A connection to the database. Defaults to None, in which case a new one will be created for the DB operations.

        #### Raises:
            - `ArgumentError`: The number of arguments passed is incorrect.
            - `ValueError`: `emoji` argument was passed and had type `PartialEmoji` but it was not a custom emoji, or `emoji_id` argument was passed and had type `str` but it was not a valid numerical ID.
            - `HTTPResponseError`: HTTP request to fetch image returned a status other than 200.
            - `InvalidURL`: URL generated from emoji was not valid.
            - `RuntimeError`: Session connection failed.
            - `ServerTimeoutError`: Connection to server timed out.
        """
        emoji_id, emoji_name, _, _ = globals.get_emoji_information(
            emoji, emoji_id, emoji_name
        )

        if self._emoji_to_hash.get(emoji_id):
            return self._emoji_to_hash[emoji_id]

        _, image_hash = await self.add_emoji(
            emoji=emoji, emoji_id=emoji_id, emoji_name=emoji_name, session=session
        )

        return image_hash


map: EmojiHashMap
