import asyncio
from typing import Any, Coroutine, Literal, Sequence, cast, overload

import discord
from sqlalchemy import ScalarResult
from sqlalchemy import Select as SQLSelect
from sqlalchemy import Update as SQLUpdate
from sqlalchemy import UpdateBase
from sqlalchemy import not_ as sql_not
from sqlalchemy.exc import StatementError as SQLError
from sqlalchemy.orm import Session as SQLSession

import globals
from database import DBEmoji, engine, sql_retry, sql_upsert
from validations import validate_types


class EmojiHashMap:
    """
    A mapping between emoji IDs and hashes of their images.

    Attributes
    ----------
    emoji_to_hash : dict[int, int]
        A dictionary whose keys are emoji IDs and whose values are the hashes of their images.

    hash_to_available_emoji :  dict[int, set[int]]
        A dictionary whose keys are image hashes and whose values are sets with the IDs of all emoji available to the bot with that image hash.

    hash_to_internal_emoji : dict[int, int]
        A dictionary whose keys are image hashes and whose values are IDs for the internal emoji with that hash.
    """

    def __init__(self, session: SQLSession | None = None):
        """Initialise the emoji hash map from the emoji table.

        #### Args:
            - `session`: A connection to the database. Defaults to None.
        """
        if session:
            validate_types({"session": (session, SQLSession)})
            close_after = False
        else:
            session = SQLSession(engine)
            close_after = True

        self.emoji_to_hash: dict[int, int] = {}
        self.hash_to_available_emoji: dict[int, set[int]] = {}
        self.hash_to_internal_emoji: dict[int, int] = {}

        try:
            select_hashed_emoji: SQLSelect = SQLSelect(DBEmoji)
            hashed_emoji_query_result: ScalarResult[DBEmoji] = session.scalars(
                select_hashed_emoji
            )
            accessibility_flips: set[str] = set()
            for row in hashed_emoji_query_result:
                emoji_id_str = row.id
                emoji_id = int(emoji_id_str)

                emoji_hash = int(row.image_hash)

                emoji_registered_as_accessible = row.accessible
                emoji_actually_accessible = not not globals.client.get_emoji(emoji_id)
                if emoji_registered_as_accessible != emoji_actually_accessible:
                    accessibility_flips.add(emoji_id_str)

                self._add_emoji_to_map(
                    emoji_id,
                    emoji_hash,
                    accessible=emoji_actually_accessible,
                    server_id=row.server_id,
                )

            if len(accessibility_flips) > 0:
                session.execute(
                    SQLUpdate(DBEmoji)
                    .where(DBEmoji.id.in_(accessibility_flips))
                    .values(accessible=sql_not(DBEmoji.accessible))
                )
        except SQLError as e:
            if close_after:
                session.rollback()
                session.close()

            raise e

        if close_after:
            session.commit()
            session.close()

    def _add_emoji_to_map(
        self,
        emoji_id: int | str,
        image_hash: int | str,
        *,
        accessible: bool | None = None,
        server_id: int | str | None = None,
        is_internal: bool | None = None,
    ):
        """Add an emoji to the hash map.

        #### Args:
            - `emoji_id`: The ID of the emoji.
            - `image_hash`: The hash of its image.
            - `accessible`: Whether the emoji is accessible to the bot. Defaults to None, in which case will try to figure it out from the other arguments.
            - `server_id`: The ID of the server this emoji is in. If included and equal to the bot's emoji server, will set `accessible` to True and add the emoji to the internal emoji hash map.
            - `is_internal`: If set to True, will set `accessible` to True and add the emoji to the internal emoji hash map.

        #### Raises:
            - `ValueError`: `emoji_id`, `image_hash`, or `server_id` were not valid numerical IDs.
        """
        types_to_validate: dict[str, tuple] = {
            "emoji_id": (emoji_id, (int, str)),
            "image_hash": (image_hash, (int, str)),
        }
        if is_internal:
            types_to_validate["is_internal"] = (is_internal, bool)
            accessible = True
        else:
            if accessible:
                types_to_validate["accessible"] = (accessible, bool)
            if server_id:
                types_to_validate["server_id"] = (server_id, (int, str))
        validate_types(types_to_validate)

        if not is_internal and server_id:
            server_id = int(server_id)
            if server_id == globals.settings.get("emoji_server_id"):
                is_internal = True
                accessible = True
            elif globals.client.get_guild(server_id):
                accessible = True

        emoji_id = int(emoji_id)
        image_hash = int(image_hash)

        self.emoji_to_hash[emoji_id] = image_hash
        if accessible is None:
            accessible = not not globals.client.get_emoji(emoji_id)

        if accessible:
            if not self.hash_to_available_emoji.get(image_hash):
                self.hash_to_available_emoji[image_hash] = set()
            self.hash_to_available_emoji[image_hash].add(emoji_id)
        elif (
            self.hash_to_available_emoji.get(image_hash)
            and emoji_id in self.hash_to_available_emoji[image_hash]
        ):
            self.hash_to_available_emoji[image_hash].remove(emoji_id)

        if is_internal:
            self.hash_to_internal_emoji[image_hash] = emoji_id

    async def _add_emoji_to_database(
        self,
        *,
        emoji: discord.PartialEmoji | discord.Emoji | None = None,
        emoji_id: int | str | None = None,
        emoji_name: str | None = None,
        emoji_server_id: int | str | None = None,
        emoji_animated: bool | None = None,
        image: bytes | None = None,
        image_hash: int | str | None = None,
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

        types_to_validate: dict[str, tuple] = {}
        if emoji_server_id:
            types_to_validate["emoji_server_id"] = (emoji_server_id, (int, str))
        if emoji_animated is not None:
            types_to_validate["emoji_animated"] = (emoji_animated, bool)
        else:
            emoji_animated = emoji_animated_inferred
        if image_hash:
            types_to_validate["image_hash"] = (image_hash, (int, str))
        elif image:
            types_to_validate["image"] = (image, bytes)
        if accessible:
            types_to_validate["accessible"] = (accessible, bool)
        if session:
            types_to_validate["session"] = (session, SQLSession)
        validate_types(types_to_validate)

        if session:
            close_after = False
        else:
            session = SQLSession(engine)
            close_after = True

        if not image_hash:
            if not image:
                image = await globals.get_image_from_URL(emoji_url)
            image_hash = hash(image)

        try:
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
            if session and close_after:
                session.rollback()
                session.close()

            raise e

        if session and close_after:
            session.commit()
            session.close()

    async def add_emoji(
        self,
        *,
        emoji: discord.PartialEmoji | discord.Emoji | None = None,
        emoji_id: int | str | None = None,
        emoji_name: str | None = None,
        emoji_server_id: int | str | None = None,
        emoji_animated: bool | None = None,
        image: bytes | None = None,
        image_hash: int | str | None = None,
        accessible: bool = False,
        is_internal: bool | None = None,
        session: SQLSession | Literal[True] | None = None,
    ):
        """Inserts an emoji into the hash map and, optionally, into the `emoji` database table.

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
        """
        if session:
            validate_types({"session": (session, (SQLSession, bool))})

        if not emoji_id or not image_hash:
            emoji_id, emoji_name, _, emoji_url = globals.get_emoji_information(
                emoji, emoji_id, emoji_name
            )
            if not image_hash:
                if not image:
                    image = await globals.get_image_from_URL(emoji_url)
                image_hash = hash(image)

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

        if session:
            if isinstance(session, bool):
                session = SQLSession(engine)
                close_after = True
            else:
                close_after = False

            try:
                await self._add_emoji_to_database(
                    emoji=emoji,
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
                if session and close_after:
                    session.rollback()
                    session.close()

                raise e

            if close_after:
                session.commit()
                session.close()

    async def upsert_emoji(
        self,
        *,
        emoji_id: int | str,
        emoji_name: str | None = None,
        emoji_server_id: int | str | None = None,
        emoji_animated: bool | None = None,
        image_hash: int | str,
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
        return await sql_upsert(
            DBEmoji,
            {
                "id": str(emoji_id),
                "name": emoji_name,
                "server_id": str(emoji_server_id),
                "animated": not not emoji_animated,
                "image_hash": str(image_hash),
                "accessible": not not accessible,
            },
            {
                "name": emoji_name,
                "server_id": str(emoji_server_id),
                "image_hash": str(image_hash),
                "accessible": not not accessible,
            },
        )

    def delete_emoji(self, emoji_id: int):
        """Delete an emoji from the hash map.

        #### Args:
            - `emoji_id`: The ID of the emoji to delete.
        """
        validate_types({"emoji_id": (emoji_id, int)})

        if not self.emoji_to_hash.get(emoji_id):
            return

        image_hash = self.emoji_to_hash[emoji_id]
        del self.emoji_to_hash[emoji_id]

        if (
            self.hash_to_available_emoji.get(image_hash)
            and emoji_id in self.hash_to_available_emoji[image_hash]
        ):
            self.hash_to_available_emoji[image_hash].remove(emoji_id)

        if self.hash_to_internal_emoji.get(image_hash):
            del self.hash_to_internal_emoji[image_hash]

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
            self.delete_emoji(emoji.id)

            image = await globals.get_image_from_URL(emoji.url)
            image_hash = hash(image)
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
    def get_available_matches(
        self,
        emoji: discord.PartialEmoji | int | str,
    ) -> frozenset[int] | None:
        ...

    @overload
    def get_available_matches(
        self, emoji: discord.PartialEmoji | int | str, *, return_str: Literal[False]
    ) -> frozenset[int] | None:
        ...

    @overload
    def get_available_matches(
        self, emoji: discord.PartialEmoji | int | str, *, return_str: Literal[True]
    ) -> frozenset[str] | None:
        ...

    def get_available_matches(
        self,
        emoji: discord.PartialEmoji | int | str,
        *,
        return_str: bool | None = False,
    ) -> frozenset[int] | frozenset[str] | None:
        """Return a frozenset with the emoji IDs of emoji available to the bot that match the emoji passed as argument.

        #### Args:
            - `emoji`: The emoji to find matches for or ID of same.
            - `return_str`: If set to `True` will return a frozenset of stringified IDs. Defaults to False.
        """
        validate_types({"emoji": (emoji, (discord.PartialEmoji, int))})

        if isinstance(emoji, discord.PartialEmoji):
            if not emoji.id:
                return None
            emoji_id = emoji.id
        else:
            try:
                emoji_id = int(emoji)
            except ValueError:
                return None

        if not self.emoji_to_hash.get(emoji_id):
            return None

        image_hash = self.emoji_to_hash[emoji_id]
        if not self.hash_to_available_emoji.get(image_hash):
            return None

        if not return_str:
            return cast(
                frozenset[int], frozenset(self.hash_to_available_emoji[image_hash])
            )

        return frozenset({str(id) for id in self.hash_to_available_emoji[image_hash]})

    def get_internal_equivalent(self, emoji_id: int) -> int | None:
        """Return the ID of an internal emoji matching the one passed, if available.

        #### Args:
            - `emoji_id`: The ID of the emoji to check.
        """
        validate_types({"emoji_id": (emoji_id, int)})

        if not self.emoji_to_hash.get(emoji_id):
            return None

        image_hash = self.emoji_to_hash[emoji_id]
        if not self.hash_to_internal_emoji.get(image_hash):
            return None

        return self.hash_to_internal_emoji[image_hash]

    def get_accessible_emoji(
        self, emoji_id: int, *, skip_self: bool = False
    ) -> discord.Emoji | None:
        """Return an emoji matching the ID passed. First tries to return the one matching the ID itself, then an internal equivalent, and finally any accessible ones.

        #### Args:
            - `emoji_id`: The ID of the emoji to get.
            - `skip_self`: Whether the function should ignore the attempt to get an emoji associated with the ID itself. Defaults to False.
        """
        validate_types({"emoji_id": (emoji_id, int)})

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
            (matching_emoji_ids := self.get_available_matches(emoji_id))
            and (matching_emoji_id := set(matching_emoji_ids).pop())
            and (emoji := globals.client.get_emoji(matching_emoji_id))
        ):
            return emoji

        return None

    async def ensure_hash_map(
        self,
        *,
        emoji: discord.Emoji | discord.PartialEmoji | None = None,
        emoji_id: int | None = None,
        emoji_name: str | None = None,
        session: SQLSession | None = None,
    ):
        """Check that the emoji is in the hash map and, if not, add it to the map and to the database.

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
        if session:
            validate_types({"session": (session, SQLSession)})

        emoji_id, emoji_name, _, emoji_url = globals.get_emoji_information(
            emoji, emoji_id, emoji_name
        )

        if self.emoji_to_hash.get(emoji_id):
            return

        try:
            image = await globals.get_image_from_URL(emoji_url)
            image_hash = hash(image)
            self._add_emoji_to_map(emoji_id, image_hash)
        except Exception:
            pass


map: EmojiHashMap
