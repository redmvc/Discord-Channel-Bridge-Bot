from typing import Any, Callable, Iterable

from sqlalchemy import Boolean
from sqlalchemy import Select as SQLSelect
from sqlalchemy import String, UniqueConstraint
from sqlalchemy import Update as SQLUpdate
from sqlalchemy import UpdateBase, create_engine
from sqlalchemy import insert as other_db_insert
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.exc import StatementError as SQLError
from sqlalchemy.orm import DeclarativeBase, Mapped
from sqlalchemy.orm import Session as SQLSession
from sqlalchemy.orm import mapped_column

from globals import T, run_retries, settings
from validations import beartype


class DBBase(DeclarativeBase):
    """
    This class serves as a base for all tables used by the bot. It should not be referenced directly, as its only purpose is running the create_all() command at the end of this file.
    """

    pass


class DBBridge(DBBase):
    """
    An SQLAlchemy ORM class representing a database table tracking existing bridges between channels and/or threads.

    #### Columns
    - `id (INT)`: The id number of a bridge, has `PRIMARY KEY` and `AUTO_INCREMENT`.
    - `source (VARCHAR(32))`: The ID of the bridge's source channel or thread.
    - `target (VARCHAR(32))`: The ID of the bridge's target channel or thread.

    #### Constraints
    - `unique_source_target (UNIQUE(source, target))`: A combination of source and target channel or thread IDs has to be unique.
    """

    __tablename__ = "bridges"
    __table_args__ = (
        UniqueConstraint("source", "target", name="unique_source_target"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    target: Mapped[str] = mapped_column(String(32), nullable=False)


class DBWebhook(DBBase):
    """
    An SQLAlchemy ORM class representing a database table tracking webhooks managed by the bot in each channel.

    #### Columns
    - `channel (VARCHAR(32))`: The ID of the bridge's target channel or thread. Has `PRIMARY KEY`.
    - `webhook (VARCHAR(32))`: The ID of the webhook attached to the target channel which bridges messages to it.
    """

    __tablename__ = "webhooks"

    channel: Mapped[str] = mapped_column(String(32), primary_key=True)
    webhook: Mapped[str] = mapped_column(String(32))


class DBMessageMap(DBBase):
    """
    An SQLAlchemy ORM class representing a database table listing the mappings between bridged messages.

    #### Columns
    - `id (INT)`: The id number of a mapping, has `PRIMARY KEY` and `AUTO_INCREMENT`.
    - `source_message (VARCHAR(32))`: The ID of the message in the original channel.
    - `source_channel (VARCHAR(32))`: The ID of the channel or thread that message was sent to.
    - `target_message (VARCHAR(32))`: The ID of the message generated by the bot across a bridge.
    - `target_channel (VARCHAR(32))`: The ID of the channel or thread the bridged message was bridged to.
    - `webhook (VARCHAR(32))`: The ID of the webhook that posted the message.
    """

    __tablename__ = "message_mappings"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source_message: Mapped[str] = mapped_column(String(32), nullable=False)
    source_channel: Mapped[str] = mapped_column(String(32), nullable=False)
    target_message: Mapped[str] = mapped_column(String(32), nullable=False)
    target_channel: Mapped[str] = mapped_column(String(32), nullable=False)
    webhook: Mapped[str] = mapped_column(String(32), nullable=True)


class DBReactionMap(DBBase):
    """
    An SQLAlchemy ORM class representing a database table listing the mappings between bridged reactions.

    #### Columns
    - `id (INT)`: The id number of a mapping, has `PRIMARY KEY` and `AUTO_INCREMENT`.
    - `source_emoji (VARCHAR(30))`: The ID of the emoji in the source message.
    - `source_message (VARCHAR(32))`: The ID of the message in the original channel.
    - `source_channel (VARCHAR(32))`: The ID of the channel or thread that message was sent to.
    - `target_message (VARCHAR(32))`: The ID of the message that got this reaction bridged to it.
    - `target_channel (VARCHAR(32))`: The ID of the channel or thread that message is in.
    - `target_emoji_id (VARCHAR(32))`: The ID of the emoji in the target message.
    - `target_emoji_name (VARCHAR(32))`: The name of the emoji in the target message.

    #### Constraints
    - `unique_emoji_source_target (UNIQUE(source_emoji, source_message, target_message))`: A combination of source emoji, source message, and target message has to be unique.
    """

    __tablename__ = "reaction_mappings"
    __table_args__ = (
        UniqueConstraint(
            "source_emoji",
            "source_message",
            "target_message",
            name="unique_emoji_source_target",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source_emoji: Mapped[str] = mapped_column(String(32), nullable=False)
    source_message: Mapped[str] = mapped_column(String(32), nullable=False)
    source_channel: Mapped[str] = mapped_column(String(32), nullable=False)
    target_message: Mapped[str] = mapped_column(String(32), nullable=False)
    target_channel: Mapped[str] = mapped_column(String(32), nullable=False)
    target_emoji_id: Mapped[str] = mapped_column(String(32), nullable=True)
    target_emoji_name: Mapped[str] = mapped_column(String(32), nullable=True)


class DBAutoBridgeThreadChannels(DBBase):
    """
    An SQLAlchemy ORM class representing a database table listing all channels that will automatically bridge newly-created threads.

    #### Columns
    - `id (INT)`: The id number of an entry, has `PRIMARY KEY` and `AUTO_INCREMENT`.
    - `channel (VARCHAR(32))`: The ID of the channel that has auto-bridge-threads enabled.
    """

    __tablename__ = "auto_bridge_thread_channels"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    channel: Mapped[str] = mapped_column(String(32), nullable=False)


class DBEmoji(DBBase):
    """
    An SQLAlchemy ORM class representing a database table storing information about emoji.

    #### Columns
    - `id (VARCHAR(32))`: The emoji ID, has `PRIMARY KEY`.
    - `name (VARCHAR(32))`: The name of the emoji.
    - `server_id (VARCHAR(32))`: The ID of the server this emoji belongs to.
    - `animated (BOOL)`: Whether it's an animated emoji.
    - `image_hash (VARCHAR(32))`: A hash of this emoji's image.
    - `accessible (BOOL)`: Whether the bot has access to this emoji.
    """

    __tablename__ = "emoji"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    name: Mapped[str] = mapped_column(String(32), nullable=True)
    server_id: Mapped[str] = mapped_column(String(32), nullable=True)
    animated: Mapped[bool] = mapped_column(Boolean, nullable=True)
    image_hash: Mapped[str] = mapped_column(String(32), nullable=True)
    accessible: Mapped[bool] = mapped_column(Boolean, nullable=False)


class DBAppWhitelist(DBBase):
    """
    An SQLAlchemy ORM class representing a database table storing IDs for applications/bots that are whitelisted for bridging messages in a given channel.

    #### Columns
    - `id (INT)`: The id number of an entry, has `PRIMARY KEY` and `AUTO_INCREMENT`.
    - `channel (VARCHAR(32))`: The ID of a source channel.
    - `application (VARCHAR(32))`: The ID of the application whose messages should be allowed through from that channel.

    #### Constraints
    - `unique_channel_app (UNIQUE(channel, application))`: A combination of channel and application IDs has to be unique.
    """

    __tablename__ = "app_whitelist"
    __table_args__ = (
        UniqueConstraint("channel", "application", name="unique_channel_app"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    channel: Mapped[str] = mapped_column(String(32), nullable=False)
    application: Mapped[str] = mapped_column(String(32), nullable=False)


@beartype
async def sql_upsert(
    *,
    table: Any,
    indices: Iterable[str],
    ignored_cols: Iterable[str] | None = None,
    **kwargs: Any,
) -> UpdateBase:
    """Return an `UpdateBase` for inserting values into a table if a set of indices is not duplicated or updating them if it is.

    #### Args:
        - `table`: The table to insert into.
        - `indices`: A list with the names of the indices (i.e. the columns whose uniqueness will be checked).
        - `ignored_cols`: A list with the names of columns whose values should not be updated but which aren't, themselves, unique indices.
        - `kwargs`: Named arguments for the values to insert or update. The values in `indices` must be a [proper subset](https://en.wikipedia.org/wiki/Subset) of the keys in `kwargs` (i.e. all `indices` should be in `kwargs.keys()` but there should be at least one key in `kwargs` that isn't in `indices`).

    #### Raises:
        - `ValueError`: `indices` is not a proper subset of `kwargs.keys()`.
        - `SQLError`: SQL statement inferred from arguments was invalid or database connection failed. This error can only be raised if the database dialect is not MySQL, PostgreSQL, nor SQLite.
    """
    indices = set(indices)
    insert_values = kwargs
    insert_value_keys = set(insert_values.keys())
    if not indices < insert_value_keys:
        raise ValueError("keys is not a proper subset of kwargs.keys().")

    if ignored_cols:
        ignored_cols = set(ignored_cols)
    else:
        ignored_cols = {}
    update_values = {
        key: value
        for key, value in insert_values.items()
        if key not in indices.union(ignored_cols)
    }

    db_dialect = engine.dialect.name
    if db_dialect == "mysql":
        return (
            mysql.insert(table)
            .values(**insert_values)
            .on_duplicate_key_update(**update_values)
        )
    elif db_dialect in {"postgresql", "sqlite"}:
        insert: postgresql.Insert | sqlite.Insert
        if db_dialect == "postgresql":
            insert = postgresql.insert(table)
        else:
            insert = sqlite.insert(table)

        return insert.values(**insert_values).on_conflict_do_update(
            index_elements=indices, set_=update_values
        )
    else:
        # I'll do a manual update in this case
        session = None
        try:
            with SQLSession(engine) as session:
                index_values = [
                    getattr(table, idx) == insert_values[idx] for idx in indices
                ]
                upsert: UpdateBase

                def select_existing(session: SQLSession):
                    select_table: SQLSelect[tuple[Any]] = SQLSelect(table).where(
                        *index_values
                    )
                    return session.execute(select_table).first()

                if await sql_retry(lambda: select_existing(session)):
                    # Values with those keys do exist, so I update
                    upsert = (
                        SQLUpdate(table).where(*index_values).values(**update_values)
                    )
                else:
                    upsert = other_db_insert(table).values(**insert_values)

            return upsert
        except Exception:
            if session:
                session.rollback()
                session.close()
            raise


@beartype
async def sql_insert_ignore_duplicate(
    *,
    table: Any,
    indices: Iterable[str],
    **kwargs: Any,
) -> UpdateBase:
    """Return an `UpdateBase` for inserting values into a table if a set of indices is not duplicated.

    #### Args:
        - `table`: The table to insert into.
        - `indices`: A list with the names of the indices (i.e. the columns whose uniqueness will be checked).
        - `kwargs`: Named arguments for the values to insert or update. The values in `indices` must be a [proper subset](https://en.wikipedia.org/wiki/Subset) of the keys in `kwargs` (i.e. all `indices` should be in `kwargs.keys()` but there should be at least one key in `kwargs` that isn't in `indices`).

    #### Raises:
        - `SQLError`: SQL statement inferred from arguments was invalid or database connection failed. This error can only be raised if the database dialect is not MySQL, PostgreSQL, nor SQLite.
    """
    indices = set(indices)
    insert_values = kwargs

    db_dialect = engine.dialect.name
    if db_dialect == "mysql":
        random_index = indices.pop()
        return (
            mysql.insert(table)
            .values(**insert_values)
            .on_duplicate_key_update(**{random_index: getattr(table, random_index)})
        )
    elif db_dialect in {"postgresql", "sqlite"}:
        insert: postgresql.Insert | sqlite.Insert
        if db_dialect == "postgresql":
            insert = postgresql.insert(table)
        else:
            insert = sqlite.insert(table)

        return insert.values(**insert_values).on_conflict_do_nothing()
    else:
        # I'll do a manual update in this case
        session = None
        try:
            with SQLSession(engine) as session:
                index_values = [
                    getattr(table, idx) == insert_values[idx] for idx in indices
                ]
                insert_unknown: UpdateBase

                def select_existing(session: SQLSession):
                    select_table: SQLSelect[tuple[Any]] = SQLSelect(table).where(
                        *index_values
                    )
                    return session.execute(select_table).first()

                if await sql_retry(lambda: select_existing(session)):
                    # Values with those keys do exist, so I do nothing
                    random_index = indices.pop()
                    insert_unknown = SQLUpdate(table).values(
                        **{random_index: getattr(table, random_index)}
                    )
                else:
                    insert_unknown = other_db_insert(table).values(**insert_values)

            return insert_unknown
        except Exception:
            if session:
                session.rollback()
                session.close()
            raise


@beartype
async def sql_retry(
    fun: Callable[..., T],
    num_retries: int = 5,
    time_to_wait: float | int = 10,
) -> T:
    """Run an SQL function and retry it every time an SQLError occurs up to a certain maximum number of tries. If it succeeds, return its result; otherwise, raise the error.

    #### Args:
        - `fun`: The function to run.
        - `num_retries`: The number of times to try the function again.
        - `time_to_wait`: How long to wait between retries.

    #### Returns:
        - `T`: The result of calling `fun()`.
    """
    return await run_retries(fun, num_retries, time_to_wait, SQLError)


# Create the engine connecting to the database
engine = create_engine(
    f"{settings['db_dialect']}+{settings['db_driver']}://{settings['db_user']}:{settings['db_pwd']}@{settings['db_host']}:{settings['db_port']}/{settings['db_name']}",
    pool_pre_ping=True,
    pool_recycle=3600,
)

# Create all tables represented by the above classes, if they haven't already been created
DBBase.metadata.create_all(engine)
