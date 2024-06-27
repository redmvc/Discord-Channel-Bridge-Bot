from typing import Any, Callable, cast

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
from sqlalchemy.sql._typing import _DMLTableArgument

from globals import _T, run_retries, settings
from validations import validate_types


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
    - `webhook (VARCHAR(32))`: The ID of the webhook attached to the target channel which bridges messages to it.

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
    """

    __tablename__ = "message_mappings"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source_message: Mapped[str] = mapped_column(String(32), nullable=False)
    source_channel: Mapped[str] = mapped_column(String(32), nullable=False)
    target_message: Mapped[str] = mapped_column(String(32), nullable=False)
    target_channel: Mapped[str] = mapped_column(String(32), nullable=False)


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


class DBEmojiMap(DBBase):
    """
    An SQLAlchemy ORM class representing a database table matching external emoji we couldn't find with emoji we have stored in our emoji server.

    #### Columns
    - `external_emoji (VARCHAR(32))`: The ID of the external emoji which had been missing. Has `PRIMARY KEY`.
    - `external_emoji_name (VARCHAR(32))`: The name of the external emoji on its original server.
    - `internal_emoji (VARCHAR(32))`: The ID of the internal emoji that we created to match it.
    """

    __tablename__ = "emoji_mapping"

    external_emoji: Mapped[str] = mapped_column(String(32), primary_key=True)
    external_emoji_name: Mapped[str] = mapped_column(String(32))
    internal_emoji: Mapped[str] = mapped_column(String(32), nullable=False)


async def sql_upsert(
    table: _DMLTableArgument,
    insert_values: dict[str, Any],
    update_values: dict[str, Any],
) -> UpdateBase:
    """Insert values into a table if a key is not duplicated or update them if it is.

    #### Args:
        - `table`: The table to insert into.
        - `insert_values`: A dictionary whose keys are the names of columns in the table being inserted into and whose values are the values to insert. Must include at least one unique key as well as all keys in `update_values`.
        - `update_values`: A dictionary whose keys are the names of columns in the table being inserted into and whose values are the values to update on duplicate keys. At least one unique key present in `insert_values` must be absent from this dictionary.

    ### Raises:
        - `ValueError`: `insert_values` does not have any keys not present in `update_values`.
        - `UnknownDBDialectError`: Invalid database dialect registered in `settings.json` file.
        - `SQLError`: SQL statement inferred from arguments was invalid or database connection failed.

    #### Returns:
        - `Insert`: The updated Insert command.
    """
    validate_types(
        {
            "insert_values": (insert_values, dict),
            "update_values": (update_values, dict),
        }
    )

    insert_keys = set(insert_values.keys())
    update_keys = set(update_values.keys())
    if not update_keys.issubset(insert_keys):
        raise ValueError(
            "At least one key present in update_keys is not present in insert_keys."
        )

    indices = list(insert_keys - update_keys)
    if len(indices) == 0:
        raise ValueError(
            "insert_values must have at least one key not present in update_values."
        )

    db_dialect = engine.dialect.name
    if db_dialect == "mysql":
        return (
            mysql.insert(table)
            .values(insert_values)
            .on_duplicate_key_update(update_values)
        )
    elif db_dialect in {"postgresql", "sqlite"}:
        insert: postgresql.Insert | sqlite.Insert
        if db_dialect == "postgresql":
            insert = postgresql.insert(table)
        else:
            insert = sqlite.insert(table)

        return insert.values(insert_values).on_conflict_do_update(
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

                def select_existing():
                    return (
                        cast(SQLSession, session)
                        .execute(SQLSelect(table).where(*index_values))
                        .first()
                    )

                if await sql_retry(select_existing):
                    # Values with those keys do exist, so I update
                    upsert = SQLUpdate(table).where(*index_values).values(update_values)
                else:
                    upsert = other_db_insert(table).values(insert_values)

            return upsert
        except SQLError as e:
            if session:
                session.close()
            raise e


async def sql_retry(
    fun: Callable[..., _T],
    num_retries: int = 3,
    time_to_wait: float = 5,
) -> _T:
    """Run an SQL function and retry it every time an SQLError occurs up to a certain maximum number of tries. If it succeeds, return its result; otherwise, raise the error.

    #### Args:
        - `fun`: The function to run.
        - `num_retries`: The number of times to try the function again.
        - `time_to_wait`: How long to wait between retries.

    #### Returns:
        - `_T`: The result of calling `fun()`.
    """
    return await run_retries(fun, num_retries, time_to_wait, SQLError)


# Create the engine connecting to the database
engine = create_engine(
    f"{settings['db_dialect']}+{settings['db_driver']}://{settings['db_user']}:{settings['db_pwd']}@{settings['db_host']}:{settings['db_port']}/{settings['db_name']}"
)

# Create all tables represented by the above classes, if they haven't already been created
DBBase.metadata.create_all(engine)
