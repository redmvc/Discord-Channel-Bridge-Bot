from typing import Any

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

from globals import credentials

engine = create_engine(
    f"{credentials['db_dialect']}+{credentials['db_driver']}://{credentials['db_user']}:{credentials['db_pwd']}@{credentials['db_host']}:{credentials['db_port']}/{credentials['db_name']}"
)


class DBBase(DeclarativeBase):
    pass


class DBBridge(DBBase):
    __tablename__ = "bridges"
    __table_args__ = (
        UniqueConstraint("source", "target", name="unique_source_target"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    target: Mapped[str] = mapped_column(String(32), nullable=False)
    webhook: Mapped[str] = mapped_column(String(32))


class DBMessageMap(DBBase):
    __tablename__ = "message_mappings"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source_message: Mapped[str] = mapped_column(String(32), nullable=False)
    source_channel: Mapped[str] = mapped_column(String(32), nullable=False)
    target_message: Mapped[str] = mapped_column(String(32), nullable=False)
    target_channel: Mapped[str] = mapped_column(String(32), nullable=False)


def sql_upsert(
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
        - `UnknownDBDialectError`: Invalid database dialect registered in `credentials` file.
        - `SQLError`: SQL statement inferred from arguments was invalid or database connection failed.

    #### Returns:
        - `Insert`: The updated Insert command.
    """
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

    session = None
    try:
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
            session = SQLSession(engine)
            index_values = [
                getattr(table, idx) == insert_values[idx] for idx in indices
            ]
            upsert: UpdateBase
            if session.execute(SQLSelect(table).where(*index_values)).first():
                # Values with those keys do exist, so I update
                upsert = SQLUpdate(table).where(*index_values).values(update_values)
            else:
                upsert = other_db_insert(table).values(insert_values)
            session.close()

            return upsert
    except SQLError as e:
        if session:
            session.close()
        raise e


DBBase.metadata.create_all(engine)
