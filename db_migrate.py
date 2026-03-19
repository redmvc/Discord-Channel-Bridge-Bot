"""Standalone script to migrate database contents between two contexts defined in settings.json.

Usage:
    python db_migrate.py --source production --target new_production
    python db_migrate.py --source production --target new_production --clean
"""

import argparse
import asyncio
import json
import sys

# Add src/ to path so we can import the ORM models
sys.path.insert(0, "src")

from sqlalchemy import sql, text
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from database import (
    DBAppWhitelist,
    DBAutoBridgeThreadChannels,
    DBBridge,
    DBEmoji,
    DBMessageMap,
    DBReactionMap,
    DBWebhook,
    create_tables,
)

# All tables to migrate, in a reasonable order
TABLES = [
    DBAppWhitelist,
    DBWebhook,
    DBEmoji,
    DBAutoBridgeThreadChannels,
    DBBridge,
    DBMessageMap,
    DBReactionMap,
]

BATCH_SIZE = 500


def build_connection_url(context_settings: dict) -> str:
    """Build a SQLAlchemy connection URL from a settings context."""
    return (
        f"{context_settings['db_dialect']}+{context_settings['db_driver']}://"
        f"{context_settings['db_user']}:{context_settings['db_pwd']}@"
        f"{context_settings['db_host']}:{context_settings['db_port']}/"
        f"{context_settings['db_name']}"
    )


def row_to_dict(row, model_class) -> dict:
    """Convert an ORM row to a dictionary of column values."""
    return {col.name: getattr(row, col.name) for col in model_class.__table__.columns}


def make_insert_ignore(model_class, dialect_name: str):
    """Return a dialect-appropriate INSERT IGNORE statement builder."""

    def build(values: list[dict]):
        if dialect_name == "mysql":
            return mysql.insert(model_class).values(values).prefix_with("IGNORE")
        elif dialect_name == "postgresql":
            return (
                postgresql.insert(model_class).values(values).on_conflict_do_nothing()
            )
        elif dialect_name == "sqlite":
            return sqlite.insert(model_class).values(values).on_conflict_do_nothing()
        else:
            # Fallback: plain insert (may fail on duplicates)
            return sql.insert(model_class).values(values)

    return build


async def migrate(source_context: str, target_context: str, clean: bool):
    """Run the migration from source to target database."""

    # Load settings
    with open("settings.json") as f:
        settings_root = json.load(f)

    if source_context not in settings_root:
        print(f"Error: context '{source_context}' not found in settings.json")
        print(f"Available contexts: {[k for k in settings_root if k != 'context']}")
        sys.exit(1)
    if target_context not in settings_root:
        print(f"Error: context '{target_context}' not found in settings.json")
        print(f"Available contexts: {[k for k in settings_root if k != 'context']}")
        sys.exit(1)

    source_settings = settings_root[source_context]
    target_settings = settings_root[target_context]

    source_url = build_connection_url(source_settings)
    target_url = build_connection_url(target_settings)

    print(f"Source: {source_settings['db_host']} / {source_settings['db_name']}")
    print(f"Target: {target_settings['db_host']} / {target_settings['db_name']}")
    print()

    # Create engines
    source_engine = create_async_engine(source_url, pool_pre_ping=True)
    target_engine = create_async_engine(target_url, pool_pre_ping=True)

    SourceSession = async_sessionmaker(
        source_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    TargetSession = async_sessionmaker(
        target_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    # Create/update tables on target
    print("Creating/updating tables on target database...")
    await create_tables(target_engine)
    print("Tables ready.")
    print()

    target_dialect = target_engine.dialect.name

    total_read = 0
    total_written = 0

    for model_class in TABLES:
        table_name = model_class.__tablename__

        # Read all rows from source
        async with SourceSession() as source_session:
            result = await source_session.execute(sql.select(model_class))
            rows = result.scalars().all()

        row_dicts = [row_to_dict(row, model_class) for row in rows]
        num_read = len(row_dicts)
        total_read += num_read

        if num_read == 0:
            print(f"  {table_name}: 0 rows (empty)")
            continue

        # Clean target table if requested
        if clean:
            async with TargetSession() as target_session:
                async with target_session.begin():
                    await target_session.execute(text(f"DELETE FROM {table_name}"))
            print(f"  {table_name}: cleaned")

        # Insert into target in batches
        insert_builder = make_insert_ignore(model_class, target_dialect)
        num_written = 0

        async with TargetSession() as target_session:
            for i in range(0, len(row_dicts), BATCH_SIZE):
                batch = row_dicts[i : i + BATCH_SIZE]
                async with target_session.begin():
                    stmt = insert_builder(batch)
                    result = await target_session.execute(stmt)
                    num_written += result.rowcount

        total_written += num_written
        status = (
            "" if num_written == num_read else f" ({num_read - num_written} skipped)"
        )
        print(f"  {table_name}: {num_read} read, {num_written} written{status}")

    print()
    print(f"Done! Total: {total_read} rows read, {total_written} rows written.")

    await source_engine.dispose()
    await target_engine.dispose()


def main():
    parser = argparse.ArgumentParser(
        description="Migrate database contents between settings.json contexts."
    )
    parser.add_argument(
        "--source",
        required=True,
        help="Source context name in settings.json (e.g. 'production')",
    )
    parser.add_argument(
        "--target",
        required=True,
        help="Target context name in settings.json (e.g. 'new_production')",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete all rows in target tables before inserting",
    )

    args = parser.parse_args()

    if args.source == args.target:
        print("Error: source and target contexts must be different.")
        sys.exit(1)

    asyncio.run(migrate(args.source, args.target, args.clean))


if __name__ == "__main__":
    main()
