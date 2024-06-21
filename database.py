from sqlalchemy import String, UniqueConstraint, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

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
