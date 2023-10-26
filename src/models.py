from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy.orm import Mapped, mapped_column

from src.database import Base


class WorkerOrm(Base):
    __tablename__ = "worker"

    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str]


metadata = MetaData()

worker_table = Table(
    "worker",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("username", String),
)
