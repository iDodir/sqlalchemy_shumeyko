import datetime
import enum
from typing import Optional, Annotated

from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, text, Enum, DateTime
from sqlalchemy.orm import Mapped, mapped_column

from src.database import Base, str_256

intpk = Annotated[int, mapped_column(primary_key=True)]
created_at = Annotated[datetime.datetime, mapped_column(server_default=text("TIMEZONE('utc', now())"))]
updated_at = Annotated[
    datetime.datetime,
    mapped_column(
        server_default=text("TIMEZONE('utc', now())"),
        onupdate=datetime.datetime.utcnow,
    )
]


class WorkerOrm(Base):
    __tablename__ = "worker"

    id: Mapped[intpk]
    username: Mapped[str]


class Workload(enum.Enum):
    parttime = "parttime"
    fulltime = "fulltime"


class ResumeOrm(Base):
    __tablename__ = "resume"

    id: Mapped[intpk]
    title: Mapped[str_256]
    compensation: Mapped[Optional[int]]
    workload: Mapped[Workload]
    worker_id: Mapped[int] = mapped_column(ForeignKey("worker.id", ondelete="CASCADE"))
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]


metadata = MetaData()

worker_table = Table(
    "worker",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("username", String),
)

resume_table = Table(
    "resume",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("title", String(256)),
    Column("compensation", Integer, nullable=True),
    Column("workload", Enum(Workload)),
    Column("worker_id", Integer, ForeignKey("worker.id", ondelete="CASCADE")),
    Column("created_at", DateTime(timezone=True), server_default=text("TIMEZONE('utc', now())")),
    Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=text("TIMEZONE('utc', now())"),
        onupdate=datetime.datetime.utcnow
    ),
)
