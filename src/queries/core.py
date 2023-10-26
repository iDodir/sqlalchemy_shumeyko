from sqlalchemy import text, insert

from src.database import sync_engine, async_engine
from src.models import metadata, worker_table


def get_123_sync():
    with sync_engine.connect() as conn:
        res = conn.execute(text("SELECT 1, 2, 3 UNION SELECT 4, 5, 6"))
        print(f"{res.first()=}")


async def get_123_async():
    async with async_engine.connect() as conn:
        res = await conn.execute(text("SELECT 1, 2, 3 UNION SELECT 4, 5, 6"))
        print(f"{res.first()=}")


def create_tables():
    sync_engine.echo = False
    metadata.drop_all(sync_engine)
    metadata.create_all(sync_engine)
    sync_engine.echo = True


def insert_data():
    with sync_engine.connect() as conn:
        # stmt = "INSERT INTO worker (username) VALUES ('Jack'), ('Michael');"
        # conn.execute(text(stmt))
        stmt = insert(worker_table).values(
            [
                {"username": "Jack"},
                {"username": "Michael"},
            ]
        )
        conn.execute(stmt)
        conn.commit()
