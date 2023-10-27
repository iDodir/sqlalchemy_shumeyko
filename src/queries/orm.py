from sqlalchemy import select

from src.database import sync_engine, sync_session_factory, Base, async_session_factory, async_engine
from src.models import WorkerOrm


class SyncORM:
    @staticmethod
    def create_tables():
        async_engine.echo = False
        Base.metadata.drop_all(sync_engine)
        Base.metadata.create_all(sync_engine)
        async_engine.echo = True

    @staticmethod
    def insert_workers():
        with sync_session_factory() as session:
            worker_jack = WorkerOrm(username="Jack")
            worker_michael = WorkerOrm(username="Michael")
            session.add_all([worker_jack, worker_michael])
            # session.flush()
            session.commit()

    @staticmethod
    def select_workers():
        with sync_session_factory() as session:
            # worker_id = 1
            # worker_jack = session.get(WorkerOrm, worker_id)
            query = select(WorkerOrm)
            result = session.execute(query)
            workers = result.all()
            print(f"{workers=}")

    @staticmethod
    def update_worker(worker_id: int = 2, new_username: str = "Misha"):
        with sync_session_factory() as session:
            worker_michael = session.get(WorkerOrm, worker_id)
            worker_michael.username = new_username
            # session.expire_all()
            session.refresh(worker_michael)
            session.commit()


class AsyncORM:
    @staticmethod
    async def create_tables():
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)

    @staticmethod
    async def insert_workers():
        async with async_session_factory() as session:
            worker_jack = WorkerOrm(username="Jack")
            worker_michael = WorkerOrm(username="Michael")
            session.add_all([worker_jack, worker_michael])
            await session.commit()
