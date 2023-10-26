from src.database import sync_engine, sync_session_factory, Base, async_session_factory
from src.models import WorkerOrm


def create_tables():
    sync_engine.echo = False
    Base.metadata.drop_all(sync_engine)
    Base.metadata.create_all(sync_engine)
    sync_engine.echo = True


def insert_data():
    with sync_session_factory() as session:
        worker_jack = WorkerOrm(username="Jack")
        worker_michael = WorkerOrm(username="Michael")
        session.add_all([worker_jack, worker_michael])
        session.commit()


async def insert_data_async():
    async with async_session_factory() as session:
        worker_jack = WorkerOrm(username="Jack")
        worker_michael = WorkerOrm(username="Michael")
        session.add_all([worker_jack, worker_michael])
        await session.commit()
