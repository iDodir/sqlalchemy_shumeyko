from sqlalchemy import select, func, cast, Integer, and_

from src.database import sync_engine, sync_session_factory, Base, async_session_factory, async_engine
from src.models import WorkerOrm, ResumeOrm, Workload


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

    @staticmethod
    def insert_resumes():
        with sync_session_factory() as session:
            resume_jack_1 = ResumeOrm(
                title="Python Junior Developer",
                compensation=50000,
                workload=Workload.fulltime,
                worker_id=1,
            )
            resume_jack_2 = ResumeOrm(
                title="Python Разработчик",
                compensation=150000,
                workload=Workload.fulltime,
                worker_id=1,
            )
            resume_michael_1 = ResumeOrm(
                title="Python Data Engineer",
                compensation=250000,
                workload=Workload.parttime,
                worker_id=2,
            )
            resume_michael_2 = ResumeOrm(
                title="Data Scientist",
                compensation=300000,
                workload=Workload.fulltime,
                worker_id=2,
            )
            session.add_all(
                [resume_jack_1, resume_jack_2, resume_michael_1, resume_michael_2]
            )
            session.commit()

    @staticmethod
    def select_resumes_avg_compensation(like_language: str = "Python"):
        with sync_session_factory() as session:
            query = (
                select(
                    ResumeOrm.workload,
                    cast(func.avg(ResumeOrm.compensation), Integer).label("avg_compensation"),
                )
                .select_from(ResumeOrm)
                .filter(and_(
                    ResumeOrm.title.contains(like_language),
                    ResumeOrm.compensation > 40_000,
                ))
                .group_by(ResumeOrm.workload)
                .having(cast(func.avg(ResumeOrm.compensation), Integer) > 70_000)
            )
            print(query.compile(compile_kwargs={"literal_binds": True}))
            res = session.execute(query)
            result = res.all()
            print(result)
            print(result[0].avg_compensation)


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
