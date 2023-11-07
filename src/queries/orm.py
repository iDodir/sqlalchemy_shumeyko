from sqlalchemy import select, func, cast, Integer, and_, insert
from sqlalchemy.orm import aliased

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

    @staticmethod
    def insert_additional_resumes():
        with sync_session_factory() as session:
            workers = [
                {"username": "Artem"},
                {"username": "Roman"},
                {"username": "Petr"},
            ]
            resumes = [
                {"title": "Python программист", "compensation": 60000, "workload": "fulltime", "worker_id": 3},
                {"title": "Machine Learning Engineer", "compensation": 70000, "workload": "parttime", "worker_id": 3},
                {"title": "Python Data Scientist", "compensation": 80000, "workload": "parttime", "worker_id": 4},
                {"title": "Python Analyst", "compensation": 90000, "workload": "fulltime", "worker_id": 4},
                {"title": "Python Junior Developer", "compensation": 100000, "workload": "fulltime", "worker_id": 5},
            ]
            insert_workers = insert(WorkerOrm).values(workers)
            insert_resumes = insert(ResumeOrm).values(resumes)
            session.execute(insert_workers)
            session.execute(insert_resumes)
            session.commit()

    @staticmethod
    def join_cte_subquery_window_func():
        with sync_session_factory() as session:
            r = aliased(ResumeOrm)
            w = aliased(WorkerOrm)
            subq = (
                select(
                    r,
                    w,
                    (
                        func.avg(r.compensation)
                        .over(partition_by=r.workload)
                        .cast(Integer)
                        .label("avg_workload_compensation")
                    ),
                )
                # .select_from(r)
                .join(r, r.worker_id == w.id).subquery("helper1")
            )
            cte = (
                select(
                    subq.c.worker_id,
                    subq.c.username,
                    subq.c.compensation,
                    subq.c.workload,
                    subq.c.avg_workload_compensation,
                    (subq.c.compensation - subq.c.avg_workload_compensation).label("compensation_diff"),
                )
                .cte("helper2")
            )
            query = (
                select(cte)
                .order_by(cte.c.compensation_diff.desc())
            )

            res = session.execute(query)
            result = res.all()
            print(f"{len(result)=}. {result=}")


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
            await session.flush()
            await session.commit()

    @staticmethod
    async def select_workers():
        async with async_session_factory() as session:
            query = select(WorkerOrm)
            result = await session.execute(query)
            workers = result.scalars().all()
            print(f"{workers=}")

    @staticmethod
    async def update_worker(worker_id: int = 2, new_username: str = "Misha"):
        async with async_session_factory() as session:
            worker_michael = await session.get(WorkerOrm, worker_id)
            worker_michael.username = new_username
            await session.refresh(worker_michael)
            await session.commit()

    @staticmethod
    async def insert_resumes():
        async with async_session_factory() as session:
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
                [resume_jack_1, resume_jack_2, resume_michael_1, resume_michael_2],
            )
            await session.commit()

    @staticmethod
    async def select_resumes_avg_compensation(like_language: str = "Python"):
        async with async_session_factory() as session:
            query = (
                select(
                    ResumeOrm.workload,
                    # cast(func.avg(ResumeOrm.compensation), Integer).label("avg_compensation"),
                    func.avg(ResumeOrm.compensation).cast(Integer).label("avg_compensation"),
                )
                .select_from(ResumeOrm)
                .filter(and_(
                    ResumeOrm.title.contains(like_language),
                    ResumeOrm.compensation > 40000,
                ))
                .group_by(ResumeOrm.workload)
                .having(func.avg(ResumeOrm.compensation) > 70000)
            )
            print(query.compile(compile_kwargs={"literal_binds": True}))
            res = await session.execute(query)
            result = res.all()
            print(result)
            print(result[0].avg_compensation)

    @staticmethod
    async def insert_additional_resumes():
        async with async_session_factory() as session:
            workers = [
                {"username": "Artem"},  # id 3
                {"username": "Roman"},  # id 4
                {"username": "Petr"},  # id 5
            ]
            resumes = [
                {"title": "Python программист", "compensation": 60000, "workload": "fulltime", "worker_id": 3},
                {"title": "Machine Learning Engineer", "compensation": 70000, "workload": "parttime", "worker_id": 3},
                {"title": "Python Data Scientist", "compensation": 80000, "workload": "parttime", "worker_id": 4},
                {"title": "Python Analyst", "compensation": 90000, "workload": "fulltime", "worker_id": 4},
                {"title": "Python Junior Developer", "compensation": 100000, "workload": "fulltime", "worker_id": 5},
            ]
            insert_workers = insert(WorkerOrm).values(workers)
            insert_resumes = insert(ResumeOrm).values(resumes)
            await session.execute(insert_workers)
            await session.execute(insert_resumes)
            await session.commit()

    @staticmethod
    async def join_cte_subquery_window_func(like_language: str = "Python"):
        async with async_session_factory() as session:
            r = aliased(ResumeOrm)
            w = aliased(WorkerOrm)
            subq = (
                select(
                    r,
                    w,
                    (
                        func.avg(r.compensation)
                        .over(partition_by=r.workload)
                        .cast(Integer)
                        .label("avg_workload_compensation")
                    ),
                )
                # .select_from(r)
            ).join(r, r.worker_id == w.id).subquery("helper1")
            cte = (
                select(
                    subq.c.worker_id,
                    subq.c.username,
                    subq.c.compensation,
                    subq.c.workload,
                    subq.c.avg_workload_compensation,
                    (subq.c.compensation - subq.c.avg_workload_compensation).label("compensation_diff"),
                )
                .cte("helper2")
            )
            query = (
                select(cte)
                .order_by(cte.c.compensation_diff.desc())
            )

            res = await session.execute(query)
            result = res.all()

            print(f"{result=}")
            print(query.compile(compile_kwargs={"literal_binds": True}))
