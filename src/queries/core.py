from sqlalchemy import insert, cast, func, Integer, and_
from sqlalchemy import select, text, update
from sqlalchemy.orm import aliased

from src.database import sync_engine, async_engine
from src.models import metadata, resume_table, Workload, ResumeOrm
from src.models import worker_table


def get_123_sync():
    with sync_engine.connect() as conn:
        res = conn.execute(text("SELECT 1, 2, 3 UNION SELECT 4, 5, 6"))
        print(f"{res.first()=}")


async def get_123_async():
    async with async_engine.connect() as conn:
        res = await conn.execute(text("SELECT 1, 2, 3 UNION SELECT 4, 5, 6"))
        print(f"{res.first()=}")


class SyncCore:
    @staticmethod
    def create_tables():
        sync_engine.echo = False
        metadata.drop_all(sync_engine)
        metadata.create_all(sync_engine)
        sync_engine.echo = True

    @staticmethod
    def insert_workers():
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

    @staticmethod
    def select_workers():
        with sync_engine.connect() as conn:
            query = select(worker_table)
            result = conn.execute(query)
            workers = result.all()
            print(f"{workers=}")

    @staticmethod
    def update_worker(worker_id: int = 2, new_username: str = "Misha"):
        with sync_engine.connect() as conn:
            # stmt = text("UPDATE worker SET username=:username WHERE id=:id")
            # stmt = stmt.bindparams(username=new_username, id=worker_id)
            stmt = (
                update(worker_table)
                .values(username=new_username)
                # .where(worker_table.c.id == worker_id)
                .filter_by(id=worker_id)
            )
            conn.execute(stmt)
            conn.commit()

    @staticmethod
    def insert_resumes():
        with sync_engine.connect() as conn:
            resumes = [
                {
                    "title": "Python Junior Developer",
                    "compensation": 50000,
                    "workload": Workload.fulltime,
                    "worker_id": 1,
                },
                {
                    "title": "Python Разработчик",
                    "compensation": 150000,
                    "workload": Workload.fulltime,
                    "worker_id": 1,
                },
                {
                    "title": "Python Data Engineer",
                    "compensation": 250000,
                    "workload": Workload.parttime,
                    "worker_id": 2,
                },
                {
                    "title": "Data Scientist",
                    "compensation": 300000,
                    "workload": Workload.fulltime,
                    "worker_id": 2,
                },
            ]
            stmt = insert(resume_table).values(resumes)
            conn.execute(stmt)
            conn.commit()

    @staticmethod
    def select_resumes_avg_compensation(like_language: str = "Python"):
        with sync_engine.connect() as conn:
            query = (
                select(
                    resume_table.c.workload,
                    # cast(func.avg(ResumeOrm.compensation), Integer).label("avg_compensation"),
                    func.avg(resume_table.c.compensation).cast(Integer).label("avg_compensation"),
                )
                .select_from(resume_table)
                .filter(and_(
                    resume_table.c.title.contains(like_language),
                    resume_table.c.compensation > 40000,
                ))
                .group_by(resume_table.c.workload)
                .having(func.avg(resume_table.c.compensation) > 70000)
            )
            print(query.compile(compile_kwargs={"literal_binds": True}))
            res = conn.execute(query)
            result = res.all()
            print(result)
            print(result[0].avg_compensation)

    @staticmethod
    def insert_additional_resumes():
        with sync_engine.connect() as conn:
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
            insert_workers = insert(worker_table).values(workers)
            insert_resumes = insert(resume_table).values(resumes)
            conn.execute(insert_workers)
            conn.execute(insert_resumes)
            conn.commit()

    @staticmethod
    def join_cte_subquery_window_func():
        with sync_engine.connect() as conn:
            r = aliased(resume_table)
            w = aliased(worker_table)
            subq = (
                select(
                    r,
                    w,
                    (
                        func.avg(r.c.compensation)
                        .over(partition_by=r.c.workload)
                        .cast(Integer)
                        .label("avg_workload_compensation")
                    ),
                )
                # .select_from(r)
                .join(r, r.c.worker_id == w.c.id).subquery("helper1")
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

            res = conn.execute(query)
            result = res.all()
            print(f"{len(result)=}. {result=}")


class AsyncCore:
    @staticmethod
    async def create_tables():
        async with async_engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)
            await conn.run_sync(metadata.create_all)

    @staticmethod
    async def insert_workers():
        async with async_engine.connect() as conn:
            # stmt = """INSERT INTO workers (username) VALUES
            #     ('Jack'),
            #     ('Michael');"""
            stmt = insert(worker_table).values(
                [
                    {"username": "Jack"},
                    {"username": "Michael"},
                ]
            )
            await conn.execute(stmt)
            await conn.commit()

    @staticmethod
    async def select_workers():
        async with async_engine.connect() as conn:
            query = select(worker_table)
            result = await conn.execute(query)
            workers = result.all()
            print(f"{workers=}")

    @staticmethod
    async def update_worker(worker_id: int = 2, new_username: str = "Misha"):
        async with async_engine.connect() as conn:
            # stmt = text("UPDATE workers SET username=:username WHERE id=:id")
            # stmt = stmt.bindparams(username=new_username, id=worker_id)
            stmt = (
                update(worker_table)
                .values(username=new_username)
                # .where(worker_table.c.id==worker_id)
                .filter_by(id=worker_id)
            )
            await conn.execute(stmt)
            await conn.commit()

    @staticmethod
    async def insert_resumes():
        async with async_engine.connect() as conn:
            resumes = [
                {
                    "title": "Python Junior Developer",
                    "compensation": 50000,
                    "workload": Workload.fulltime,
                    "worker_id": 1,
                },
                {
                    "title": "Python Разработчик",
                    "compensation": 150000,
                    "workload": Workload.fulltime,
                    "worker_id": 1,
                },
                {
                    "title": "Python Data Engineer",
                    "compensation": 250000,
                    "workload": Workload.parttime,
                    "worker_id": 2,
                },
                {
                    "title": "Data Scientist",
                    "compensation": 300000,
                    "workload": Workload.fulltime,
                    "worker_id": 2,
                },
            ]
            stmt = insert(resume_table).values(resumes)
            await conn.execute(stmt)
            await conn.commit()

    @staticmethod
    async def select_resumes_avg_compensation(like_language: str = "Python"):
        async with async_engine.connect() as conn:
            query = (
                select(
                    resume_table.c.workload,
                    # cast(func.avg(ResumeOrm.compensation), Integer).label("avg_compensation"),
                    func.avg(resume_table.c.compensation).cast(Integer).label("avg_compensation"),
                )
                .select_from(resume_table)
                .filter(and_(
                    resume_table.c.title.contains(like_language),
                    resume_table.c.compensation > 40000,
                ))
                .group_by(resume_table.c.workload)
                .having(func.avg(resume_table.c.compensation) > 70000)
            )
            print(query.compile(compile_kwargs={"literal_binds": True}))
            res = await conn.execute(query)
            result = res.all()
            print(result)
            print(result[0].avg_compensation)

    @staticmethod
    async def insert_additional_resumes():
        async with async_engine.connect() as conn:
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
            insert_workers = insert(worker_table).values(workers)
            insert_resumes = insert(resume_table).values(resumes)
            await conn.execute(insert_workers)
            await conn.execute(insert_resumes)
            await conn.commit()

    @staticmethod
    async def join_cte_subquery_window_func():
        async with async_engine.connect() as conn:
            r = aliased(resume_table)
            w = aliased(worker_table)
            subq = (
                select(
                    r,
                    w,
                    (
                        func.avg(r.c.compensation)
                        .over(partition_by=r.c.workload)
                        .cast(Integer)
                        .label("avg_workload_compensation")
                    ),
                )
                # .select_from(r)
                .join(r, r.c.worker_id == w.c.id).subquery("helper1")
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

            res = await conn.execute(query)
            result = res.all()
            print(f"{len(result)=}. {result=}")
