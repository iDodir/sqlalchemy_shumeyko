from sqlalchemy import insert, cast, func, Integer, and_
from sqlalchemy import select, text, update

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
