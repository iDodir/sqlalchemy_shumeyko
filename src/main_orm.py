import asyncio

from src.queries.orm import SyncORM, AsyncORM


def work_with_sync_orm():
    SyncORM.create_tables()
    SyncORM.insert_workers()
    SyncORM.select_workers()
    SyncORM.update_worker()
    SyncORM.select_workers()
    SyncORM.insert_resumes()
    SyncORM.select_resumes_avg_compensation()


async def work_with_async_orm():
    await AsyncORM.create_tables()
    await AsyncORM.insert_workers()
    await AsyncORM.select_workers()
    await AsyncORM.update_worker()
    await AsyncORM.select_workers()
    await AsyncORM.insert_resumes()
    await AsyncORM.select_resumes_avg_compensation()


if __name__ == "__main__":
    work_with_sync_orm()
    asyncio.run(work_with_async_orm())
