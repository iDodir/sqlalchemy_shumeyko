import asyncio

from src.queries.orm import SyncORM, AsyncORM


def work_with_sync_orm():
    SyncORM.create_tables()
    SyncORM.insert_workers()
    # SyncORM.select_workers()
    SyncORM.update_worker()
    # SyncORM.select_workers()
    SyncORM.insert_resumes()
    # SyncORM.select_resumes_avg_compensation()
    SyncORM.insert_additional_resumes()
    # SyncORM.join_cte_subquery_window_func()
    # SyncORM.select_workers_with_lazy_relationship()
    # SyncORM.select_workers_with_joined_relationship()
    SyncORM.select_workers_with_selectin_relationship()


async def work_with_async_orm():
    await AsyncORM.create_tables()
    await AsyncORM.insert_workers()
    # await AsyncORM.select_workers()
    await AsyncORM.update_worker()
    # await AsyncORM.select_workers()
    await AsyncORM.insert_resumes()
    # await AsyncORM.select_resumes_avg_compensation()
    await AsyncORM.insert_additional_resumes()
    # await AsyncORM.join_cte_subquery_window_func()
    # await AsyncORM.select_workers_with_lazy_relationship()
    # await AsyncORM.select_workers_with_joined_relationship()
    await AsyncORM.select_workers_with_lazy_relationship()


if __name__ == "__main__":
    # work_with_sync_orm()
    asyncio.run(work_with_async_orm())
