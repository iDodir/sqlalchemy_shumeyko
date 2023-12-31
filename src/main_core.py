import asyncio

from src.queries.core import SyncCore, AsyncCore


def work_with_sync_core():
    SyncCore.create_tables()
    SyncCore.insert_workers()
    SyncCore.select_workers()
    SyncCore.update_worker()
    SyncCore.select_workers()
    SyncCore.insert_resumes()
    SyncCore.select_resumes_avg_compensation()
    SyncCore.insert_additional_resumes()
    SyncCore.join_cte_subquery_window_func()


async def work_with_async_core():
    await AsyncCore.create_tables()
    await AsyncCore.insert_workers()
    await AsyncCore.select_workers()
    await AsyncCore.update_worker()
    await AsyncCore.select_workers()
    await AsyncCore.insert_resumes()
    await AsyncCore.select_resumes_avg_compensation()
    await AsyncCore.insert_additional_resumes()
    await AsyncCore.join_cte_subquery_window_func()


if __name__ == "__main__":
    work_with_sync_core()
    asyncio.run(work_with_async_core())
