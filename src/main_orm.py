from src.queries.orm import SyncORM

SyncORM.create_tables()
SyncORM.insert_workers()
SyncORM.select_workers()
SyncORM.update_worker()
SyncORM.select_workers()
SyncORM.insert_resumes()
SyncORM.select_resumes_avg_compensation()
