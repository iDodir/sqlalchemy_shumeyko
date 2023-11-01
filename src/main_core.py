from src.queries.core import SyncCore

SyncCore.create_tables()
SyncCore.insert_workers()
SyncCore.select_workers()
SyncCore.update_worker()
SyncCore.select_workers()
SyncCore.insert_resumes()
SyncCore.select_resumes_avg_compensation()
