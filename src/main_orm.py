import asyncio

from src.queries.orm import create_tables, insert_data, insert_data_async

create_tables()
# insert_data()
asyncio.run(insert_data_async())
