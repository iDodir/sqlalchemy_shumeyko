from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine

from src.config import settings

sync_engine = create_engine(
    url=settings.DB_URL_PSYCOPG,
    echo=True,
    # pool_size=5,
    # max_overflow=10,
)

async_engine = create_async_engine(
    url=settings.DB_URL_ASYNCPG,
    echo=False,
)
