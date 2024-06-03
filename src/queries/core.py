from sqlalchemy import Integer, and_, func, insert, select, text, update
from sqlalchemy.orm import aliased

from src.database import async_engine, sync_engine
from src.models import metadata_obj, workers_table


def get_123_sync():
    with sync_engine.connect() as conn:
        res = conn.execute(text("SELECT 1,2,3 union select 4,5,6"))
        print(f"{res.first()=}")


async def get_123_async():
    async with async_engine.connect() as conn:
        res = await conn.execute(text("SELECT 1,2,3 union select 4,5,6"))
        print(f"{res.first()=}")

def create_table():
    metadata_obj.create_all(sync_engine)