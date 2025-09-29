import asyncio
from sqlalchemy.dialects.postgresql import insert
from .db import SessionLocal
from .models import Warehouse
from .ms_client import MSClient

async def main() -> int:
    client = MSClient()
    session = SessionLocal()
    count = 0
    try:
        async for ws in client.get_stores():
            ms_id = ws.get("id")
            name = (ws.get("name") or "Без названия").strip()
            if not ms_id:
                continue
            stmt = insert(Warehouse).values(ms_id=ms_id, name=name)
            stmt = stmt.on_conflict_do_update(
                index_elements=[Warehouse.ms_id],
                set_={"name": stmt.excluded.name},
            )
            session.execute(stmt)
            count += 1
        session.commit()
    finally:
        session.close()
        await client.close()
    print(f"warehouses synced: {count}")
    return count

if __name__ == "__main__":
    asyncio.run(main())
