import os, asyncio, datetime as dt
from decimal import Decimal
from typing import Dict, Tuple
import httpx

from .db import SessionLocal
from .models import Warehouse, SalesDaily
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

MS_BASE = "https://api.moysklad.ru/api/remap/1.2"

# -------- токен: читаем MS_API_TOKEN из .env/окружения --------
def _read_token() -> str | None:
    tok = os.getenv("MS_API_TOKEN")
    if tok:
        return tok.strip().strip('"')
    # пробуем прочитать из .env файла проекта
    try:
        from pathlib import Path
        for line in (Path(__file__).resolve().parent.parent / ".env").read_text().splitlines():
            if line.startswith("MS_API_TOKEN="):
                v = line.split("=",1)[1].strip()
                if v.startswith('"') and v.endswith('"'):
                    v = v[1:-1]
                return v
    except Exception:
        pass
    return None

TOKEN = _read_token()
if not TOKEN:
    raise RuntimeError("MS_API_TOKEN пуст. Добавь в .env строку MS_API_TOKEN=\"...\"")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json;charset=utf-8",
    "Content-Type": "application/json",
}

# точное имя кастомного поля
REASON_FIELD_NAME = "ПРИЧИНА СПИСАНИЯ"

def bucket_for_reason(value: str) -> str:
    if not value:
        return "other"
    v = value.strip().lower()
    if "брак" in v:
        return "defect"
    if "инвентар" in v:
        return "inventory"
    return "other"

async def fetch_loss_docs(ac: httpx.AsyncClient, store_href: str, day: dt.date) -> list[dict]:
    docs = []
    offset = 0
    while True:
        params = {
            "limit": 100,
            "offset": offset,
            "filter": f"moment>={day} 00:00:00;moment<={day} 23:59:59;store={store_href}",
            "expand": "attributes",
            "order": "moment"
        }
        r = await ac.get(f"{MS_BASE}/entity/loss", params=params)
        r.raise_for_status()
        data = r.json()
        rows = data.get("rows", [])
        docs += rows
        if len(rows) < 100:
            break
        offset += 100
    return docs

async def fetch_positions_sum(ac: httpx.AsyncClient, doc_href: str) -> Decimal:
    total = Decimal("0")
    offset = 0
    while True:
        r = await ac.get(f"{doc_href}/positions", params={"limit": 1000, "offset": offset})
        if r.status_code == 429:
            # �������� ������������ ������� ��������, ��� � ���������
            await asyncio.sleep(2)
            continue
        r.raise_for_status()
        data = r.json()
        rows = data.get("rows", [])
        for p in rows:
            s = p.get("sum", 0) or 0
            total += Decimal(s) / Decimal(100)
        if len(rows) < 1000:
            break
        offset += 1000
        await asyncio.sleep(0.5)  # ���� ��������� �����, ����� �� ������ 429
    return total

async def collect_writeoff_for_day(ac: httpx.AsyncClient, wh_ms_id: str, day: dt.date) -> Tuple[Decimal, Dict[str, Decimal]]:
    store_href = f"{MS_BASE}/entity/store/{wh_ms_id}"
    docs = await fetch_loss_docs(ac, store_href, day)
    buckets = {"defect": Decimal("0"), "inventory": Decimal("0"), "other": Decimal("0")}
    total = Decimal("0")
    for d in docs:
        # причина списания из атрибутов
        reason_val = None
        for a in d.get("attributes") or []:
            if a.get("name") == REASON_FIELD_NAME:
                val = a.get("value")
                if isinstance(val, dict):
                    reason_val = val.get("name") or ""
                else:
                    reason_val = str(val or "")
                break
        bucket = bucket_for_reason(reason_val or "")
        # считаем себестоимость по позициям
        doc_href = d["meta"]["href"]
        cost = await fetch_positions_sum(ac, doc_href)
        total += cost
        buckets[bucket] += cost
    return total, buckets

def upsert_writeoff(db: Session, wh_id: int, day: dt.date, total, buckets):
    stmt = insert(SalesDaily).values(
        date=day, warehouse_id=wh_id,
        writeoff_cost_total=total,
        writeoff_cost_defect=buckets["defect"],
        writeoff_cost_inventory=buckets["inventory"],
        writeoff_cost_other=buckets["other"],
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["date", "warehouse_id"],
        set_={
            "writeoff_cost_total": stmt.excluded.writeoff_cost_total,
            "writeoff_cost_defect": stmt.excluded.writeoff_cost_defect,
            "writeoff_cost_inventory": stmt.excluded.writeoff_cost_inventory,
            "writeoff_cost_other": stmt.excluded.writeoff_cost_other,
        },
    )
    db.execute(stmt)

async def run_days(days_back: int = 30):
    async with httpx.AsyncClient(http2=True, headers=HEADERS, timeout=60.0) as ac:
        with SessionLocal() as db:
            whs = db.query(Warehouse).order_by(Warehouse.id).all()
            today = dt.date.today()
            start = today - dt.timedelta(days=days_back-1)
            for wh in whs:
                for i in range(days_back):
                    day = start + dt.timedelta(days=i)
                    total, buckets = await collect_writeoff_for_day(ac, wh.ms_id, day)
                    upsert_writeoff(db, wh.id, day, total, buckets)
                db.commit()
                print(f"[{wh.name}] {start}..{today} writeoff synced")
    print("done")

async def run_range(start, end):
    import asyncio
    days = []
    d = start
    while d <= end:
        days.append(d)
        d += datetime.timedelta(days=1)
    await run_days(days)


if __name__ == "__main__":
    days = int(os.getenv("DAYS_BACK", "30"))
    asyncio.run(run_days(days))
