import datetime as dt
from decimal import Decimal, ROUND_HALF_UP
import httpx
from sqlalchemy.dialects.postgresql import insert
from .config import settings
from .db import SessionLocal
from .models import Warehouse, SalesDaily

MS_BASE = settings.MS_BASE_URL.rstrip("/")
HEADERS = {
    "Authorization": f"Bearer {settings.MS_API_TOKEN}",
    "Accept": "application/json;charset=utf-8",
    "Content-Type": "application/json",
    "User-Agent": "worker-analytics/1.0",
}

def fetch_enter_sum_for_day(store_ms_id: str, day: dt.date) -> Decimal:
    """Сумма оприходований (entity/enter.sum) за сутки по складу, в рублях."""
    # фильтр: момент в пределах суток + склад
    flt = f"moment>={day} 00:00:00;moment<={day} 23:59:59;store={MS_BASE}/entity/store/{store_ms_id}"
    url = f"{MS_BASE}/entity/enter"
    total_cents = Decimal("0")
    limit = 1000
    offset = 0
    with httpx.Client(timeout=60.0, headers=HEADERS) as c:
        while True:
            r = c.get(url, params={"limit": limit, "offset": offset, "filter": flt})
            r.raise_for_status()
            data = r.json()
            rows = data.get("rows", [])
            for doc in rows:
                total_cents += Decimal(str(doc.get("sum", 0) or 0))
            if len(rows) < limit:
                break
            offset += limit
    return (total_cents / Decimal(100)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def upsert_inflow(session, warehouse_id: int, day: dt.date, inflow_rub: Decimal):
    ins = insert(SalesDaily).values(
        date=day,
        warehouse_id=warehouse_id,
        inflow_cost=inflow_rub,
    )
    upsert = ins.on_conflict_do_update(
        index_elements=[SalesDaily.date, SalesDaily.warehouse_id],
        set_={"inflow_cost": ins.excluded.inflow_cost},
    )
    session.execute(upsert)

def main(days_back: int = 30):
    today = dt.date.today()
    start = today - dt.timedelta(days=days_back-1)

    db = SessionLocal()
    whs = db.query(Warehouse).all()
    if not whs:
        print("no warehouses in DB. run sync_warehouses first")
        return

    total = 0
    for w in whs:
        d = start
        while d <= today:
            inflow = fetch_enter_sum_for_day(w.ms_id, d)
            upsert_inflow(db, w.id, d, inflow)
            db.commit()
            print(f"{w.name} {d}: inflow={inflow}")
            total += 1
            d += dt.timedelta(days=1)
    db.close()
    print(f"done, updated days: {total}")

if __name__ == "__main__":
    main(days_back=30)
