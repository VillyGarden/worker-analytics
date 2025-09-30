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

def fetch_discount_by_day(store_ms_id: str, day: dt.date) -> Decimal:
    """Возвращает сумму скидок за сутки по складу (в рублях)."""
    params = {
        "momentFrom": f"{day} 00:00:00",
        "momentTo": f"{day} 23:59:59",
        "filter": f"store={MS_BASE}/entity/store/{store_ms_id}",
        "limit": 1000,
    }
    url = f"{MS_BASE}/report/profit/byproduct"
    with httpx.Client(timeout=60.0, headers=HEADERS) as c:
        r = c.get(url, params=params)
        r.raise_for_status()
        data = r.json()
    rows = data.get("rows") or []

    # скидка = (sellPrice * sellQuantity) - sellSum, всё в копейках
    disc_cents = Decimal("0")
    for row in rows:
        price = Decimal(str(row.get("sellPrice", 0)))
        qty   = Decimal(str(row.get("sellQuantity", 0)))
        sum_  = Decimal(str(row.get("sellSum", 0)))
        disc_cents += (price * qty) - sum_
    # в рубли и округляем до копеек
    return (disc_cents / Decimal(100)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def upsert_discount(session, warehouse_id: int, day: dt.date, discount_rub: Decimal):
    ins = insert(SalesDaily).values(
        date=day,
        warehouse_id=warehouse_id,
        discount=discount_rub,
    )
    upsert = ins.on_conflict_do_update(
        index_elements=[SalesDaily.date, SalesDaily.warehouse_id],
        set_={"discount": ins.excluded.discount},
    )
    session.execute(upsert)

def main(days_back: int = 14):
    today = dt.date.today()
    start = today - dt.timedelta(days=days_back-1)

    db = SessionLocal()
    warehouses = db.query(Warehouse).all()
    if not warehouses:
        print("no warehouses in DB. run sync_warehouses first")
        return

    total = 0
    for w in warehouses:
        d = start
        while d <= today:
            disc = fetch_discount_by_day(w.ms_id, d)
            upsert_discount(db, w.id, d, disc)
            db.commit()
            print(f"{w.name} {d}: discount={disc}")
            total += 1
            d += dt.timedelta(days=1)
    db.close()
    print(f"done, updated days: {total}")

if __name__ == "__main__":
    main(days_back=14)
