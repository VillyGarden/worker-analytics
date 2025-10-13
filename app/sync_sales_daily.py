import datetime as dt
from dateutil.relativedelta import relativedelta
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

def iter_months(start: dt.date, end: dt.date):
    cur = dt.date(start.year, start.month, 1)
    stop = dt.date(end.year, end.month, 1)
    while cur <= stop:
        month_start = cur
        month_end = (cur + relativedelta(months=1)) - dt.timedelta(days=1)
        yield month_start, month_end
        cur = cur + relativedelta(months=1)

def fetch_sales_series(store_ms_id: str, date_from: dt.date, date_to: dt.date):
    params = {
        "momentFrom": f"{date_from} 00:00:00",
        "momentTo": f"{date_to} 23:59:59",
        "interval": "day",
        "filter": f"store={MS_BASE}/entity/store/{store_ms_id}",
    }
    url = f"{MS_BASE}/report/sales/plotseries"
    with httpx.Client(timeout=60.0, headers=HEADERS) as client:
        r = client.get(url, params=params)
        r.raise_for_status()
        data = r.json()
        return data.get("series", [])

def upsert_sales_daily(session, warehouse_id: int, d: dt.date, revenue_rub: Decimal, receipts: int):
    revenue_rub = (revenue_rub if isinstance(revenue_rub, Decimal) else Decimal(str(revenue_rub)))
    revenue_rub = revenue_rub.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    stmt = insert(SalesDaily).values(
        date=d,
        warehouse_id=warehouse_id,
        revenue=revenue_rub,
        receipts_count=receipts,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[SalesDaily.date, SalesDaily.warehouse_id],
        set_={
            "revenue": stmt.excluded.revenue,
            "receipts_count": stmt.excluded.receipts_count,
        },
    )
    session.execute(stmt)

def main(full_history: bool = False):
    start = dt.date(2019, 1, 1)
    today = dt.date.today()
    if not full_history:
        # только прошлый и текущий месяц для теста
        start = (today.replace(day=1) - dt.timedelta(days=1)).replace(day=1)

    db = SessionLocal()
    warehouses = db.query(Warehouse).all()
    if not warehouses:
        print("no warehouses in DB. run sync_warehouses first")
        return

    total_points = 0
    for w in warehouses:
        for m_from, m_to in iter_months(start, today):
            series = fetch_sales_series(w.ms_id, m_from, m_to)
            for point in series:
                d = dt.datetime.strptime(point["date"], "%Y-%m-%d %H:%M:%S").date()
                # у МС суммы в копейках, переводим в рубли
                revenue_rub = Decimal(str(point.get("sum", 0))) / Decimal(100)
                receipts = int(point.get("quantity", 0))
                upsert_sales_daily(db, w.id, d, revenue_rub, receipts)
                total_points += 1
            db.commit()
            print(f"{w.name}: {m_from}..{m_to} -> {len(series)} days")
    db.close()
    print(f"done, upserted points: {total_points}")

if __name__ == "__main__":
    main(full_history=False)
