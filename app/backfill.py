import datetime as dt
from decimal import Decimal, ROUND_HALF_UP
import httpx
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from .config import settings
from .db import SessionLocal
from .models import Warehouse, SalesDaily

# используем готовые функции из наших синк-скриптов
from .sync_profit_daily import fetch_profit_by_day
from .sync_discounts_daily import fetch_discount_by_day
from .sync_inflow_daily import fetch_enter_sum_for_day

MS_BASE = settings.MS_BASE_URL.rstrip("/")
HEADERS = {
    "Authorization": f"Bearer {settings.MS_API_TOKEN}",
    "Accept": "application/json;charset=utf-8",
    "Content-Type": "application/json",
    "User-Agent": "worker-analytics/backfill",
}

def month_range(y: int, m: int) -> tuple[dt.date, dt.date]:
    start = dt.date(y, m, 1)
    if m == 12:
        end = dt.date(y + 1, 1, 1) - dt.timedelta(days=1)
    else:
        end = dt.date(y, m + 1, 1) - dt.timedelta(days=1)
    return start, end

def fetch_sales_plotseries(store_ms_id: str, start: dt.date, end: dt.date):
    """Возвращает список {date, revenue_rub, receipts} по дням за месяц по складу."""
    url = f"{MS_BASE}/report/sales/plotseries"
    params = {
        "momentFrom": f"{start} 00:00:00",
        "momentTo": f"{end} 23:59:59",
        "interval": "day",
        "filter": f"store={MS_BASE}/entity/store/{store_ms_id}",
    }
    with httpx.Client(timeout=60.0, headers=HEADERS) as c:
        r = c.get(url, params=params)
        r.raise_for_status()
        data = r.json()
    out = []
    for row in data.get("series", []) or []:
        d = dt.datetime.strptime(row["date"], "%Y-%m-%d %H:%M:%S").date()
        # в ответе суммы и количества в КОПЕЙКАХ и ШТ, revenue = sum/100
        receipts = int(row.get("quantity", 0) or 0)
        revenue_rub = (Decimal(str(row.get("sum", 0) or 0)) / Decimal(100)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        out.append({"date": d, "revenue": revenue_rub, "receipts": receipts})
    return out

def upsert_sales(session: Session, warehouse_id: int, day: dt.date, revenue_rub: Decimal, receipts: int):
    ins = insert(SalesDaily).values(
        date=day,
        warehouse_id=warehouse_id,
        revenue=revenue_rub,
        receipts_count=receipts,
    )
    upsert = ins.on_conflict_do_update(
        index_elements=[SalesDaily.date, SalesDaily.warehouse_id],
        set_={"revenue": ins.excluded.revenue, "receipts_count": ins.excluded.receipts_count},
    )
    session.execute(upsert)

def upsert_costs(session: Session, warehouse_id: int, day: dt.date, cost_rub: Decimal, returns_cost_rub: Decimal):
    ins = insert(SalesDaily).values(
        date=day,
        warehouse_id=warehouse_id,
        cost=cost_rub,
        returns_cost=returns_cost_rub,
    )
    upsert = ins.on_conflict_do_update(
        index_elements=[SalesDaily.date, SalesDaily.warehouse_id],
        set_={"cost": ins.excluded.cost, "returns_cost": ins.excluded.returns_cost},
    )
    session.execute(upsert)

def upsert_discount(session: Session, warehouse_id: int, day: dt.date, discount_rub: Decimal):
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

def upsert_inflow(session: Session, warehouse_id: int, day: dt.date, inflow_rub: Decimal):
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

def backfill_month(year: int, month: int):
    start, end = month_range(year, month)
    db = SessionLocal()
    warehouses = db.query(Warehouse).all()
    if not warehouses:
        print("no warehouses in DB")
        return
    total_points = 0
    for w in warehouses:
        # 1) выручка и чеки за месяц (один запрос plotseries)
        daily = {d["date"]: d for d in fetch_sales_plotseries(w.ms_id, start, end)}
        for day, rec in daily.items():
            upsert_sales(db, w.id, day, rec["revenue"], rec["receipts"])
            total_points += 1

        # 2) по каждому дню подтягиваем cost/returns, discount и inflow
        d = start
        while d <= end:
            cost, ret_cost = fetch_profit_by_day(w.ms_id, d)
            disc = fetch_discount_by_day(w.ms_id, d)
            inflow = fetch_enter_sum_for_day(w.ms_id, d)

            upsert_costs(db, w.id, d, cost, ret_cost)
            upsert_discount(db, w.id, d, disc)
            upsert_inflow(db, w.id, d, inflow)
            db.commit()
            print(f"[{w.name}] {d}: revenue={daily.get(d,{}).get('revenue','—')} checks={daily.get(d,{}).get('receipts','—')} cost={cost} ret={ret_cost} disc={disc} inflow={inflow}")
            d += dt.timedelta(days=1)
    db.close()
    print(f"month {year}-{month:02d} done. upserted days: {total_points}")

def main():
    import os
    y = int(os.environ.get("BF_YEAR"))
    m = int(os.environ.get("BF_MONTH"))
    backfill_month(y, m)

if __name__ == "__main__":
    main()
