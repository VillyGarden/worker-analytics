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

def fetch_profit_by_day(store_ms_id: str, day: dt.date) -> tuple[Decimal, Decimal]:
    """Возвращает (sellCostSum, returnCostSum) за сутки по складу."""
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
    sell_cost_sum = Decimal("0")
    return_cost_sum = Decimal("0")
    for row in rows:
        sell_cost_sum += Decimal(str(row.get("sellCostSum", 0)))
        return_cost_sum += Decimal(str(row.get("returnCostSum", 0)))
    # из копеек в рубли
    sell_cost_sum = (sell_cost_sum / Decimal(100)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return_cost_sum = (return_cost_sum / Decimal(100)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return sell_cost_sum, return_cost_sum

def upsert_costs(session, warehouse_id: int, day: dt.date, cost_rub: Decimal, returns_cost_rub: Decimal):
    ins = insert(SalesDaily).values(
        date=day,
        warehouse_id=warehouse_id,
        cost=cost_rub,
        returns_cost=returns_cost_rub,
    )
    upsert = ins.on_conflict_do_update(
        index_elements=[SalesDaily.date, SalesDaily.warehouse_id],
        set_={
            "cost": ins.excluded.cost,
            "returns_cost": ins.excluded.returns_cost,
        },
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

    total_updates = 0
    for w in warehouses:
        d = start
        while d <= today:
            cost, ret_cost = fetch_profit_by_day(w.ms_id, d)
            upsert_costs(db, w.id, d, cost, ret_cost)
            db.commit()
            print(f"{w.name} {d}: cost={cost} returns_cost={ret_cost}")
            total_updates += 1
            d += dt.timedelta(days=1)
    db.close()
    print(f"done, updated days: {total_updates}")

if __name__ == "__main__":
    main(days_back=14)
