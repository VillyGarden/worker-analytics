import os, datetime as dt, time, random, sys
from decimal import Decimal, ROUND_HALF_UP
import requests
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text
from app.db import SessionLocal
API = 'https://api.moysklad.ru/api/remap/1.2'

def _jget(url, headers, params=None, tries=0, timeout=60):
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    if r.status_code in (429, 500, 502, 503) and tries < 5:
        ra = r.headers.get('Retry-After')
        delay = float(ra) if ra else min(2**tries, 30) + random.uniform(0, 0.3)
        time.sleep(delay)
        return _jget(url, headers, params=params, tries=tries+1, timeout=timeout)
    r.raise_for_status()
    return r.json()

def fetch_retail_day(day: dt.date, token: str):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json;charset=utf-8",
        "Content-Type": "application/json",
        "User-Agent": "worker-analytics/retail-day-load"
    }
    start_iso = f"{day.isoformat()} 00:00:00"
    end_iso   = f"{(day + dt.timedelta(days=1)).isoformat()} 00:00:00"
    url = f"{API}/entity/retaildemand"
    params = {"limit": 1000, "expand": "positions,store", "filter": f"moment>={start_iso};moment<{end_iso}"}
    out = []
    while True:
        data = _jget(url, headers=headers, params=params)
        rows = data.get("rows", [])
        out.extend(rows)
        next_href = (data.get("meta") or {}).get("nextHref")
        if not next_href:
            break
        url, params = next_href, None
    return out

def upsert_positions(db, day, docs):
    # одна позиция -> одна строка в sales_item_fact
    stmt = text("""
        INSERT INTO sales_item_fact (position_id, doc_id, date, warehouse_id, product_id, qty, price, revenue)
        VALUES (:position_id, :doc_id, :date, :warehouse_id, :product_id, :qty, :price, :revenue)
        ON CONFLICT (position_id) DO UPDATE SET
          qty = EXCLUDED.qty,
          price = EXCLUDED.price,
          revenue = EXCLUDED.revenue,
          date = EXCLUDED.date,
          warehouse_id = EXCLUDED.warehouse_id,
          product_id = EXCLUDED.product_id,
          updated_at = now()
    """)
    total = 0
    for d in docs:
        doc_id = d.get("id")
        wh = d.get("store") or {}
        wh_href = (wh.get("meta") or {}).get("href","")
        try:
            warehouse_id = wh_href.rsplit("/",1)[-1]
        except Exception:
            continue
        positions = (d.get("positions") or {}).get("rows", [])
        for p in positions:
            pos_id = p.get("id")
            a = p.get("assortment") or {}
            a_href = (a.get("meta") or {}).get("href","")
            try:
                product_id = a_href.rsplit("/",1)[-1]
            except Exception:
                continue
            qty = Decimal(str(p.get("quantity", 0)))
            price = (Decimal(str(p.get("price", 0))) / Decimal(100)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            revenue = (Decimal(str(p.get("sum", 0))) / Decimal(100)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            db.execute(stmt, {
                "position_id": pos_id,
                "doc_id": doc_id,
                "date": day.isoformat(),
                "warehouse_id": warehouse_id,
                "product_id": product_id,
                "qty": qty,
                "price": price,
                "revenue": revenue
            })
            total += 1
    return total

def main():
    token = os.getenv("MS_TOKEN") or os.getenv("MS_API_TOKEN")
    if not token:
        print("MS_TOKEN отсутствует"); sys.exit(2)
    day_s = os.getenv("DAY")
    day = dt.date.fromisoformat(day_s) if day_s else dt.date.today()
    docs = fetch_retail_day(day, token)
    with SessionLocal() as db:
        db.execute(text("SELECT 1"))
        n = upsert_positions(db, day, docs)
        db.commit()
    print(f"[retail] {day}: {len(docs)} документов, позиций upsert: {n}")

if __name__ == "__main__":
    main()
