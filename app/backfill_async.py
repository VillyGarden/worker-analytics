import asyncio
import datetime as dt
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Tuple, Optional
import random
import httpx
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from .config import settings
from .db import SessionLocal
from .models import Warehouse, SalesDaily

MS_BASE = settings.MS_BASE_URL.rstrip("/")
HEADERS = {
    "Authorization": f"Bearer {settings.MS_API_TOKEN}",
    "Accept": "application/json;charset=utf-8",
    "Content-Type": "application/json",
    "User-Agent": "worker-analytics/backfill-async",
}

# Глобальный семафор и «тикер» чтобы не превысить лимиты (45 rps в ответах мы видели — возьмём безопасные ~10 rps)
GLOBAL_CONCURRENCY = 3
REQS_PER_SECOND = 10
_last_req_ts = 0.0
_lock = asyncio.Lock()
_sem = asyncio.Semaphore(GLOBAL_CONCURRENCY)

def month_range(y: int, m: int) -> tuple[dt.date, dt.date]:
    start = dt.date(y, m, 1)
    end = (dt.date(y + (m==12), (m % 12) + 1, 1) - dt.timedelta(days=1))
    return start, end

def rub(cents: Decimal | int | float) -> Decimal:
    d = Decimal(str(cents))
    return (d / Decimal(100)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

async def _pacer():
    """Примитивный rate-limit по времени между запросами."""
    global _last_req_ts
    async with _lock:
        import time
        now = time.monotonic()
        min_gap = 1.0 / REQS_PER_SECOND
        dt_gap = now - _last_req_ts
        if dt_gap < min_gap:
            await asyncio.sleep(min_gap - dt_gap)
        _last_req_ts = time.monotonic()

def _retry_sleep_hint(resp: httpx.Response) -> float:
    # Понимаем заголовки МоегоСклада: x-lognex-retry-after (секунды), x-lognex-retry-timeinterval (мс)
    try:
        if resp is None:
            return 1.5
        h = resp.headers
        if "x-lognex-retry-after" in h:
            return max(1.0, float(h.get("x-lognex-retry-after", "1")))
        if "x-lognex-retry-timeinterval" in h:
            return max(0.5, float(h.get("x-lognex-retry-timeinterval", "1000"))/1000.0)
    except Exception:
        pass
    return 1.5

async def _get(ac: httpx.AsyncClient, url: str, *, params: Optional[dict]=None, max_attempts: int=6) -> httpx.Response:
    """GET с троттлингом, ретраями на 429/5xx и экспоненциальным джиттером."""
    attempt = 0
    backoff = 1.0
    while True:
        attempt += 1
        async with _sem:
            await _pacer()
            r = await ac.get(url, params=params)
        if r.status_code < 400:
            return r
        # ошибки
        if r.status_code in (429, 500, 502, 503, 504):
            wait = max(backoff, _retry_sleep_hint(r)) * (1.0 + random.random()*0.25)
            if attempt >= max_attempts:
                r.raise_for_status()
            await asyncio.sleep(wait)
            backoff = min(backoff*1.7, 15.0)
            continue
        r.raise_for_status()

async def fetch_sales_plotseries(ac: httpx.AsyncClient, store_id: str, start: dt.date, end: dt.date):
    url = f"{MS_BASE}/report/sales/plotseries"
    params = {
        "momentFrom": f"{start} 00:00:00",
        "momentTo": f"{end} 23:59:59",
        "interval": "day",
        "filter": f"store={MS_BASE}/entity/store/{store_id}",
    }
    r = await _get(ac, url, params=params)
    data = r.json()
    out: Dict[dt.date, Tuple[Decimal, int]] = {}
    for row in data.get("series") or []:
        d = dt.datetime.strptime(row["date"], "%Y-%m-%d %H:%M:%S").date()
        revenue = rub(row.get("sum", 0) or 0)
        checks = int(row.get("quantity", 0) or 0)
        out[d] = (revenue, checks)
    return out

async def fetch_profit_by_day(ac: httpx.AsyncClient, store_id: str, day: dt.date) -> Tuple[Decimal, Decimal, Decimal]:
    """Возвращает (sellCostSum_rub, returnCostSum_rub, discount_rub) за день."""
    url = f"{MS_BASE}/report/profit/byproduct"
    params = {
        "momentFrom": f"{day} 00:00:00",
        "momentTo": f"{day} 23:59:59",
        "filter": f"store={MS_BASE}/entity/store/{store_id}",
        "limit": 1000,
    }
    r = await _get(ac, url, params=params)
    data = r.json()
    rows = data.get("rows") or []
    sell_cost_c = Decimal("0")
    ret_cost_c  = Decimal("0")
    disc_c      = Decimal("0")
    for row in rows:
        sell_cost_c += Decimal(str(row.get("sellCostSum", 0)))
        ret_cost_c  += Decimal(str(row.get("returnCostSum", 0)))
        price = Decimal(str(row.get("sellPrice", 0)))
        qty   = Decimal(str(row.get("sellQuantity", 0)))
        ssum  = Decimal(str(row.get("sellSum", 0)))
        disc_c += (price * qty) - ssum
    return rub(sell_cost_c), rub(ret_cost_c), rub(disc_c)

async def fetch_inflow_by_day(ac: httpx.AsyncClient, store_id: str, day: dt.date) -> Decimal:
    url = f"{MS_BASE}/entity/enter"
    flt = f"moment>={day} 00:00:00;moment<={day} 23:59:59;store={MS_BASE}/entity/store/{store_id}"
    total_c = Decimal("0")
    limit = 1000
    offset = 0
    while True:
        r = await _get(ac, url, params={"limit": limit, "offset": offset, "filter": flt})
        data = r.json()
        rows = data.get("rows", [])
        for doc in rows:
            total_c += Decimal(str(doc.get("sum", 0) or 0))
        if len(rows) < limit:
            break
        offset += limit
    return rub(total_c)

async def backfill_month_async(year: int, month: int, concurrency: int = 2):
    start, end = month_range(year, month)
    db = SessionLocal()
    warehouses = db.query(Warehouse).all()
    if not warehouses:
        print("no warehouses in DB"); db.close(); return

    async with httpx.AsyncClient(timeout=60.0, headers=HEADERS, http2=True) as ac:
        for w in warehouses:
            sales_map = await fetch_sales_plotseries(ac, w.ms_id, start, end)

            sem_local = asyncio.Semaphore(max(1, concurrency))

            async def one_day(d: dt.date):
                async with sem_local:
                    cost, ret_cost, disc = await fetch_profit_by_day(ac, w.ms_id, d)
                    inflow = await fetch_inflow_by_day(ac, w.ms_id, d)
                    rev, checks = sales_map.get(d, (Decimal("0"), 0))
                    return d, rev, checks, cost, ret_cost, disc, inflow

            days = []
            cur = start
            while cur <= end:
                days.append(cur)
                cur += dt.timedelta(days=1)

            results = []
            # небольшие порции, чтобы ровнее ложился rate-limit
            chunk_size = max(3, concurrency * 2)
            for i in range(0, len(days), chunk_size):
                chunk = days[i:i+chunk_size]
                results += await asyncio.gather(*[one_day(d) for d in chunk])
                # пауза между пачками
                await asyncio.sleep(0.5)

            # один коммит на склад-месяц
            for d, rev, checks, cost, ret_cost, disc, inflow in results:
                ins = insert(SalesDaily).values(
                    date=d, warehouse_id=w.id,
                    revenue=rev, receipts_count=checks,
                    cost=cost, returns_cost=ret_cost,
                    discount=disc, inflow_cost=inflow,
                )
                up = ins.on_conflict_do_update(
                    index_elements=[SalesDaily.date, SalesDaily.warehouse_id],
                    set_={
                        "revenue": ins.excluded.revenue,
                        "receipts_count": ins.excluded.receipts_count,
                        "cost": ins.excluded.cost,
                        "returns_cost": ins.excluded.returns_cost,
                        "discount": ins.excluded.discount,
                        "inflow_cost": ins.excluded.inflow_cost,
                    },
                )
                db.execute(up)
            db.commit()
            print(f"[{w.name}] {year}-{month:02d}: {len(results)} days upserted")

    db.close()
    print(f"month {year}-{month:02d} done")

async def backfill_range(y_from: int, m_from: int, y_to: int, m_to: int):
    y, m = y_from, m_from
    while (y < y_to) or (y == y_to and m <= m_to):
        await backfill_month_async(y, m, concurrency=2)
        if m == 12:
            y += 1; m = 1
        else:
            m += 1

def main():
    import os
    y1 = int(os.environ.get("BF_FROM_YEAR"))
    m1 = int(os.environ.get("BF_FROM_MONTH"))
    y2 = int(os.environ.get("BF_TO_YEAR"))
    m2 = int(os.environ.get("BF_TO_MONTH"))
    asyncio.run(backfill_range(y1, m1, y2, m2))

if __name__ == "__main__":
    main()
