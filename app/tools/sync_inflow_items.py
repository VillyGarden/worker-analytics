import os, datetime as dt
import time, random
import requests
from app.db import SessionLocal
from sqlalchemy import text

API = 'https://api.moysklad.ru/api/remap/1.2'

def _date_range():
    start_s = os.getenv("START")
    end_s   = os.getenv("END")
    if start_s:
        ds = dt.date.fromisoformat(start_s)
        de = dt.date.fromisoformat(end_s) if end_s else ds
    else:
        de = dt.date.today()
        days = int(os.getenv("DAYS_BACK","7"))  # пока 7 для быстрых прогонов
        ds = de - dt.timedelta(days=days-1)
    return ds, de

def main():
    ds, de = _date_range()
    ms_token = bool(os.getenv("MS_TOKEN"))
    # Просто проверяем, что можем открыть сессию к БД
    with SessionLocal() as db:
        db.execute(text("SELECT 1"))
    ms_token = os.getenv("MS_TOKEN")
    ds, de = _date_range()
    print(f"[ok] skeleton ready. dates={ds}..{de}  MS_TOKEN={'set' if bool(ms_token) else 'MISSING'}")
    if ms_token:
        # берём последний день диапазона для проверки
        probe_day = de
        rows = fetch_enters_day(probe_day, ms_token)
        sample = [(r.get('id'), r.get('name')) for r in rows[:5]]
        print(f"[probe] enter {probe_day}: total={len(rows)} sample={sample}")

if __name__ == "__main__":
    main()


def _jget(url, headers, params=None, tries=0, timeout=60):
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    if r.status_code in (429, 500, 502, 503) and tries < 5:
        ra = r.headers.get('Retry-After')
        delay = float(ra) if ra else min(2**tries, 30) + random.uniform(0, 0.3)
        time.sleep(delay)
        return _jget(url, headers, params=params, tries=tries+1, timeout=timeout)
    r.raise_for_status()
    return r.json()


def fetch_enters_day(day: dt.date, token: str):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "worker-analytics/enter-day-check"
    }
    start_iso = f"{day.isoformat()} 00:00:00"
    end_iso   = f"{day.isoformat()} 23:59:59"
    url = f"{API}/entity/enter"
    params = {"limit": 1000, "expand": "positions,store", "filter": f"moment>={start_iso};moment<={end_iso}"}
    out = []
    while True:
        data = _jget(url, headers=headers, params=params)
        out.extend(data.get("rows", []))
        next_href = (data.get("meta") or {}).get("nextHref")
        if not next_href:
            break
        url, params = next_href, None
    return out
