import os, sys, datetime as dt, time, random, requests, json

API = os.getenv("MS_BASE_URL", "https://api.moysklad.ru/api/remap/1.2").rstrip("/")
def _env(name):
    v = os.getenv(name)
    if v: return v
    # попробовать вытащить из .env (лениво и без зависимостей)
    try:
        from pathlib import Path
        for line in Path(".env").read_text(encoding="utf-8", errors="ignore").splitlines():
            if not line or line.lstrip().startswith("#"): continue
            if line.split("=",1)[0].strip()==name:
                val = line.split("=",1)[1].split("#")[0].strip().strip("'").strip('"')
                return val
    except Exception:
        pass
    return None

def _jget(url, headers, params=None, tries=0, timeout=60):
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    if r.status_code in (429,500,502,503) and tries < 5:
        ra = r.headers.get("Retry-After")
        try:
            delay = float(ra) if ra else min(2**tries,30) + random.uniform(0,0.3)
        except Exception:
            delay = min(2**tries,30) + random.uniform(0,0.3)
        time.sleep(delay)
        return _jget(url, headers, params=params, tries=tries+1, timeout=timeout)
    try:
        r.raise_for_status()
    except Exception as e:
        try:
            print('--- MS ERROR BODY ---')
            print(r.text)
            print('----------------------')
        except Exception:
            pass
        raise
    return r.json()

def fetch_enters_day(day: dt.date, token: str):
    next_day = day + dt.timedelta(days=1)
    CANDIDATE_FILTERS = [
        f"moment>={day} 00:00:00;moment<{next_day} 00:00:00",
        f"moment>={day} 00:00:00;moment<={day} 23:59:59",
        f"updated>={day} 00:00:00;updated<{next_day} 00:00:00",
        f"created>={day} 00:00:00;created<{next_day} 00:00:00",
    ]
    last_err = None
    for filt in CANDIDATE_FILTERS:
        try:
            rows = []
            headers = {
                'Authorization': f'Bearer {token}',
                'Accept': 'application/json;charset=utf-8',
                'Content-Type': 'application/json',
                'User-Agent': 'worker-analytics/probe-enter'
            }
            url = f"{API}/entity/enter"
            params = {'limit': 1000, 'expand': 'positions,store', 'filter': filt}
            while True:
                data = _jget(url, headers=headers, params=params)
                rows.extend(data.get('rows', []))
                next_href = (data.get('meta') or {}).get('nextHref')
                if not next_href:
                    break
                url, params = next_href, None
            if rows:
                print(f"[filter-ok] {filt} -> {len(rows)} docs")
            else:
                print(f"[filter-ok-empty] {filt} -> 0 docs")
            return rows
        except Exception as e:
            last_err = e
            print(f"[filter-fail] {filt}: {e}")
            continue
    if last_err:
        raise last_err
    return []

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "worker-analytics/probe-enter"
    }
    start_iso = f"{day.isoformat()} 00:00:00"
    end_iso   = f"{day.isoformat()} 23:59:59"
    url = f"{API}/entity/enter"
    params = {"limit": 1000, "expand":"positions,store", "filter": f"moment>={start_iso};moment<={end_iso}"}
    rows = []
    while True:
        data = _jget(url, headers=headers, params=params)
        rows.extend(data.get("rows", []))
        next_href = (data.get("meta") or {}).get("nextHref")
        if not next_href: break
        url, params = next_href, None
    return rows

def main():
    day_str = os.getenv("DAY") or os.getenv("START") or dt.date.today().isoformat()
    day = dt.date.fromisoformat(day_str)
    token = _env("MS_TOKEN") or _env("MS_API_TOKEN")
    if not token:
        print("⛔ Нет токена MS_TOKEN/MS_API_TOKEN ни в окружении, ни в .env")
        sys.exit(2)
    docs = fetch_enters_day(day, token)
    # соберём краткую сводку и первые позиции
    total_docs = len(docs)
    sample = [(d.get("id"), d.get("name")) for d in docs[:5]]
    pos_count = 0
    first_positions = []
    for d in docs[:3]:
        for p in (d.get("positions") or {}).get("rows", [])[:3]:
            pos_count += 1
            ass = p.get("assortment") or {}
            first_positions.append({
                "doc": d.get("name"),
                "product": ass.get("name"),
                "qty": p.get("quantity"),
                "price_rub": (p.get("price") or 0)/100,
                "sum_rub": (p.get("sum") or 0)/100,
            })
    out = {
        "day": day.isoformat(),
        "docs_total": total_docs,
        "docs_sample": sample,
        "positions_sample_count": pos_count,
        "positions_sample": first_positions
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
