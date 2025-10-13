import os, datetime as dt, time, random, json, sys
import requests

API = 'https://api.moysklad.ru/api/remap/1.2'

def _jget(url, headers, params=None, tries=0, timeout=60):
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    if r.status_code in (429, 500, 502, 503) and tries < 5:
        ra = r.headers.get('Retry-After')
        delay = float(ra) if ra else min(2**tries, 30) + random.uniform(0, 0.3)
        time.sleep(delay)
        return _jget(url, headers, params=params, tries=tries+1, timeout=timeout)
    if not r.ok:
        sys.stderr.write("--- MS ERROR BODY ---\n"+r.text+"\n----------------------\n")
    r.raise_for_status()
    return r.json()

def fetch_retail_day(day: dt.date, token: str):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json;charset=utf-8",
        "Content-Type": "application/json",
        "User-Agent": "worker-analytics/retail-day-check"
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

def main():
    token = os.getenv("MS_TOKEN") or os.getenv("MS_API_TOKEN")
    if not token:
        print("MS_TOKEN отсутствует")
        sys.exit(2)
    day_s = os.getenv("DAY")
    day = dt.date.fromisoformat(day_s) if day_s else dt.date.today()
    docs = fetch_retail_day(day, token)
    sample = [[d.get("id"), d.get("name")] for d in docs[:3]]
    print(json.dumps({"day": day.isoformat(), "docs_total": len(docs), "docs_sample": sample}, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
