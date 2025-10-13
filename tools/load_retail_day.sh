#!/usr/bin/env bash
# Загрузка retaildemand (продажи) за указанный день в sales_item_fact
set -euo pipefail

DAY="${1:-${DAY:-}}"
if [[ -z "${DAY}" ]]; then
  echo "Usage: tools/load_retail_day.sh YYYY-MM-DD" >&2
  exit 1
fi

# Токен берём из MS_API_TOKEN (как у тебя в .env)
: "${MS_API_TOKEN:?MS_API_TOKEN not set}"

PY="${PY:-python3}"
OUT_JSON="/tmp/ms_retail_${DAY}.json"
OUT_TSV="/tmp/retail_items_${DAY}.tsv"

# 1) Тянем документы retaildemand (с развернутыми positions)
"$PY" - <<PY
import os, json, pathlib, requests, datetime as dt
DAY = os.environ["DAY"]
token = os.environ["MS_API_TOKEN"]
nday = (dt.datetime.strptime(DAY, "%Y-%m-%d")+dt.timedelta(days=1)).strftime("%Y-%m-%d")
base = "https://api.moysklad.ru/api/remap/1.2/entity/retaildemand"
H = {
    "Authorization": f"Bearer {token}",
    "Accept": "application/json;charset=utf-8",
    "User-Agent": "worker-analytics/1.0",
}
r = requests.get(base, headers=H, params={
    "limit": 1000,
    "expand": "positions,store",
    "filter": f"moment>={DAY} 00:00:00;moment<{nday} 00:00:00",
}, timeout=60)
r.raise_for_status()
pathlib.Path("${OUT_JSON}").write_text(r.text, encoding="utf-8")
print("json_ok")
PY

# 2) JSON -> TSV (position_id,doc_id,date,warehouse_id,product_id,qty,price,revenue)
"$PY" - <<'PY'
import os, json, pathlib, decimal, requests
DAY = os.environ["DAY"]
token = os.environ["MS_API_TOKEN"]
dec = decimal.Decimal
data = json.loads(pathlib.Path(f"/tmp/ms_retail_{DAY}.json").read_text(encoding="utf-8"))
rows = data.get("rows", [])

H = {
    "Authorization": f"Bearer {token}",
    "Accept": "application/json;charset=utf-8",
    "User-Agent": "worker-analytics/1.0",
}
def uid_from(meta):
    href = (meta or {}).get("href") or ""
    return href.rsplit("/", 1)[-1] if "/" in href else href

cnt = 0
with open(f"/tmp/retail_items_{DAY}.tsv","w",encoding="utf-8") as out:
    for doc in rows:
        doc_id = doc.get("id")
        date = (doc.get("moment") or "")[:10] or DAY
        wh = uid_from((doc.get("store") or {}).get("meta") or {})
        pos_rows = (doc.get("positions") or {}).get("rows")
        if pos_rows is None:
            href = (doc.get("positions") or {}).get("meta", {}).get("href")
            pos_rows = []
            if href:
                rr = requests.get(href, headers=H, params={"limit": 1000}, timeout=60)
                rr.raise_for_status()
                pos_rows = rr.json().get("rows", [])
        for p in pos_rows:
            pos_id = p.get("id")
            prod = uid_from((p.get("assortment") or {}).get("meta") or {})
            qty = dec(str(p.get("quantity") or 0))
            price_kop = dec(str(p.get("price") or 0))
            sum_kop = p.get("sum")
            revenue = (price_kop*qty)/dec(100) if sum_kop is None else dec(str(sum_kop))/dec(100)
            price = dec("0.00") if qty == 0 else (revenue/qty)
            out.write("\t".join([
                str(pos_id), str(doc_id), str(date),
                str(wh), str(prod),
                f"{qty.normalize():f}",
                f"{price.quantize(dec('0.01'))}",
                f"{revenue.quantize(dec('0.01'))}",
            ]) + "\n")
            cnt += 1
print("__COUNT__", cnt)
PY

# 3) Загрузка TSV в БД (важно: \copy в начале строки)
psql -h "${DB_HOST:-localhost}" -p "${DB_PORT:-5432}" -U "${DB_USER:-worker}" -d "${DB_NAME:-worker_analytics}" <<SQL
BEGIN;
CREATE TEMP TABLE _sales_items_tmp(
  position_id uuid,
  doc_id uuid,
  date date,
  warehouse_id uuid,
  product_id uuid,
  qty numeric,
  price numeric,
  revenue numeric
) ON COMMIT DROP;
\copy _sales_items_tmp FROM '${OUT_TSV}' WITH (FORMAT text, DELIMITER E'\t', NULL '');
INSERT INTO sales_item_fact (position_id, doc_id, date, warehouse_id, product_id, qty, price, revenue, updated_at)
SELECT position_id, doc_id, date, warehouse_id, product_id, qty, price, revenue, now()
FROM _sales_items_tmp
ON CONFLICT (position_id) DO UPDATE
SET qty = EXCLUDED.qty,
    price = EXCLUDED.price,
    revenue = EXCLUDED.revenue,
    updated_at = now();
COMMIT;
SQL
