from datetime import date
from sqlalchemy import text
from app.db import get_session
from contextlib import contextmanager as _cm
from app.db import get_session as _get_session

@_cm
def session_cm():
    gen = _get_session()
    s = next(gen)
    try:
        yield s
    finally:
        try:
            # корректно закрыть генератор get_session()
            next(gen)
        except StopIteration:
            pass
def norm_reason(r: str) -> str:
    r = (r or "").lower()
    if "инвент" in r: return "Инвентаризация"
    if "брак" in r:   return "Брак"
    return "Прочее"

def run(start: date, end: date):
with session_cm() as s:
           s.execute(text("DELETE FROM writeoff_daily WHERE date BETWEEN :s AND :e"), {"s": start, "e": end})
        s.execute(text("DELETE FROM writeoff_daily_reason WHERE date BETWEEN :s AND :e"), {"s": start, "e": end})

        rows = s.execute(text("""
            SELECT day, warehouse_id,
                   SUM(cost) AS total,
                   SUM(CASE WHEN lower(reason) LIKE 'брак%' THEN cost ELSE 0 END) AS defect,
                   SUM(CASE WHEN lower(reason) LIKE 'инвент%' THEN cost ELSE 0 END) AS inventory
            FROM writeoff_item
            WHERE day BETWEEN :s AND :e
            GROUP BY day, warehouse_id
        """), {"s": start, "e": end}).mappings().all()

        if rows:
            s.execute(text("""
                INSERT INTO writeoff_daily(date, warehouse_id, total, defect, inventory, other)
                VALUES (:day, :wid, :t, :d, :i, (:t - :d - :i))
            """), [{"day":r["day"], "wid":r["warehouse_id"],
                    "t":float(r["total"] or 0), "d":float(r["defect"] or 0), "i":float(r["inventory"] or 0)} for r in rows])

        rows2 = s.execute(text("""
            SELECT day, warehouse_id, reason, SUM(cost) AS cost
            FROM writeoff_item
            WHERE day BETWEEN :s AND :e
            GROUP BY day, warehouse_id, reason
        """), {"s": start, "e": end}).mappings().all()

        if rows2:
            s.execute(text("""
                INSERT INTO writeoff_daily_reason(date, warehouse_id, reason, cost)
                VALUES (:day, :wid, :reason, :cost)
            """), [{"day":r["day"], "wid":r["warehouse_id"],
                    "reason":norm_reason(r["reason"]), "cost":float(r["cost"] or 0)} for r in rows2])

        s.commit()
    print(f"✅ rebuild done: {start}..{end}")

if __name__ == "__main__":
    import os
    s = date.fromisoformat(os.environ.get("START") or "2019-01-01")
    e = date.fromisoformat(os.environ.get("END")   or date.today().isoformat())
    run(s, e)
