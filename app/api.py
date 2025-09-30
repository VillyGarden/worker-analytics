from datetime import date, timedelta
from typing import List, Dict, Any, Optional, Literal
from sqlalchemy import select, func
from sqlalchemy.orm import Session
from .models import SalesDaily, Warehouse

def get_revenue_daily(session: Session, days: int = 60) -> List[Dict[str, Any]]:
    start = date.today() - timedelta(days=days)
    stmt = (
        select(
            SalesDaily.date,
            Warehouse.name.label("warehouse"),
            func.sum(SalesDaily.revenue).label("revenue"),
            func.sum(SalesDaily.receipts_count).label("receipts"),
        )
        .join(Warehouse, Warehouse.id == SalesDaily.warehouse_id)
        .where(SalesDaily.date >= start)
        .group_by(SalesDaily.date, Warehouse.name)
        .order_by(SalesDaily.date.asc(), Warehouse.name.asc())
    )
    rows = session.execute(stmt).fetchall()
    return [
        {
            "date": r.date.isoformat(),
            "warehouse": r.warehouse,
            "revenue": float(r.revenue or 0),
            "receipts": int(r.receipts or 0),
        }
        for r in rows
    ]

def get_margin_daily(session: Session, days: int = 60) -> List[Dict[str, Any]]:
    start = date.today() - timedelta(days=days)
    stmt = (
        select(
            SalesDaily.date,
            Warehouse.name.label("warehouse"),
            func.sum(SalesDaily.revenue).label("revenue"),
            func.sum(SalesDaily.cost).label("cost"),
            func.sum(SalesDaily.discount).label("discount"),
            func.sum(SalesDaily.revenue - SalesDaily.cost).label("gross_profit"),
        )
        .join(Warehouse, Warehouse.id == SalesDaily.warehouse_id)
        .where(SalesDaily.date >= start)
        .group_by(SalesDaily.date, Warehouse.name)
        .order_by(SalesDaily.date.asc(), Warehouse.name.asc())
    )
    rows = session.execute(stmt).fetchall()
    out = []
    for r in rows:
        revenue = float(r.revenue or 0)
        cost = float(r.cost or 0)
        discount = float(r.discount or 0)
        gp = float(r.gross_profit or (revenue - cost))
        margin = (gp / revenue * 100.0) if revenue else 0.0
        out.append({
            "date": r.date.isoformat(),
            "warehouse": r.warehouse,
            "revenue": revenue,
            "cost": cost,
            "discount": discount,
            "gross_profit": gp,
            "margin_pct": margin,
        })
    return out

def get_inflow_daily(session: Session, days: int = 60) -> List[Dict[str, Any]]:
    start = date.today() - timedelta(days=days)
    stmt = (
        select(
            SalesDaily.date,
            Warehouse.name.label("warehouse"),
            func.sum(SalesDaily.inflow_cost).label("inflow"),
        )
        .join(Warehouse, Warehouse.id == SalesDaily.warehouse_id)
        .where(SalesDaily.date >= start)
        .group_by(SalesDaily.date, Warehouse.name)
        .order_by(SalesDaily.date.asc(), Warehouse.name.asc())
    )
    rows = session.execute(stmt).fetchall()
    return [
        {
            "date": r.date.isoformat(),
            "warehouse": r.warehouse,
            "inflow": float(r.inflow or 0),
        }
        for r in rows
    ]

# ==== Универсальная сводка для произвольного периода ====

Granularity = Literal["day", "month", "year"]

def _date_trunc(granularity: Granularity):
    mapping = {"day": "day", "month": "month", "year": "year"}
    return func.date_trunc(mapping[granularity], SalesDaily.date).label("period")

def _aggregate_stmt(granularity: Granularity, start: date, end: date, warehouse_id: Optional[int]):
    period = _date_trunc(granularity)
    stmt = (
        select(
            period,
            func.sum(SalesDaily.revenue).label("revenue"),
            func.sum(SalesDaily.cost).label("cost"),
            func.sum(SalesDaily.discount).label("discount"),
            func.sum(SalesDaily.returns_cost).label("returns_cost"),
            func.sum(SalesDaily.inflow_cost).label("inflow_cost"),
            func.sum(SalesDaily.receipts_count).label("receipts"),
        )
        .where(SalesDaily.date >= start, SalesDaily.date <= end)
        .group_by(period)
        .order_by(period.asc())
    )
    if warehouse_id:
        stmt = stmt.where(SalesDaily.warehouse_id == warehouse_id)
    return stmt

def _sum_stmt(start: date, end: date, warehouse_id: Optional[int]):
    stmt = (
        select(
            func.sum(SalesDaily.revenue).label("revenue"),
            func.sum(SalesDaily.cost).label("cost"),
            func.sum(SalesDaily.discount).label("discount"),
            func.sum(SalesDaily.returns_cost).label("returns_cost"),
            func.sum(SalesDaily.inflow_cost).label("inflow_cost"),
            func.sum(SalesDaily.receipts_count).label("receipts"),
        )
        .where(SalesDaily.date >= start, SalesDaily.date <= end)
    )
    if warehouse_id:
        stmt = stmt.where(SalesDaily.warehouse_id == warehouse_id)
    return stmt

def _as_float(x): return float(x or 0)

def _period_compare(session: Session, start: date, end: date, warehouse_id: Optional[int]) -> Dict[str, Any]:
    length = (end - start).days + 1
    prev_end = start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=length-1)
    prev_year_start = date(start.year - 1, start.month, start.day)
    prev_year_end = date(end.year - 1, end.month, end.day)

    cur = session.execute(_sum_stmt(start, end, warehouse_id)).first()
    prev = session.execute(_sum_stmt(prev_start, prev_end, warehouse_id)).first()
    yoy = session.execute(_sum_stmt(prev_year_start, prev_year_end, warehouse_id)).first()

    def pack(row):
        return {
            "revenue": _as_float(row.revenue),
            "cost": _as_float(row.cost),
            "discount": _as_float(row.discount),
            "returns_cost": _as_float(row.returns_cost),
            "inflow_cost": _as_float(row.inflow_cost),
            "receipts": int(row.receipts or 0),
        }

    return {
        "current": pack(cur),
        "previous": pack(prev),
        "previous_year": pack(yoy),
    }

def get_summary(
    session: Session,
    start: date,
    end: date,
    granularity: Granularity = "day",
    warehouse_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Возвращает:
      - series: агрегаты по периодам (day/month/year)
      - totals: суммы за текущий период
      - compare: суммы за предыдущий период и за прошлый год, для сравнения
    """
    stmt = _aggregate_stmt(granularity, start, end, warehouse_id)
    rows = session.execute(stmt).fetchall()
    series = []
    for r in rows:
        rev = _as_float(r.revenue)
        cost = _as_float(r.cost)
        gp = rev - cost
        margin = (gp / rev * 100.0) if rev else 0.0
        series.append({
            "period": r.period.date().isoformat(),
            "revenue": rev,
            "cost": cost,
            "discount": _as_float(r.discount),
            "returns_cost": _as_float(r.returns_cost),
            "inflow_cost": _as_float(r.inflow_cost),
            "receipts": int(r.receipts or 0),
            "gross_profit": gp,
            "margin_pct": margin,
        })

    totals_row = session.execute(_sum_stmt(start, end, warehouse_id)).first()
    totals = {
        "revenue": _as_float(totals_row.revenue),
        "cost": _as_float(totals_row.cost),
        "discount": _as_float(totals_row.discount),
        "returns_cost": _as_float(totals_row.returns_cost),
        "inflow_cost": _as_float(totals_row.inflow_cost),
        "receipts": int(totals_row.receipts or 0),
    }
    totals["gross_profit"] = totals["revenue"] - totals["cost"]
    totals["margin_pct"] = (totals["gross_profit"] / totals["revenue"] * 100.0) if totals["revenue"] else 0.0

    compare = _period_compare(session, start, end, warehouse_id)

    return {"series": series, "totals": totals, "compare": compare}
