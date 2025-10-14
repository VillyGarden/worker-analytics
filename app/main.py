from fastapi import FastAPI, Request, Form, status, Depends, Query
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from datetime import datetime, date, timedelta
import httpx

from .config import settings
from .db import get_session
from .models import Warehouse, SalesDaily
from .api import get_revenue_daily, get_margin_daily, get_inflow_daily, get_summary
try:
    from sqlalchemy import text as _sa_text
except Exception:  # на всякий случай (юнит-тесты/линтер без SA)
    _sa_text = lambda x: x


def is_public(path: str) -> bool:

    return False

def _is_public_path(path: str) -> bool:
    # публичные ручки, доступные без авторизации
    allow = (
        "/openapi.json",
        "/docs",
        "/redoc",
        "/health/db",
        "/api/inflow/items",
        "/api/top/products",
        "/api/top/warehouses",
        "/api/top/products_v2",
    )
    return any(path.startswith(a) for a in allow)
def _is_public_path(path: str) -> bool:
    # публичные ручки, доступные без авторизации
    allow = (
        "/openapi.json",
        "/docs",
        "/redoc",
        "/health/db",
        "/api/inflow/items",
        "/api/top/products",
        "/api/top/warehouses",
        "/api/top/products_v2",
    )
    return any(path.startswith(a) for a in allow)
app = FastAPI(title="Worker Analytics")
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

WHITELIST_EXACT = {"/login", "/health"}
WHITELIST_PREFIXES = ("/static", "/favicon.ico")

from sqlalchemy import text as _sqlalchemy_text

_sa_text = None

class AuthRequiredMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = (request.url.path or '/').rstrip('/') or '/'
        try:
            user = request.session.get('user')
        except Exception as e:
            user = None
            print('AUTHDBG_ERR', {'err': str(e)})

        # public allowlist — пропускаем без авторизации
        if _is_public_path(path):
            return await call_next(request)

        # компактный лог
        print('AUTHDBG', {'path': path, 'user': user})

        WHITELIST_EXACT = {'/login', '/health'}
        WHITELIST_PREFIXES = ('/static', '/favicon.ico', '/openapi.json', '/docs', '/redoc')

        if path == '/login' and user:
            return RedirectResponse('/', status_code=302)
        if (path in WHITELIST_EXACT) or any(path.startswith(p) for p in WHITELIST_PREFIXES):
            return await call_next(request)
        if not user:
            return RedirectResponse('/login', status_code=302)

        return await call_next(request)

@app.get("/login", response_class=HTMLResponse)
def login_form():
    return """
    <h2>Вход</h2>
    <form method="post" action="/login">
      <input type="text" name="username" placeholder="Логин"/><br/>
      <input type="password" name="password" placeholder="Пароль"/><br/>
      <button type="submit">Войти</button>
    </form>
    """

@app.post("/login")
def login(username: str = Form(...), password: str = Form(...), request: Request = None):
    if username == settings.ADMIN_USERNAME and password == settings.ADMIN_PASSWORD:
        request.session["user"] = username
        return RedirectResponse(url="/dashboard", status_code=302)
    return HTMLResponse("<p>Неверный логин или пароль</p><a href='/login'>Попробовать снова</a>", status_code=401)

@app.get("/")
def root(request: Request):
    if request.session.get("user"):
        return RedirectResponse(url="/dashboard", status_code=302)
    return RedirectResponse(url="/login", status_code=302)

@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=302)

@app.get("/health")
def health():
    return {"status": "ok"}

# ===== API: существующие =====

@app.get("/api/revenue/daily")
def api_revenue_daily(days: int = Query(60, ge=1, le=365), session: Session = Depends(get_session)):
    try:
        data = get_revenue_daily(session, days=days)
        return {"data": data}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/api/margin/daily")
def api_margin_daily(days: int = Query(60, ge=1, le=365), session: Session = Depends(get_session)):
    try:
        data = get_margin_daily(session, days=days)
        return {"data": data}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/api/inflow/daily")
def api_inflow_daily(days: int = Query(60, ge=1, le=365), session: Session = Depends(get_session)):
    try:
        data = get_inflow_daily(session, days=days)
        return {"data": data}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/api/warehouses")
def api_warehouses(session: Session = Depends(get_session)):
    rows = session.query(Warehouse.id, Warehouse.name).order_by(Warehouse.name.asc()).all()
    return {"data": [{"id": r.id, "name": r.name} for r in rows]}

@app.get("/api/summary")
def api_summary(
    request: Request,
    start: str,
    end: str,
    group: str = "day",
    warehouse_id: int | None = None,
    session: Session = Depends(get_session),
):
    try:
        start_d = datetime.strptime(start, "%Y-%m-%d").date()
        end_d = datetime.strptime(end, "%Y-%m-%d").date()
        data = get_summary(session, start=start_d, end=end_d, granularity=group, warehouse_id=warehouse_id)
        return data
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# ===== Новые API: ТОП складов (из БД) и ТОП товаров (из МоегоСклада) =====

@app.get("/api/top/warehouses")
def api_top_warehouses(
    start: str,
    end: str,
    limit: int = Query(5, ge=1, le=20),
    session: Session = Depends(get_session),
):
    s = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end, "%Y-%m-%d").date()
    stmt = (
        select(
            Warehouse.name.label("warehouse"),
            func.sum(SalesDaily.revenue).label("revenue"),
            func.sum(SalesDaily.cost).label("cost"),
            func.sum(SalesDaily.receipts_count).label("checks"),
        )
        .join(Warehouse, Warehouse.id == SalesDaily.warehouse_id)
        .where(SalesDaily.date >= s, SalesDaily.date <= e)
        .group_by(Warehouse.name)
        .order_by(func.sum(SalesDaily.revenue).desc())
        .limit(limit)
    )
    rows = session.execute(stmt).fetchall()
    out = []
    for r in rows:
        rev = float(r.revenue or 0)
        cost = float(r.cost or 0)
        gp = rev - cost
        out.append({
            "warehouse": r.warehouse,
            "revenue": rev,
            "gross_profit": gp,
            "margin_pct": (gp / rev * 100.0) if rev else 0.0,
            "checks": int(r.checks or 0),
            "avg_ticket": (rev / int(r.checks)) if int(r.checks or 0) else 0.0,
        })
    return {"data": out}

@app.get("/api/top/products")
def api_top_products(
    start: str,
    end: str,
    warehouse_id: int | None = None,
    limit: int = Query(10, ge=1, le=50),
    session: Session = Depends(get_session),
):
    # если указан склад, получаем его ms_id
    store_ms_id = None
    if warehouse_id:
        w = session.query(Warehouse).filter(Warehouse.id == warehouse_id).first()
        if not w:
            return JSONResponse(status_code=400, content={"error": "warehouse_id not found"})
        store_ms_id = w.ms_id

    MS_BASE = settings.MS_BASE_URL.rstrip("/")
    headers = {
        "Authorization": f"Bearer {settings.MS_API_TOKEN}",
        "Accept": "application/json;charset=utf-8",
        "Content-Type": "application/json",
        "User-Agent": "worker-analytics/top-products",
    }
    params = {
        "momentFrom": f"{start} 00:00:00",
        "momentTo": f"{end} 23:59:59",
        "limit": 1000,
    }
    if store_ms_id:
        params["filter"] = f"store={MS_BASE}/entity/store/{store_ms_id}"

    with httpx.Client(timeout=60.0, headers=headers) as c:
        r = c.get(f"{MS_BASE}/report/profit/byproduct", params=params)
        r.raise_for_status()
        data = r.json()

    agg = {}
    for row in (data.get("rows") or []):
        name = None
        code = None
        try:
            ass = row.get("assortment") or {}
            name = ass.get("name") or "Без названия"
            code = ass.get("code") or ""
        except Exception:
            name = "Без названия"
        key = f"{name} {('['+code+']') if code else ''}".strip()
        ssum = float(row.get("sellSum", 0) or 0) / 100.0
        scost = float(row.get("sellCostSum", 0) or 0) / 100.0
        qty = float(row.get("sellQuantity", 0) or 0)
        ret_cost = float(row.get("returnCostSum", 0) or 0) / 100.0
        price = float(row.get("sellPrice", 0) or 0) / 100.0
        disc = price * qty - ssum
        a = agg.setdefault(key, {"name": key, "revenue": 0.0, "cost": 0.0, "qty": 0.0, "discount": 0.0, "returns_cost": 0.0})
        a["revenue"] += ssum
        a["cost"] += scost
        a["qty"] += qty
        a["discount"] += disc
        a["returns_cost"] += ret_cost

    items = list(agg.values())
    items.sort(key=lambda x: x["revenue"], reverse=True)
    items = items[:limit]
    for it in items:
        it["gross_profit"] = it["revenue"] - it["cost"]
        it["margin_pct"] = (it["gross_profit"] / it["revenue"] * 100.0) if it["revenue"] else 0.0
        it["avg_price"] = (it["revenue"] / it["qty"]) if it["qty"] else 0.0

    return {"data": items}

# ===== Dashboard =====

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    return '''
<!doctype html>
<html lang="ru"><head>
<meta charset="utf-8"/>
<title>Worker Analytics — Дашборд</title>
<script src="/static/plotly.min.js"></script>
<script src="/static/dashboard.js?v=9"></script>
<style>
 body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,"Helvetica Neue",Arial,"Noto Sans","Liberation Sans";margin:24px;background:#0b0c10;color:#e6e6e6}
 h1{font-size:22px;margin:0 0 12px}
 .grid{display:grid;grid-template-columns:repeat(12,1fr);gap:16px}
 .card{background:#14161b;border:1px solid #1f232b;border-radius:14px;padding:16px;box-shadow:0 1px 2px rgba(0,0,0,.25)}
 .muted{color:#9aa0a6;font-size:13px}
 .kpi{font-size:26px;font-weight:700}
 .kpi-label{font-size:12px;color:#9aa0a6}
 .col-3{grid-column:span 3}
 .col-4{grid-column:span 4}
 .col-6{grid-column:span 6}
 .col-12{grid-column:span 12}
 .row{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:0 0 8px}
 input, select, button{background:#0f1116;color:#e6e6e6;border:1px solid #2a2f3a;border-radius:10px;padding:8px 10px}
 button{cursor:pointer}
 .good{color:#2ecc71} .bad{color:#ff6b6b}
 table{width:100%;border-collapse:collapse}
 th,td{padding:8px 10px;border-bottom:1px solid #242a34;text-align:right}
 th:first-child, td:first-child{text-align:left}
 .btn{padding:6px 10px;border-radius:10px;border:1px solid #2a2f3a;background:#0f1116}
 .btn:hover{filter:brightness(1.1)}
 .chip{font-size:12px;padding:2px 8px;border-radius:10px;background:#0f1116;border:1px solid #2a2f3a}
</style>
</head>
<body>
  <h1>Дашборд продаж</h1>

  <div class="card col-12">
    <div class="row" style="gap:14px">
      <label>Период:</label>
      <input type="date" id="start">
      <span>–</span>
      <input type="date" id="end">
      <label>Группировка:</label>
      <select id="group">
        <option value="day">День</option>
        <option value="month" selected>Месяц</option>
        <option value="year">Год</option>
      </select>
      <label>Склад:</label>
      <select id="warehouse"><option value="">Все</option></select>
      <button id="apply" class="btn">Применить</button>
      <span class="muted">Быстрые периоды:</span>
      <button class="btn" id="btn-cur-month">Текущий месяц</button>
      <button class="btn" id="btn-prev-month">Прошлый месяц</button>
      <button class="btn" id="btn-ytd">Год к дате</button>
      <button class="btn" id="btn-prev-year">Прошлый год</button>
    </div>
    <div class="row"><span id="compare" class="muted">Загрузите сравнение…</span></div>
    <div id="compare-table"></div>
  </div>

  
  <div class="card col-12">
    <div class="muted">Сравнительный график выручки (текущий период vs выбранные сравнения)</div>
    <div id="chart-compare" style="height:360px"></div>
    <div style="margin-top:8px" class="muted">
      <label><input type="checkbox" id="cmp-prev-period" checked> с предыдущим периодом</label>
      <label style="margin-left:12px"><input type="checkbox" id="cmp-prev-year"> с прошлым годом</label>
    </div>
  </div>
  <div class="grid">
    <div class="card col-3">
      <div class="kpi" id="kpi-rev">—</div>
      <div class="kpi-label">Выручка за 7 дней</div>
    </div>
    <div class="card col-3">
      <div class="kpi" id="kpi-gp">—</div>
      <div class="kpi-label">Валовая прибыль за 7 дней</div>
    </div>
    <div class="card col-3">
      <div class="kpi" id="kpi-margin">—</div>
      <div class="kpi-label">Средняя маржа за 7 дней</div>
    </div>
    <div class="card col-3">
      <div class="kpi" id="kpi-at">—</div>
      <div class="kpi-label">Средний чек за 7 дней</div>
    </div>

    <div class="card col-12">
      <div class="muted">Выручка по дням, разрез по складам (60 дней)</div>
      <div id="chart-revenue" style="height:360px"></div>
    </div>
    <div class="card col-12">
      <div class="muted">Валовая прибыль и маржа по дням, разрез по складам (60 дней)</div>
      <div id="chart-margin" style="height:400px"></div>
    </div>
    <div class="card col-12">
      <div class="muted">Оприходования по дням, разрез по складам (60 дней)</div>
      <div id="chart-inflow" style="height:340px"></div>
    </div>

    <div class="card col-6">
      <div class="muted">Топ складов за период</div>
      <div id="top-warehouses"></div>
    </div>
    <div class="card col-6">
      <div class="muted">Топ товаров за период <span id="tp-scope" class="chip">по всем складам</span></div>
      <div id="top-products"></div>
    </div>
  </div>
</body></html>
'''

@app.exception_handler(StarletteHTTPException)
def not_found_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        if request.session.get("user"):
            return RedirectResponse(url="/dashboard", status_code=302)
        return RedirectResponse(url="/login", status_code=302)
    return HTMLResponse(str(exc.detail), status_code=exc.status_code)

# ===== NEW: Writeoff APIs =====

@app.get("/api/writeoff/daily")
def api_writeoff_daily(
    start: str,
    end: str,
    warehouse_id: int | None = None,
    session: Session = Depends(get_session),
):
    """
    Агрегация списаний по дням из sales_daily:
      total / defect / inventory / other + проценты.
    """
    s = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end, "%Y-%m-%d").date()
    stmt = (
        select(
            SalesDaily.date.label("date"),
            Warehouse.name.label("warehouse"),
            func.sum(SalesDaily.writeoff_cost_total).label("total"),
            func.sum(SalesDaily.writeoff_cost_defect).label("defect"),
            func.sum(SalesDaily.writeoff_cost_inventory).label("inventory"),
            func.sum(SalesDaily.writeoff_cost_other).label("other"),
        )
        .join(Warehouse, Warehouse.id == SalesDaily.warehouse_id)
        .where(SalesDaily.date >= s, SalesDaily.date <= e)
        .group_by(SalesDaily.date, Warehouse.name)
        .order_by(SalesDaily.date.asc(), Warehouse.name.asc())
    )
    if warehouse_id:
        stmt = stmt.where(SalesDaily.warehouse_id == warehouse_id)

    rows = session.execute(stmt).fetchall()
    out = []
    for r in rows:
        total = float(r.total or 0)
        defect = float(r.defect or 0)
        inventory = float(r.inventory or 0)
        other = float(r.other or 0)
        out.append({
            "date": r.date.isoformat(),
            "warehouse": r.warehouse,
            "total": total,
            "defect": defect,
            "inventory": inventory,
            "other": other,
            "defect_pct": (defect/total*100.0) if total else 0.0,
            "inventory_pct": (inventory/total*100.0) if total else 0.0,
            "other_pct": (other/total*100.0) if total else 0.0,
        })
    return {"data": out}

@app.get("/api/writeoff/reasons")
def api_writeoff_reasons(
    start: str,
    end: str,
    warehouse_id: int | None = None,
    session: Session = Depends(get_session),
):
    """
    Разбивка списаний по причинам из writeoff_daily_reason (как в БД, без нормализации).
    """
    from sqlalchemy import text
    s = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end, "%Y-%m-%d").date()

    if warehouse_id:
        sql = text("""
          SELECT date, warehouse_id, reason, SUM(cost) AS cost
          FROM writeoff_daily_reason
          WHERE date BETWEEN :s AND :e AND warehouse_id = :wid
          GROUP BY date, warehouse_id, reason
          ORDER BY date ASC, warehouse_id ASC, reason ASC
        """)
        rows = session.execute(sql, {"s": s, "e": e, "wid": warehouse_id}).fetchall()
    else:
        sql = text("""
          SELECT date, warehouse_id, reason, SUM(cost) AS cost
          FROM writeoff_daily_reason
          WHERE date BETWEEN :s AND :e
          GROUP BY date, warehouse_id, reason
          ORDER BY date ASC, warehouse_id ASC, reason ASC
        """)
        rows = session.execute(sql, {"s": s, "e": e}).fetchall()

    wh_map = {w.id: w.name for w in session.query(Warehouse).all()}
    out = []
    for r in rows:
        out.append({
            "date": r.date.isoformat(),
            "warehouse_id": int(r.warehouse_id),
            "warehouse": wh_map.get(int(r.warehouse_id), f"id={r.warehouse_id}"),
            "reason": r.reason or "",
            "cost": float(r.cost or 0),
        })
    return {"data": out}

@app.get("/api/inflow/items")
def api_inflow_items(
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
    warehouse_id: str = Query(None, description="UUID склада"),
    limit: int = Query(500, ge=1, le=5000),
    session = Depends(get_session),
):
    # Позиции оприходований за период (и опционально по складу)
    q = (
        "SELECT i.date, i.warehouse_id, w.name AS warehouse, "
        "i.product_id, i.qty, i.price, i.cost, i.inventory_based "
        "FROM inflow_item_fact i "
        "JOIN warehouse w ON w.ms_id = i.warehouse_id::text "
        "WHERE i.date BETWEEN :start AND :end "
        "{wh_filter} "
        "ORDER BY i.date DESC "
        "LIMIT :limit"
    )
    wh_filter = ""
    params = {"start": start, "end": end, "limit": limit}
    if warehouse_id:
        wh_filter = "AND i.warehouse_id::text = :wh"
        params["wh"] = warehouse_id
    q = q.format(wh_filter=wh_filter)
    from sqlalchemy import text as _sa_text  # local
    rows = session.execute(_sa_text(q), params).mappings().all()
    return {"data": [dict(r) for r in rows]}

@app.get("/api/inflow/items")
def api_inflow_items(
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
    warehouse_id: str | None = Query(None, description="UUID склада"),
    limit: int = Query(500, ge=1, le=5000),
    session = Depends(get_session),
):
    """
    Позиции оприходований по периоду (и опционально по складу).
    Источник: inflow_item_fact. Денежные поля в рублях.
    """
    q = (
        "SELECT "
        "  i.date, "
        "  i.warehouse_id, "
        "  w.name AS warehouse, "
        "  i.product_id, "
        "  i.qty, "
        "  i.price, "
        "  i.cost, "
        "  i.inventory_based "
        "FROM inflow_item_fact i "
        "JOIN warehouse w ON i.warehouse_id::text = w.ms_id "
        "WHERE i.date BETWEEN :start AND :end "
        "{wh_filter} "
        "ORDER BY i.date DESC "
        "LIMIT :limit"
    )
    wh_filter = ""
    params = {"start": start, "end": end, "limit": limit}
    if warehouse_id:
        wh_filter = "AND i.warehouse_id::text = :wh "
        params["wh"] = warehouse_id
    q = q.format(wh_filter=wh_filter)
    from sqlalchemy import text as _sa_text  # local
    rows = session.execute(_sa_text(q), params).mappings().all()
    return {"data": [dict(r) for r in rows]}


@app.get("/api/top/products_v2")
def api_top_products_v2(
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
    limit: int = Query(20, ge=1, le=1000),
    sort_by: str = Query("revenue", description="revenue|sold_qty|avg_price|inflow_qty|inflow_cost|product_id"),
    order: str = Query("desc", description="asc|desc"),
    min_qty: float = Query(0),
    min_revenue: float = Query(0),
    session = Depends(get_session),
):
    """
    Агрегаты по товарам за период, с фильтрами/сортировкой.
    """
    # Белый список сортируемых полей
    sort_map = {
        "revenue": "revenue",
        "sold_qty": "sold_qty",
        "avg_price": "avg_price",
        "inflow_qty": "inflow_qty",
        "inflow_cost": "inflow_cost",
        "product_id": "product_id",
    }
    sort_col = sort_map.get((sort_by or "").lower(), "revenue")
    ord_kw = "ASC" if (order or "").lower() == "asc" else "DESC"

    # SQL: считаем отдельно продажи и оприходование, потом FULL JOIN, после чего фильтры по агрегатам
    q = (
        "WITH sales AS ("
        "  SELECT product_id::text AS product_id,"
        "         SUM(revenue) AS revenue,"
        "         SUM(qty) AS sold_qty,"
        "         CASE WHEN SUM(qty)=0 THEN 0 ELSE SUM(revenue)/SUM(qty) END AS avg_price "
        "  FROM sales_item_fact "
        "  WHERE date BETWEEN :start AND :end "
        "  GROUP BY 1"
        "), "
        "inflow AS ("
        "  SELECT product_id::text AS product_id,"
        "         SUM(qty) AS inflow_qty,"
        "         SUM(cost) AS inflow_cost "
        "  FROM inflow_item_fact "
        "  WHERE date BETWEEN :start AND :end "
        "  GROUP BY 1"
        ") "
        "SELECT COALESCE(s.product_id, i.product_id) AS product_id, "
        "       COALESCE(s.revenue,0) AS revenue, "
        "       COALESCE(s.sold_qty,0) AS sold_qty, "
        "       COALESCE(s.avg_price,0) AS avg_price, "
        "       COALESCE(i.inflow_qty,0) AS inflow_qty, "
        "       COALESCE(i.inflow_cost,0) AS inflow_cost "
        "FROM sales s FULL JOIN inflow i USING (product_id) "
        "WHERE COALESCE(s.sold_qty,0) >= :min_qty "
        "  AND COALESCE(s.revenue,0)  >= :min_revenue "
        f"ORDER BY {sort_col} {ord_kw} NULLS LAST "
        "LIMIT :limit"
    )

    params = {
        "start": start,
        "end": end,
        "limit": int(limit or 20),
        "min_qty": float(min_qty or 0),
        "min_revenue": float(min_revenue or 0),
    }

    rows = session.execute(_sa_text(q), params).mappings().all()
    return {"data": [dict(r) for r in rows]}
@app.get("/api/top/products_v2")
def api_top_products_v2(
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
    limit: int = Query(20, ge=1, le=1000),
    sort_by: str = Query("revenue"),
    order: str = Query("desc"),
    min_qty: float = Query(0),
    min_revenue: float = Query(0),
    session = Depends(get_session),
):
    """
    Топ товаров за период (продажи + приход), с именем товара и сортировкой.
    """
    # безопасная валидация сортировки
    sort_map = {
        "revenue": "revenue",
        "sold_qty": "sold_qty",
        "avg_price": "avg_price",
        "inflow_qty": "inflow_qty",
        "inflow_cost": "inflow_cost",
        "name": "name"
    }
    col = sort_map.get(sort_by, "revenue")
    ord_sql = "DESC" if str(order).lower() == "desc" else "ASC"
    order_expr = f"{col} {ord_sql}"

    q = """
    WITH sales AS (
      SELECT
        product_id::text AS product_id,
        SUM(revenue) AS revenue,
        SUM(qty) AS sold_qty,
        CASE WHEN SUM(qty)=0 THEN 0 ELSE SUM(revenue)/SUM(qty) END AS avg_price
      FROM sales_item_fact
      WHERE date BETWEEN :start AND :end
      GROUP BY 1
    ),
    inflow AS (
      SELECT
        product_id::text AS product_id,
        SUM(qty) AS inflow_qty,
        SUM(cost) AS inflow_cost
      FROM inflow_item_fact
      WHERE date BETWEEN :start AND :end
      GROUP BY 1
    ),
    merged AS (
      SELECT
        COALESCE(s.product_id, i.product_id) AS product_id,
        COALESCE(s.revenue, 0)      AS revenue,
        COALESCE(s.sold_qty, 0)     AS sold_qty,
        COALESCE(s.avg_price, 0)    AS avg_price,
        COALESCE(i.inflow_qty, 0)   AS inflow_qty,
        COALESCE(i.inflow_cost, 0)  AS inflow_cost
      FROM sales s
      FULL JOIN inflow i USING (product_id)
    )
    SELECT
      m.product_id,
      p.name,
      ROUND(m.revenue::numeric, 2)       AS revenue,
      m.sold_qty,
      ROUND(m.avg_price::numeric, 2)     AS avg_price,
      m.inflow_qty,
      ROUND(m.inflow_cost::numeric, 2)   AS inflow_cost
    FROM merged m
    LEFT JOIN product p ON p.ms_id::uuid = m.product_id::uuid
    WHERE m.sold_qty >= :min_qty AND m.revenue >= :min_rev
    ORDER BY {order_expr}
    LIMIT :limit
    """.format(order_expr=order_expr)

    global _sa_text
    if _sa_text is None:
        try:
            _sa_text = _sqlalchemy_text  # типизированный импорт, который у нас уже есть
        except Exception:
            from sqlalchemy import text as _sa_text  # запасной путь

    params = {
        "start": start,
        "end": end,
        "limit": limit,
        "min_qty": min_qty,
        "min_rev": min_revenue,
    }
    rows = session.execute(_sa_text(q), params).mappings().all()
    # приводим к dict + ensure float
    out = []
    for r in rows:
        d = dict(r)
        # на всякий случай float-каст денег
        for k in ("revenue", "avg_price", "inflow_cost"):
            if d.get(k) is not None:
                d[k] = float(d[k])
        if d.get("inflow_qty") is not None:
            d["inflow_qty"] = float(d["inflow_qty"])
        if d.get("sold_qty") is not None:
            d["sold_qty"] = float(d["sold_qty"])
        out.append(d)
    return {"data": out}

@app.get("/api/top/products_v3")
def api_top_products_v3(
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
    limit: int = Query(20, ge=1, le=1000),
    sort_by: str = Query("revenue", description="revenue|sold_qty|avg_price|inflow_qty|inflow_cost|product_id"),
    order: str = Query("desc", description="asc|desc"),
    min_qty: float = Query(0, ge=0),
    min_revenue: float = Query(0, ge=0),
    session = Depends(get_session),
):
    # локальный импорт sqlalchemy.text — не зависит от глобального _sa_text
    try:
        from sqlalchemy import text as sa_text  # type: ignore
    except Exception:
        from sqlalchemy.sql import text as sa_text  # type: ignore
    """
    Топ товаров за период с агрегацией продаж и поступлений.
    """
    # валидация сортировки
    allowed_cols = {"revenue","sold_qty","avg_price","inflow_qty","inflow_cost","product_id"}
    sort_col = sort_by if sort_by in allowed_cols else "revenue"
    sort_dir = "ASC" if str(order).lower() == "asc" else "DESC"

    q = (
        "WITH sales AS ("
        "  SELECT product_id::text AS product_id,"
        "         SUM(revenue) AS revenue,"
        "         SUM(qty) AS sold_qty,"
        "         CASE WHEN SUM(qty)=0 THEN 0 ELSE SUM(revenue)/SUM(qty) END AS avg_price "
        "  FROM sales_item_fact "
        "  WHERE date BETWEEN :start AND :end "
        "  GROUP BY 1"
        "), "
        "inflow AS ("
        "  SELECT product_id::text AS product_id,"
        "         SUM(qty) AS inflow_qty,"
        "         SUM(cost) AS inflow_cost "
        "  FROM inflow_item_fact "
        "  WHERE date BETWEEN :start AND :end "
        "  GROUP BY 1"
        ") "
        "SELECT "
        "  COALESCE(s.product_id, i.product_id) AS product_id, "
        "  COALESCE(s.revenue,0) AS revenue, "
        "  COALESCE(s.sold_qty,0) AS sold_qty, "
        "  COALESCE(s.avg_price,0) AS avg_price, "
        "  COALESCE(i.inflow_qty,0) AS inflow_qty, "
        "  COALESCE(i.inflow_cost,0) AS inflow_cost "
        "FROM sales s "
        "FULL JOIN inflow i USING (product_id) "
        "WHERE COALESCE(s.revenue,0) >= :min_revenue "
        "  AND COALESCE(s.sold_qty,0) >= :min_qty "
        f"ORDER BY {sort_col} {sort_dir} NULLS LAST "
        "LIMIT :limit"
    )
    params = {
        "start": start,
        "end": end,
        "limit": limit,
        "min_revenue": min_revenue,
        "min_qty": min_qty,
    }
    rows = session.execute(sa_text(q), params).mappings().all()
    return {"data": [dict(r) for r in rows]}

@app.get("/api/top/products_v3")
def api_top_products_v3(
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
    limit: int = Query(20, ge=1, le=1000),
    sort_by: str = Query("revenue", description="revenue|sold_qty|avg_price|inflow_qty|inflow_cost|product_id"),
    order: str = Query("desc", description="asc|desc"),
    min_qty: float = Query(0, ge=0),
    min_revenue: float = Query(0, ge=0),
    session = Depends(get_session),
):
    """
    Топ товаров за период: продажи + поступления (FULL JOIN по product_id).
    """
    # локальный импорт на случай отсутствия глобального
    try:
        from sqlalchemy import text as _sa_text  # type: ignore
    except Exception:
        from sqlalchemy.sql import text as _sa_text  # type: ignore

    # валидация сортировки/направления
    allowed_cols = {"revenue","sold_qty","avg_price","inflow_qty","inflow_cost","product_id"}
    sort_col = sort_by if sort_by in allowed_cols else "revenue"
    sort_dir = "ASC" if str(order).lower() == "asc" else "DESC"

    q = (
        "WITH sales AS ("
        "  SELECT product_id::text AS product_id, "
        "         SUM(revenue) AS revenue, "
        "         SUM(qty) AS sold_qty, "
        "         CASE WHEN SUM(qty)=0 THEN 0 ELSE SUM(revenue)/SUM(qty) END AS avg_price "
        "  FROM sales_item_fact "
        "  WHERE date BETWEEN :start AND :end "
        "  GROUP BY 1"
        "), "
        "inflow AS ("
        "  SELECT product_id::text AS product_id, "
        "         SUM(qty) AS inflow_qty, "
        "         SUM(cost) AS inflow_cost "
        "  FROM inflow_item_fact "
        "  WHERE date BETWEEN :start AND :end "
        "  GROUP BY 1"
        ") "
        "SELECT "
        "  COALESCE(s.product_id, i.product_id) AS product_id, "
        "  COALESCE(s.revenue,0) AS revenue, "
        "  COALESCE(s.sold_qty,0) AS sold_qty, "
        "  COALESCE(s.avg_price,0) AS avg_price, "
        "  COALESCE(i.inflow_qty,0) AS inflow_qty, "
        "  COALESCE(i.inflow_cost,0) AS inflow_cost "
        "FROM sales s "
        "FULL JOIN inflow i USING (product_id) "
        "WHERE COALESCE(s.revenue,0) >= :min_revenue "
        "  AND COALESCE(s.sold_qty,0) >= :min_qty "
        f"ORDER BY {sort_col} {sort_dir} NULLS LAST "
        "LIMIT :limit"
    )
    params = {
        "start": start,
        "end": end,
        "limit": limit,
        "min_revenue": float(min_revenue),
        "min_qty": float(min_qty),
    }
    try:
        rows = session.execute(_sa_text(q), params).mappings().all()
        return {"data": [dict(r) for r in rows]}
    except Exception as e:
        # отдадим понятный текст ошибки, чтобы её видно было в curl
        return {"error": str(e)}

