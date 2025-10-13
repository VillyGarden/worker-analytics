#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/root/worker-analytics"
cd "$APP_DIR"

# Подтягиваем .env и экспортируем переменные
set -a
[ -f .env ] && . .env
set +a

# Нормализуем токен: и MS_API_TOKEN, и MS_TOKEN (на всякий)
if [[ -n "${MS_API_TOKEN:-}" ]]; then
  export MS_TOKEN="$MS_API_TOKEN"
fi

# День берём из аргумента или "вчера"
DAY="${1:-$(date -d 'yesterday' +%F)}"

echo "[daily] DAY=$DAY"

# 1) Оприходования (enter) -> inflow_item_fact
PY="$APP_DIR/.venv/bin/python"; [ -x "$PY" ] || PY="$(command -v python3)"
PYTHONPATH="$APP_DIR" DAY="$DAY" "$PY" -m app.tools.load_enter_day

# 2) Продажи (retaildemand) -> sales_item_fact
tools/load_retail_day.sh "$DAY"

echo "[daily] done for $DAY"
