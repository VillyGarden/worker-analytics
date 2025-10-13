#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/root/worker-analytics}"
PY="$APP_DIR/.venv/bin/python"; [ -x "$PY" ] || PY="$(command -v python3)"

START="${1:-2019-01-01}"
END="${2:-$(date +%F)}"

LOG_DIR="$APP_DIR/logs"
mkdir -p "$LOG_DIR"
OK_LOG="$LOG_DIR/inflow_backfill.ok"
ERR_LOG="$LOG_DIR/inflow_backfill.err"
touch "$OK_LOG" "$ERR_LOG"

echo "== Backfill inflow enter positions =="
echo "Range: $START .. $END"
echo "OK log:  $OK_LOG"
echo "ERR log: $ERR_LOG"

# сравнение дат через epoch (надёжнее, чем строками)
ds() { date -d "$1" +%s; }

d="$START"
while [ "$(ds "$d")" -le "$(ds "$END")" ]; do
  if grep -qx "$d" "$OK_LOG"; then
    printf "[skip] %s already done\n" "$d"
    d="$(date -d "$d +1 day" +%F)"
    continue
  fi

  printf "[run ] %s ... " "$d"
  if START="$d" END="$d" PYTHONPATH="$APP_DIR" "$PY" -m app.tools.load_enter_day >/dev/null 2>&1; then
    echo "$d" >> "$OK_LOG"
    echo "ok"
  else
    echo "$d" >> "$ERR_LOG"
    echo "FAIL"
  fi

  sleep 0.3
  d="$(date -d "$d +1 day" +%F)"
done

echo "== Done. OK: $(wc -l < "$OK_LOG")  ERR: $(wc -l < "$ERR_LOG")"
