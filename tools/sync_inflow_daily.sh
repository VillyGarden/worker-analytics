#!/usr/bin/env bash
set -euo pipefail
APP_DIR="/root/worker-analytics"
PY="$APP_DIR/.venv/bin/python"; [ -x "$PY" ] || PY="$(command -v python3)"

# даты: по умолчанию последние 3 дня [D-2..D]
START="${1:-$(date -d '2 days ago' +%F)}"
END="${2:-$(date +%F)}"

export PYTHONPATH="$APP_DIR"

echo "== inflow enter positions: $START .. $END =="
cur="$START"
while [ "$(date -d "$cur" +%s)" -le "$(date -d "$END" +%s)" ]; do
  DAY="$cur" "$PY" -m app.tools.load_enter_day
  echo "[ok] $cur"
  cur="$(date -I -d "$cur + 1 day")"
done
