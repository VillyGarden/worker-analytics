#!/usr/bin/env bash
set -euo pipefail
cd /root/worker-analytics

# экспортируем переменные из .env
set -a
[ -f .env ] && . .env
set +a

# нормализуем токен (на всякий)
if [[ -n "${MS_API_TOKEN:-}" ]]; then export MS_TOKEN="$MS_API_TOKEN"; fi

DAY="$(date -d 'yesterday' +%F)"
echo "[$(date '+%F %T')] run daily_sync for $DAY"
tools/daily_sync.sh "$DAY"
echo "[$(date '+%F %T')] done $DAY"
