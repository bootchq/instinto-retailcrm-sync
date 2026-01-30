#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="/Users/noor/Documents/Obsidian Vault/Cursor/INSTINTO/Анализ работы менеджеров/retailcrm_chats"

cd "$BASE_DIR"

source ".venv/bin/activate"

# Не даём Mac уйти в сон во время прогона (если ноут открыт).
# Если крышка закрыта и Mac уходит в sleep — задача всё равно будет поставлена на паузу.
/usr/bin/caffeinate -dimsu -t 21600 bash -lc "
  python export_to_sheets.py
  python behavior_digest.py
  python weekly_digest.py
  python send_weekly_telegram.py
  python sheet_audit.py
"

