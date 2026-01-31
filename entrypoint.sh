#!/bin/bash
# Универсальный entrypoint для Railway
# Читает переменную RUN_SCRIPT и запускает соответствующий скрипт

SCRIPT="${RUN_SCRIPT:-export_to_sheets.py}"
echo "Запускаю: python $SCRIPT"
exec python "$SCRIPT"
