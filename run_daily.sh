#!/bin/bash

# Ежедневный запуск анализа работы менеджеров

cd "/Users/noor/Documents/Obsidian Vault/Cursor/INSTINTO/Анализ работы менеджеров/retailcrm_chats"

# Активируем виртуальное окружение
source .venv/bin/activate

# Запускаем ежедневный отчёт и отправку в Telegram
python telegram_daily_report.py

# Выход
exit 0




