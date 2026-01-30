#!/bin/bash

# Скрипт для пакетной выгрузки чатов с промежуточным сохранением
# Обрабатывает по 500 чатов за раз и записывает в Google Sheets

cd "/Users/noor/Documents/Obsidian Vault/Cursor/INSTINTO/Анализ работы менеджеров/retailcrm_chats"

# Активируем виртуальное окружение
source .venv/bin/activate

# Запускаем выгрузку с пакетной обработкой
# BATCH_SIZE=500 означает обработку по 500 чатов за раз
python export_to_sheets.py

echo ""
echo "✅ Выгрузка завершена!"
echo "   Проверьте Google Sheets для просмотра результатов"

