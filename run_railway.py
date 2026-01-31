#!/usr/bin/env python3
"""
Railway runner для автоматического запуска скриптов анализа.

Настройка расписания через Railway Scheduler или cron.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Добавляем текущую директорию в путь
sys.path.insert(0, str(Path(__file__).parent))

# Загружаем переменные окружения из Railway
# Railway автоматически предоставляет все переменные через os.environ

def main():
    """Главная функция запуска."""
    
    # Проверяем, какой скрипт нужно запустить
    script = os.environ.get("RAILWAY_SCRIPT", "export_to_sheets")
    
    print(f"🚀 Запуск скрипта: {script}")
    print(f"📁 Рабочая директория: {os.getcwd()}")
    
    if script == "export_to_sheets":
        from export_to_sheets import main as export_main
        export_main()
    elif script == "incremental_export":
        # Инкрементальная выгрузка для частых запусков (каждые 15 мин)
        # Смотрит последние 7 дней, добавляет только новые чаты
        os.environ.setdefault("LAST_DAYS", "7")
        from complete_export import main as incremental_main
        incremental_main()
    elif script == "update_order_payment_only":
        from update_order_payment_only import main as update_main
        update_main()
    elif script == "spin_analysis":
        from spin_analysis import main as spin_main
        spin_main()
    elif script == "weekly_metrics_analysis":
        from weekly_metrics_analysis import main as weekly_main
        weekly_main()
    elif script == "telegram_daily_report":
        from telegram_daily_report import main as telegram_main
        telegram_main()
    elif script == "run_all":
        # Запускаем все анализы последовательно
        print("📊 Запуск всех анализов...")
        
        print("\n1️⃣ Обновление данных о заказах...")
        from update_order_payment_only import main as update_main
        update_main()
        
        print("\n2️⃣ SPIN-анализ...")
        from spin_analysis import main as spin_main
        spin_main()
        
        print("\n3️⃣ Топ-5 проблем...")
        from weekly_metrics_analysis import main as weekly_main
        weekly_main()
        
        print("\n4️⃣ Отправка отчёта в Telegram...")
        from telegram_daily_report import main as telegram_main
        telegram_main()
        
        print("\n✅ Все анализы завершены!")
    else:
        print(f"❌ Неизвестный скрипт: {script}")
        print("Доступные скрипты:")
        print("  - export_to_sheets")
        print("  - incremental_export  (каждые 15 мин)")
        print("  - update_order_payment_only")
        print("  - spin_analysis")
        print("  - weekly_metrics_analysis")
        print("  - telegram_daily_report")
        print("  - run_all")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

