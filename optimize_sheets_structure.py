"""
Этап 1: Оптимизация существующих листов.

1. Объединить behavior_snapshot_managers + history_behavior_managers + weekly_behavior_delta_managers → behavior_history_managers
2. Объединить manager_summary в manager_report
3. Удалить chat_order_payment
4. Удалить Лист1
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from sheets import open_spreadsheet, upsert_worksheet, dicts_to_table


def _load_env(env_path: Path) -> Dict[str, str]:
    env: Dict[str, str] = {}
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def _read_table(ws) -> List[Dict[str, Any]]:
    """Читает таблицу из листа."""
    values = ws.get_all_values()
    if not values:
        return []
    header = values[0]
    out: List[Dict[str, Any]] = []
    for row in values[1:]:
        d: Dict[str, Any] = {}
        for i, h in enumerate(header):
            d[h] = row[i] if i < len(row) else ""
        out.append(d)
    return out


def merge_behavior_sheets(ss) -> None:
    """Объединяет 3 листа с поведенческими метриками в один."""
    print("\n📊 Шаг 1: Объединение листов с поведенческими метриками...")
    
    all_data: List[Dict[str, Any]] = []
    
    # Читаем данные из всех трёх листов
    sheets_to_merge = [
        "behavior_snapshot_managers",
        "history_behavior_managers",
        "weekly_behavior_delta_managers",
    ]
    
    for sheet_name in sheets_to_merge:
        try:
            ws = ss.worksheet(sheet_name)
            data = _read_table(ws)
            print(f"   ✅ Загружено {len(data)} строк из '{sheet_name}'")
            all_data.extend(data)
        except Exception as e:
            print(f"   ⚠️ Ошибка при чтении '{sheet_name}': {e}")
    
    if not all_data:
        print("   ⚠️ Нет данных для объединения")
        return
    
    # Определяем все уникальные колонки
    all_columns = set()
    for row in all_data:
        all_columns.update(row.keys())
    
    # Сортируем колонки (run_ts первым, затем остальные)
    sorted_columns = []
    if "run_ts" in all_columns:
        sorted_columns.append("run_ts")
    for col in sorted(all_columns):
        if col != "run_ts":
            sorted_columns.append(col)
    
    # Сортируем данные по дате (run_ts)
    all_data.sort(key=lambda x: x.get("run_ts", ""), reverse=True)
    
    print(f"   📝 Всего строк для объединения: {len(all_data)}")
    print(f"   📋 Колонок: {len(sorted_columns)}")
    
    # Создаём объединённый лист
    rows = dicts_to_table(all_data, header=sorted_columns)
    upsert_worksheet(ss, "behavior_history_managers", rows=rows)
    
    print(f"   ✅ Создан лист 'behavior_history_managers' с {len(all_data)} строками")
    
    # Удаляем старые листы
    for sheet_name in sheets_to_merge:
        try:
            ws = ss.worksheet(sheet_name)
            ss.del_worksheet(ws)
            print(f"   🗑️ Удалён лист '{sheet_name}'")
        except Exception as e:
            print(f"   ⚠️ Ошибка при удалении '{sheet_name}': {e}")


def merge_manager_summary(ss) -> None:
    """Объединяет manager_summary в manager_report."""
    print("\n📊 Шаг 2: Объединение manager_summary в manager_report...")
    
    try:
        # Читаем manager_summary
        summary_ws = ss.worksheet("manager_summary")
        summary_data = _read_table(summary_ws)
        
        if not summary_data:
            print("   ⚠️ manager_summary пуст, пропускаем")
            return
        
        print(f"   ✅ Загружено {len(summary_data)} строк из 'manager_summary'")
        
        # Читаем manager_report
        report_ws = ss.worksheet("manager_report")
        report_data = _read_table(report_ws)
        
        print(f"   ✅ manager_report содержит {len(report_data)} строк")
        
        # Проверяем, есть ли данные из manager_summary в manager_report
        # Если manager_report уже содержит все данные, просто удаляем manager_summary
        if len(report_data) > 0:
            print("   ℹ️ manager_report уже содержит данные, удаляем manager_summary")
            ss.del_worksheet(summary_ws)
            print("   🗑️ Удалён лист 'manager_summary'")
        else:
            # Если manager_report пуст, копируем данные из manager_summary
            print("   ℹ️ manager_report пуст, копируем данные из manager_summary")
            summary_header = summary_ws.get_all_values()[0] if summary_ws.get_all_values() else []
            rows = dicts_to_table(summary_data, header=summary_header)
            upsert_worksheet(ss, "manager_report", rows=rows)
            ss.del_worksheet(summary_ws)
            print("   ✅ Данные скопированы, удалён лист 'manager_summary'")
            
    except Exception as e:
        print(f"   ⚠️ Ошибка: {e}")


def delete_unnecessary_sheets(ss) -> None:
    """Удаляет ненужные листы."""
    print("\n🗑️ Шаг 3: Удаление ненужных листов...")
    
    sheets_to_delete = ["chat_order_payment", "Лист1"]
    
    for sheet_name in sheets_to_delete:
        try:
            ws = ss.worksheet(sheet_name)
            ss.del_worksheet(ws)
            print(f"   ✅ Удалён лист '{sheet_name}'")
        except Exception as e:
            print(f"   ⚠️ Лист '{sheet_name}' не найден или ошибка: {e}")


def main() -> None:
    base = Path(__file__).resolve().parent
    env = _load_env(base / "env")
    
    ss = open_spreadsheet(
        spreadsheet_id=env["GOOGLE_SHEETS_ID"],
        service_account_json_path=env["GOOGLE_SERVICE_ACCOUNT_JSON"],
    )
    
    print("=" * 60)
    print("ЭТАП 1: ОПТИМИЗАЦИЯ СУЩЕСТВУЮЩИХ ЛИСТОВ")
    print("=" * 60)
    
    # Шаг 1: Объединить поведенческие метрики
    merge_behavior_sheets(ss)
    
    # Шаг 2: Объединить manager_summary в manager_report
    merge_manager_summary(ss)
    
    # Шаг 3: Удалить ненужные листы
    delete_unnecessary_sheets(ss)
    
    print("\n" + "=" * 60)
    print("✅ ЭТАП 1 ЗАВЕРШЁН!")
    print("=" * 60)
    
    # Проверяем результат
    print("\n📋 Проверка результата:")
    worksheets = ss.worksheets()
    print(f"   Всего листов: {len(worksheets)}")
    print(f"   Список листов:")
    for i, ws in enumerate(worksheets, 1):
        values = ws.get_all_values()
        row_count = len(values) - 1 if len(values) > 0 else 0
        print(f"     {i}. {ws.title} ({row_count} строк)")


if __name__ == "__main__":
    main()

