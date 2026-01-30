"""
Детальный анализ структуры всех листов Google Sheets.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from sheets import open_spreadsheet


def _load_env(env_path: Path) -> Dict[str, str]:
    env: Dict[str, str] = {}
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def analyze_sheet(ws) -> Dict[str, Any]:
    """Анализирует структуру листа."""
    values = ws.get_all_values()
    
    if not values:
        return {
            "rows": 0,
            "cols": 0,
            "header": [],
            "data_rows": 0,
            "sample_data": [],
        }
    
    header = values[0] if values else []
    data_rows = values[1:] if len(values) > 1 else []
    
    # Примеры данных (первые 3 строки)
    sample_data = []
    for i, row in enumerate(data_rows[:3], 1):
        sample_data.append({
            "row_num": i + 1,
            "values": row[:10] if len(row) > 10 else row,  # Первые 10 колонок
        })
    
    return {
        "rows": len(values),
        "cols": len(header) if header else 0,
        "header": header,
        "data_rows": len(data_rows),
        "sample_data": sample_data,
    }


def main() -> None:
    base = Path(__file__).resolve().parent
    env = _load_env(base / "env")
    
    ss = open_spreadsheet(
        spreadsheet_id=env["GOOGLE_SHEETS_ID"],
        service_account_json_path=env["GOOGLE_SERVICE_ACCOUNT_JSON"],
    )
    
    worksheets = ss.worksheets()
    
    print("=" * 80)
    print("ДЕТАЛЬНЫЙ АНАЛИЗ ВСЕХ ЛИСТОВ")
    print("=" * 80)
    print()
    
    analysis_results = []
    
    for ws in worksheets:
        if ws.title == "Лист1" and ws.row_count == 1:
            continue  # Пропускаем пустой лист
        
        print(f"📄 {ws.title}")
        print("-" * 80)
        
        analysis = analyze_sheet(ws)
        analysis_results.append({
            "name": ws.title,
            **analysis
        })
        
        print(f"Строк данных: {analysis['data_rows']}")
        print(f"Колонок: {analysis['cols']}")
        print(f"\nКолонки ({len(analysis['header'])}):")
        for i, col in enumerate(analysis['header'], 1):
            print(f"  {i}. {col}")
        
        if analysis['sample_data']:
            print(f"\nПримеры данных:")
            for sample in analysis['sample_data']:
                print(f"  Строка {sample['row_num']}: {', '.join(str(v)[:40] for v in sample['values'][:5])}")
        
        print()
        print("=" * 80)
        print()
    
    # Сохраняем результаты
    output_file = base / "sheets_analysis_report.md"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("# Анализ структуры Google Sheets\n\n")
        f.write(f"Всего листов: {len(analysis_results)}\n\n")
        
        for result in analysis_results:
            f.write(f"## {result['name']}\n\n")
            f.write(f"- Строк данных: {result['data_rows']}\n")
            f.write(f"- Колонок: {result['cols']}\n")
            f.write(f"- Колонки: {', '.join(result['header'])}\n\n")
    
    print(f"✅ Отчёт сохранён в {output_file}")


if __name__ == "__main__":
    main()

