from __future__ import annotations

"""
Анализ связи SPIN-метрик с конверсией (заказами).

Анализирует, какие этапы SPIN и какие метрики сильнее всего влияют на конверсию.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

from sheets import dicts_to_table, open_spreadsheet, upsert_worksheet


def _load_env(env_path: Path) -> Dict[str, str]:
    env: Dict[str, str] = {}
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(str(v).replace("%", ""))
    except Exception:
        return None


def _read_table(ws) -> List[Dict[str, Any]]:
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


def main() -> None:
    base = Path(__file__).resolve().parent
    env = _load_env(base / "env")
    
    ss = open_spreadsheet(
        spreadsheet_id=env["GOOGLE_SHEETS_ID"],
        service_account_json_path=env["GOOGLE_SERVICE_ACCOUNT_JSON"],
    )
    
    print("Читаю данные из Google Sheets...")
    
    chats = _read_table(ss.worksheet("chats_raw"))
    spin_metrics = _read_table(ss.worksheet("spin_chat_metrics"))
    
    print(f"Загружено: {len(chats)} чатов, {len(spin_metrics)} SPIN-метрик")
    
    # Создаём словарь SPIN-метрик по chat_id
    spin_by_chat: Dict[str, Dict[str, Any]] = {}
    for m in spin_metrics:
        chat_id = str(m.get("chat_id", ""))
        if chat_id:
            spin_by_chat[chat_id] = m
    
    # Анализируем связь с конверсией
    chats_with_order = []
    chats_without_order = []
    
    for chat in chats:
        chat_id = str(chat.get("chat_id", ""))
        order_id = str(chat.get("order_id", ""))
        has_order = bool(order_id and order_id.strip())
        
        spin_data = spin_by_chat.get(chat_id)
        if not spin_data:
            continue
        
        chat_data = {
            "chat_id": chat_id,
            "manager_name": str(chat.get("manager_name", "")),
            "spin_completeness": _to_float(spin_data.get("spin_completeness", "0%")) or 0,
            "has_situation": spin_data.get("has_situation") == "Да",
            "has_problem": spin_data.get("has_problem") == "Да",
            "has_implication": spin_data.get("has_implication") == "Да",
            "has_need_payoff": spin_data.get("has_need_payoff") == "Да",
            "spin_s_count": _to_float(spin_data.get("spin_s_count", 0)) or 0,
            "spin_p_count": _to_float(spin_data.get("spin_p_count", 0)) or 0,
            "spin_i_count": _to_float(spin_data.get("spin_i_count", 0)) or 0,
            "spin_n_count": _to_float(spin_data.get("spin_n_count", 0)) or 0,
            "total_questions": _to_float(spin_data.get("total_questions", 0)) or 0,
            "total_messages": _to_float(spin_data.get("total_messages", 0)) or 0,
        }
        
        if has_order:
            chats_with_order.append(chat_data)
        else:
            chats_without_order.append(chat_data)
    
    print(f"\nЧатов с заказами (конверсия): {len(chats_with_order)}")
    print(f"Чатов без заказов: {len(chats_without_order)}")
    
    if not chats_with_order:
        print("\n⚠️ Не найдено чатов с заказами. Проверьте, есть ли order_id в chats_raw.")
        return
    
    # Считаем средние значения для каждой группы
    def calc_avg(data_list: List[Dict[str, Any]], key: str) -> float:
        values = [d.get(key, 0) for d in data_list if d.get(key) is not None]
        return sum(values) / len(values) if values else 0
    
    def calc_rate(data_list: List[Dict[str, Any]], key: str) -> float:
        return sum(1 for d in data_list if d.get(key)) / len(data_list) * 100 if data_list else 0
    
    analysis_rows = []
    
    metrics_to_compare = [
        ("spin_completeness", "Полнота SPIN-цикла (%)", "avg"),
        ("has_situation", "Использование этапа S (%)", "rate"),
        ("has_problem", "Использование этапа P (%)", "rate"),
        ("has_implication", "Использование этапа I (%)", "rate"),
        ("has_need_payoff", "Использование этапа N (%)", "rate"),
        ("spin_s_count", "Количество S-вопросов", "avg"),
        ("spin_p_count", "Количество P-вопросов", "avg"),
        ("spin_i_count", "Количество I-вопросов", "avg"),
        ("spin_n_count", "Количество N-вопросов", "avg"),
        ("total_questions", "Общее количество вопросов", "avg"),
        ("total_messages", "Общее количество сообщений", "avg"),
    ]
    
    for metric_key, metric_name, calc_type in metrics_to_compare:
        if calc_type == "avg":
            with_order = calc_avg(chats_with_order, metric_key)
            without_order = calc_avg(chats_without_order, metric_key)
        else:  # rate
            with_order = calc_rate(chats_with_order, metric_key)
            without_order = calc_rate(chats_without_order, metric_key)
        
        difference = with_order - without_order
        impact = "ВЫСОКИЙ" if abs(difference) > 20 else "СРЕДНИЙ" if abs(difference) > 10 else "НИЗКИЙ"
        
        analysis_rows.append({
            "metric_name": metric_name,
            "with_order": f"{with_order:.2f}",
            "without_order": f"{without_order:.2f}",
            "difference": f"{difference:.2f}",
            "impact": impact,
            "recommendation": "КРИТИЧЕСКИ ВАЖНО" if abs(difference) > 20 else "ВАЖНО" if abs(difference) > 10 else "ЖЕЛАТЕЛЬНО",
        })
    
    # Записываем результаты
    print("\nЗаписываю анализ связи SPIN с конверсией в Google Sheets...")
    
    upsert_worksheet(
        ss,
        "spin_conversion_correlation",
        rows=dicts_to_table(
            analysis_rows,
            header=["metric_name", "with_order", "without_order", "difference", "impact", "recommendation"],
        ),
    )
    
    # Выводим результаты
    print("\n" + "="*80)
    print("АНАЛИЗ СВЯЗИ SPIN-МЕТРИК С КОНВЕРСИЕЙ")
    print("="*80 + "\n")
    
    print(f"Чатов с заказами: {len(chats_with_order)}")
    print(f"Чатов без заказов: {len(chats_without_order)}")
    print(f"Общая конверсия: {len(chats_with_order) / (len(chats_with_order) + len(chats_without_order)) * 100:.1f}%")
    
    print("\nСРАВНЕНИЕ МЕТРИК:")
    print("-" * 80)
    print(f"{'Метрика':<40} | {'С заказом':<15} | {'Без заказа':<15} | {'Разница':<10} | {'Влияние'}")
    print("-" * 80)
    
    for row in analysis_rows:
        print(f"{row['metric_name']:<40} | {row['with_order']:<15} | {row['without_order']:<15} | {row['difference']:<10} | {row['impact']}")
    
    print("\n✅ Анализ завершён! Результаты записаны в Google Sheets (лист 'spin_conversion_correlation')")
    print("\nРЕКОМЕНДАЦИИ:")
    print("Метрики с ВЫСОКИМ влиянием на конверсию нужно улучшать в первую очередь!")


if __name__ == "__main__":
    main()




