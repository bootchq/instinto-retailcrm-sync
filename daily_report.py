from __future__ import annotations

"""
Ежедневный автоматический отчёт по работе менеджеров.

Анализирует изменения в метриках и отправляет отчёт.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from dateutil import parser as dtparser

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
        return float(str(v))
    except Exception:
        return None


def _to_int(v: Any) -> Optional[int]:
    try:
        if v is None or v == "":
            return None
        return int(float(str(v)))
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


def _calculate_change(current: Optional[float], previous: Optional[float]) -> Optional[float]:
    """Вычисляет изменение в процентах."""
    if current is None or previous is None:
        return None
    if previous == 0:
        return None
    return ((current - previous) / previous) * 100


def _format_change(change: Optional[float]) -> str:
    """Форматирует изменение для отображения."""
    if change is None:
        return "N/A"
    if change > 0:
        return f"+{change:.1f}%"
    return f"{change:.1f}%"


def _get_emoji(change: Optional[float], threshold: float = 5.0) -> str:
    """Возвращает эмодзи в зависимости от изменения."""
    if change is None:
        return "⚪"
    if change >= threshold:
        return "🟢"
    elif change >= 0:
        return "🟡"
    elif change >= -threshold:
        return "🟠"
    return "🔴"


def main() -> None:
    base = Path(__file__).resolve().parent
    env = _load_env(base / "env")
    
    ss = open_spreadsheet(
        spreadsheet_id=env["GOOGLE_SHEETS_ID"],
        service_account_json_path=env["GOOGLE_SERVICE_ACCOUNT_JSON"],
    )
    
    print("Читаю данные из Google Sheets...")
    
    # Читаем текущие метрики
    try:
        spin_metrics = _read_table(ss.worksheet("spin_manager_metrics"))
        manager_summary = _read_table(ss.worksheet("manager_summary"))
    except Exception as e:
        print(f"Ошибка чтения данных: {e}")
        return
    
    # Читаем историю (если есть)
    history_file = base / "daily_metrics_history.json"
    previous_metrics: Dict[str, Dict[str, Any]] = {}
    
    if history_file.exists():
        try:
            with open(history_file, "r", encoding="utf-8") as f:
                previous_metrics = json.load(f)
        except Exception:
            pass
    
    today = datetime.now().strftime("%Y-%m-%d")
    
    # Формируем отчёт
    report_rows: List[Dict[str, Any]] = []
    current_metrics: Dict[str, Dict[str, Any]] = {}
    
    for spin_stat in spin_metrics:
        manager_id = str(spin_stat.get("manager_id", ""))
        manager_name = str(spin_stat.get("manager_name", ""))
        
        if not manager_id or not manager_name:
            continue
        
        # Находим данные менеджера в manager_summary
        mgr_summary = next(
            (m for m in manager_summary if str(m.get("manager_id", "")) == manager_id),
            None
        )
        
        # Текущие метрики
        current = {
            "date": today,
            "manager_id": manager_id,
            "manager_name": manager_name,
            "spin_completeness": _to_float(spin_stat.get("avg_spin_completeness", 0)) or 0,
            "spin_s_rate": _to_float(spin_stat.get("s_usage_rate", 0)) or 0,
            "spin_p_rate": _to_float(spin_stat.get("p_usage_rate", 0)) or 0,
            "spin_i_rate": _to_float(spin_stat.get("i_usage_rate", 0)) or 0,
            "spin_n_rate": _to_float(spin_stat.get("n_usage_rate", 0)) or 0,
            "avg_questions": _to_float(spin_stat.get("avg_questions_per_chat", 0)) or 0,
            "total_chats": _to_int(spin_stat.get("total_chats", 0)) or 0,
        }
        
        if mgr_summary:
            current["response_rate"] = _to_float(mgr_summary.get("response_rate", 0)) or 0
            median_sec = _to_int(mgr_summary.get("median_first_reply_sec"))
            current["median_response_min"] = (median_sec / 60) if median_sec else None
            current["no_reply_rate"] = (
                (_to_int(mgr_summary.get("no_reply_chats", 0)) or 0) /
                (_to_int(mgr_summary.get("chats", 0)) or 1) * 100
            ) if mgr_summary.get("chats") else 0
        
        current_metrics[manager_id] = current
        
        # Предыдущие метрики
        prev = previous_metrics.get(manager_id, {})
        
        # Вычисляем изменения
        changes = {
            "spin_completeness": _calculate_change(
                current["spin_completeness"],
                _to_float(prev.get("spin_completeness"))
            ),
            "spin_s_rate": _calculate_change(
                current["spin_s_rate"],
                _to_float(prev.get("spin_s_rate"))
            ),
            "spin_p_rate": _calculate_change(
                current["spin_p_rate"],
                _to_float(prev.get("spin_p_rate"))
            ),
            "spin_i_rate": _calculate_change(
                current["spin_i_rate"],
                _to_float(prev.get("spin_i_rate"))
            ),
            "spin_n_rate": _calculate_change(
                current["spin_n_rate"],
                _to_float(prev.get("spin_n_rate"))
            ),
            "avg_questions": _calculate_change(
                current["avg_questions"],
                _to_float(prev.get("avg_questions"))
            ),
        }
        
        if "response_rate" in current:
            changes["response_rate"] = _calculate_change(
                current["response_rate"],
                _to_float(prev.get("response_rate"))
            )
        
        if "median_response_min" in current and current["median_response_min"]:
            prev_median = _to_float(prev.get("median_response_min"))
            if prev_median:
                # Для времени ответа: отрицательное изменение = улучшение
                changes["median_response_min"] = _calculate_change(
                    prev_median,
                    current["median_response_min"]
                )
        
        if "no_reply_rate" in current:
            prev_no_reply = _to_float(prev.get("no_reply_rate"))
            if prev_no_reply:
                # Для процента без ответа: отрицательное изменение = улучшение
                changes["no_reply_rate"] = _calculate_change(
                    prev_no_reply,
                    current["no_reply_rate"]
                )
        
        # Формируем строку отчёта
        report_rows.append({
            "date": today,
            "manager_name": manager_name,
            "manager_id": manager_id,
            "spin_completeness": f"{current['spin_completeness']:.1f}%",
            "spin_completeness_change": _format_change(changes.get("spin_completeness")),
            "spin_completeness_emoji": _get_emoji(changes.get("spin_completeness")),
            "spin_s_rate": f"{current['spin_s_rate']:.1f}%",
            "spin_s_change": _format_change(changes.get("spin_s_rate")),
            "spin_p_rate": f"{current['spin_p_rate']:.1f}%",
            "spin_p_change": _format_change(changes.get("spin_p_rate")),
            "spin_i_rate": f"{current['spin_i_rate']:.1f}%",
            "spin_i_change": _format_change(changes.get("spin_i_rate")),
            "spin_n_rate": f"{current['spin_n_rate']:.1f}%",
            "spin_n_change": _format_change(changes.get("spin_n_rate")),
            "avg_questions": f"{current['avg_questions']:.2f}",
            "avg_questions_change": _format_change(changes.get("avg_questions")),
            "response_rate": f"{current.get('response_rate', 0):.1f}%" if current.get('response_rate') else "N/A",
            "response_rate_change": _format_change(changes.get("response_rate")),
            "median_response_min": f"{current.get('median_response_min', 0):.1f}" if current.get('median_response_min') else "N/A",
            "median_response_change": _format_change(changes.get("median_response_min")),
            "no_reply_rate": f"{current.get('no_reply_rate', 0):.1f}%" if current.get('no_reply_rate') is not None else "N/A",
            "no_reply_change": _format_change(changes.get("no_reply_rate")),
            "total_chats": current["total_chats"],
        })
    
    # Сохраняем текущие метрики в историю
    with open(history_file, "w", encoding="utf-8") as f:
        json.dump(current_metrics, f, indent=2, ensure_ascii=False)
    
    # Записываем в Google Sheets
    print("Записываю ежедневный отчёт в Google Sheets...")
    
    upsert_worksheet(
        ss,
        "daily_report",
        rows=dicts_to_table(
            report_rows,
            header=[
                "date", "manager_name", "manager_id",
                "spin_completeness", "spin_completeness_change", "spin_completeness_emoji",
                "spin_s_rate", "spin_s_change",
                "spin_p_rate", "spin_p_change",
                "spin_i_rate", "spin_i_change",
                "spin_n_rate", "spin_n_change",
                "avg_questions", "avg_questions_change",
                "response_rate", "response_rate_change",
                "median_response_min", "median_response_change",
                "no_reply_rate", "no_reply_change",
                "total_chats",
            ],
        ),
    )
    
    # Выводим краткий отчёт в консоль
    print("\n" + "="*80)
    print(f"ЕЖЕДНЕВНЫЙ ОТЧЁТ - {today}")
    print("="*80 + "\n")
    
    for row in report_rows:
        name = row["manager_name"]
        print(f"\n{name}:")
        print(f"  SPIN Полнота: {row['spin_completeness']} {row['spin_completeness_change']} {row['spin_completeness_emoji']}")
        print(f"  Этап S: {row['spin_s_rate']} {row['spin_s_change']}")
        print(f"  Этап P: {row['spin_p_rate']} {row['spin_p_change']}")
        print(f"  Этап I: {row['spin_i_rate']} {row['spin_i_change']}")
        print(f"  Этап N: {row['spin_n_rate']} {row['spin_n_change']}")
        print(f"  Вопросов/чат: {row['avg_questions']} {row['avg_questions_change']}")
        if row.get('response_rate') != "N/A":
            print(f"  Response Rate: {row['response_rate']} {row['response_rate_change']}")
        if row.get('median_response_min') != "N/A":
            print(f"  Медианное время ответа: {row['median_response_min']} мин {row['median_response_change']}")
        if row.get('no_reply_rate') != "N/A":
            print(f"  Чатов без ответа: {row['no_reply_rate']} {row['no_reply_change']}")
    
    print("\n✅ Ежедневный отчёт сохранён в Google Sheets (лист 'daily_report')")


if __name__ == "__main__":
    main()




