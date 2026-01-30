from __future__ import annotations

"""
Ежедневный отчёт в Telegram-бот.

Отправляет ежедневный отчёт с метриками менеджеров и уведомления об ухудшении.
"""

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from daily_report import main as generate_daily_report
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


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(str(v).replace("%", "").replace("+", ""))
    except Exception:
        return None


def send_telegram_message(token: str, chat_id: str, text: str, max_retries: int = 3) -> bool:
    """Отправляет сообщение в Telegram."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    
    for attempt in range(max_retries):
        try:
            response = requests.post(
                url,
                json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
                timeout=10,
            )
            if response.status_code == 200:
                return True
            time.sleep(2 ** attempt)
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"Ошибка отправки в Telegram: {e}")
            time.sleep(2 ** attempt)
    
    return False


def format_daily_report(rows: List[Dict[str, Any]]) -> str:
    """Форматирует ежедневный отчёт для Telegram."""
    if not rows:
        return "📊 <b>Ежедневный отчёт</b>\n\nДанные отсутствуют."
    
    text = "📊 <b>Ежедневный отчёт по работе менеджеров</b>\n\n"
    
    for row in rows:
        manager_name = row.get("manager_name", "Неизвестно")
        text += f"👤 <b>{manager_name}</b>\n"
        
        # SPIN метрики
        spin_completeness = row.get("spin_completeness", "N/A")
        spin_change = row.get("spin_completeness_change", "")
        spin_emoji = row.get("spin_completeness_emoji", "⚪")
        text += f"  {spin_emoji} SPIN Полнота: {spin_completeness} {spin_change}\n"
        
        # Этапы SPIN
        s_rate = row.get("spin_s_rate", "N/A")
        s_change = row.get("spin_s_change", "")
        text += f"  S (Ситуационные): {s_rate} {s_change}\n"
        
        p_rate = row.get("spin_p_rate", "N/A")
        p_change = row.get("spin_p_change", "")
        text += f"  P (Проблемные): {p_rate} {p_change}\n"
        
        i_rate = row.get("spin_i_rate", "N/A")
        i_change = row.get("spin_i_change", "")
        text += f"  I (Извлекающие): {i_rate} {i_change}\n"
        
        n_rate = row.get("spin_n_rate", "N/A")
        n_change = row.get("spin_n_change", "")
        text += f"  N (Выгоды): {n_rate} {n_change}\n"
        
        # Вопросы
        avg_questions = row.get("avg_questions", "N/A")
        questions_change = row.get("avg_questions_change", "")
        text += f"  ❓ Вопросов/чат: {avg_questions} {questions_change}\n"
        
        # Скорость ответа
        if row.get("response_rate") != "N/A":
            response_rate = row.get("response_rate", "N/A")
            response_change = row.get("response_rate_change", "")
            text += f"  ⚡ Response Rate: {response_rate} {response_change}\n"
        
        if row.get("median_response_min") != "N/A":
            median = row.get("median_response_min", "N/A")
            median_change = row.get("median_response_change", "")
            text += f"  ⏱ Время ответа: {median} мин {median_change}\n"
        
        text += "\n"
    
    return text


def check_warnings(rows: List[Dict[str, Any]]) -> List[str]:
    """Проверяет метрики на ухудшение и возвращает предупреждения."""
    warnings = []
    
    for row in rows:
        manager_name = row.get("manager_name", "Неизвестно")
        
        # Проверяем изменения метрик
        changes_to_check = [
            ("spin_completeness_change", "SPIN Полнота", -5),
            ("spin_p_change", "Этап P", -3),
            ("spin_n_change", "Этап N", -3),
            ("avg_questions_change", "Количество вопросов", -0.5),
            ("response_rate_change", "Response Rate", -5),
        ]
        
        for change_key, metric_name, threshold in changes_to_check:
            change = _to_float(row.get(change_key, "0"))
            if change is not None and change < threshold:
                warnings.append(
                    f"⚠️ <b>{manager_name}</b>: {metric_name} снизилась на {abs(change):.1f}%"
                )
        
        # Проверяем низкие значения
        spin_completeness = _to_float(row.get("spin_completeness", "0%"))
        if spin_completeness is not None and spin_completeness < 30:
            warnings.append(
                f"🔴 <b>{manager_name}</b>: SPIN Полнота критически низкая ({spin_completeness:.1f}%)"
            )
        
        avg_questions = _to_float(row.get("avg_questions", "0"))
        if avg_questions is not None and avg_questions < 3:
            warnings.append(
                f"🔴 <b>{manager_name}</b>: Мало вопросов ({avg_questions:.2f} на чат)"
            )
    
    return warnings


def main() -> None:
    base = Path(__file__).resolve().parent
    env = _load_env(base / "env")
    
    # Генерируем ежедневный отчёт
    print("Генерирую ежедневный отчёт...")
    generate_daily_report()
    
    # Читаем отчёт
    ss = open_spreadsheet(
        spreadsheet_id=env["GOOGLE_SHEETS_ID"],
        service_account_json_path=env["GOOGLE_SERVICE_ACCOUNT_JSON"],
    )
    
    report_rows = _read_table(ss.worksheet("daily_report"))
    
    # Берем последние записи (сегодня)
    if not report_rows:
        print("Нет данных для отчёта")
        return
    
    # Фильтруем по сегодняшней дате
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")
    today_rows = [r for r in report_rows if r.get("date") == today]
    
    if not today_rows:
        # Берем последние записи
        today_rows = report_rows[-2:] if len(report_rows) >= 2 else report_rows
    
    # Форматируем отчёт
    report_text = format_daily_report(today_rows)
    
    # Проверяем предупреждения
    warnings = check_warnings(today_rows)
    
    # Отправляем в Telegram
    telegram_token = env.get("TELEGRAM_BOT_TOKEN")
    telegram_chat_id = env.get("TELEGRAM_CHAT_ID")
    
    if not telegram_token or not telegram_chat_id:
        print("⚠️ TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не настроены")
        print("\nОтчёт:")
        print(report_text)
        if warnings:
            print("\n⚠️ Предупреждения:")
            for w in warnings:
                print(f"  {w}")
        return
    
    # Отправляем основной отчёт
    print("Отправляю отчёт в Telegram...")
    send_telegram_message(telegram_token, telegram_chat_id, report_text)
    
    # Отправляем предупреждения отдельным сообщением
    if warnings:
        warnings_text = "⚠️ <b>Предупреждения</b>\n\n" + "\n".join(warnings)
        send_telegram_message(telegram_token, telegram_chat_id, warnings_text)
        print(f"Отправлено {len(warnings)} предупреждений")
    
    print("✅ Ежедневный отчёт отправлен в Telegram")


if __name__ == "__main__":
    main()




