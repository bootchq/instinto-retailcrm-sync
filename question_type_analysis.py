from __future__ import annotations

"""
Анализ типов вопросов: открытые vs закрытые.

Открытые вопросы (кто, что, где, когда, почему, как) - лучше для выявления потребностей
Закрытые вопросы (да/нет, выбор из вариантов) - хуже для понимания клиента
"""

import re
from pathlib import Path
from typing import Any, Dict, List

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


# Паттерны для открытых вопросов
_OPEN_QUESTION_PATTERNS = [
    r"\b(какой|какая|какие|как|сколько|когда|куда|где|откуда|зачем|почему|что|кто|кому|для кого)\b",
    r"\b(расскажите|подскажите|объясните|опишите|уточните)\b",
    r"\b(как вы|что вы|где вы|когда вы)\b",
]

# Паттерны для закрытых вопросов
_CLOSED_QUESTION_PATTERNS = [
    r"\b(да|нет|верно|правильно|так|не так)\s*\?",
    r"\b(вам подходит|вам нравится|вам удобно|вам подойдёт)\s*\?",
    r"\b(это|этот|эта|эти)\s+(вам|подходит|нравится)\s*\?",
    r"\b(вы|вам)\s+(нужен|нужна|нужно|подходит|нравится)\s*\?",
]


def is_open_question(text: str) -> bool:
    """Определяет, является ли вопрос открытым."""
    if not text or "?" not in text:
        return False
    text_lower = text.lower()
    return any(re.search(pattern, text_lower, re.IGNORECASE) for pattern in _OPEN_QUESTION_PATTERNS)


def is_closed_question(text: str) -> bool:
    """Определяет, является ли вопрос закрытым."""
    if not text or "?" not in text:
        return False
    text_lower = text.lower()
    return any(re.search(pattern, text_lower, re.IGNORECASE) for pattern in _CLOSED_QUESTION_PATTERNS)


def main() -> None:
    base = Path(__file__).resolve().parent
    env = _load_env(base / "env")
    
    ss = open_spreadsheet(
        spreadsheet_id=env["GOOGLE_SHEETS_ID"],
        service_account_json_path=env["GOOGLE_SERVICE_ACCOUNT_JSON"],
    )
    
    print("Читаю данные из Google Sheets...")
    
    messages = _read_table(ss.worksheet("messages_raw"))
    chats = _read_table(ss.worksheet("chats_raw"))
    
    print(f"Загружено: {len(messages)} сообщений, {len(chats)} чатов")
    
    # Группируем сообщения по чатам и менеджерам
    manager_questions: Dict[str, Dict[str, Any]] = {}
    
    for msg in messages:
        if msg.get("direction") != "out":
            continue
        
        manager_id = str(msg.get("manager_id", ""))
        manager_name = str(msg.get("manager_name", ""))
        text = str(msg.get("text", ""))
        
        if not manager_id or not text:
            continue
        
        key = f"{manager_id}|{manager_name}"
        if key not in manager_questions:
            manager_questions[key] = {
                "manager_id": manager_id,
                "manager_name": manager_name,
                "total_questions": 0,
                "open_questions": 0,
                "closed_questions": 0,
                "other_questions": 0,
            }
        
        if "?" in text:
            manager_questions[key]["total_questions"] += 1
            
            if is_open_question(text):
                manager_questions[key]["open_questions"] += 1
            elif is_closed_question(text):
                manager_questions[key]["closed_questions"] += 1
            else:
                manager_questions[key]["other_questions"] += 1
    
    # Формируем результаты
    results = []
    for key, data in manager_questions.items():
        total = data["total_questions"]
        if total == 0:
            continue
        
        open_rate = (data["open_questions"] / total * 100) if total > 0 else 0
        closed_rate = (data["closed_questions"] / total * 100) if total > 0 else 0
        
        results.append({
            "manager_id": data["manager_id"],
            "manager_name": data["manager_name"],
            "total_questions": total,
            "open_questions": data["open_questions"],
            "closed_questions": data["closed_questions"],
            "other_questions": data["other_questions"],
            "open_questions_rate": f"{open_rate:.1f}%",
            "closed_questions_rate": f"{closed_rate:.1f}%",
            "recommendation": "Увеличить долю открытых вопросов до ≥70%" if open_rate < 70 else "Хорошо",
        })
    
    # Записываем результаты
    print("\nЗаписываю анализ типов вопросов в Google Sheets...")
    
    upsert_worksheet(
        ss,
        "question_type_analysis",
        rows=dicts_to_table(
            results,
            header=[
                "manager_id", "manager_name", "total_questions",
                "open_questions", "closed_questions", "other_questions",
                "open_questions_rate", "closed_questions_rate", "recommendation",
            ],
        ),
    )
    
    # Выводим результаты
    print("\n" + "="*80)
    print("АНАЛИЗ ТИПОВ ВОПРОСОВ")
    print("="*80 + "\n")
    
    for r in results:
        print(f"{r['manager_name']}:")
        print(f"  Всего вопросов: {r['total_questions']}")
        print(f"  Открытые: {r['open_questions']} ({r['open_questions_rate']})")
        print(f"  Закрытые: {r['closed_questions']} ({r['closed_questions_rate']})")
        print(f"  Рекомендация: {r['recommendation']}")
        print()
    
    print("✅ Анализ завершён! Результаты записаны в Google Sheets (лист 'question_type_analysis')")


if __name__ == "__main__":
    main()




