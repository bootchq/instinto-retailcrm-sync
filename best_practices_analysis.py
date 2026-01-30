from __future__ import annotations

"""
Анализ лучших практик менеджеров.

Находит чаты с высокой конверсией и анализирует:
- Какие фразы/скрипты использовались
- Какая логика ведения диалога
- Какие этапы SPIN использовались
- Создаёт готовые скрипты на основе лучших практик
"""

import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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


@dataclass
class BestChat:
    """Лучший чат с анализом."""
    chat_id: str
    manager_name: str
    has_order: bool
    spin_completeness: float
    questions_count: int
    messages: List[str]
    spin_stages: Dict[str, bool]
    key_phrases: List[str]


def analyze_best_chats(
    chats: List[Dict[str, Any]],
    messages: List[Dict[str, Any]],
    spin_metrics: List[Dict[str, Any]],
) -> List[BestChat]:
    """Анализирует лучшие чаты (с заказами и хорошим SPIN)."""
    
    # Группируем сообщения по чатам
    messages_by_chat: Dict[str, List[Dict[str, Any]]] = {}
    for msg in messages:
        chat_id = str(msg.get("chat_id", ""))
        if chat_id:
            messages_by_chat.setdefault(chat_id, []).append(msg)
    
    # Создаём словарь SPIN-метрик
    spin_by_chat: Dict[str, Dict[str, Any]] = {}
    for m in spin_metrics:
        chat_id = str(m.get("chat_id", ""))
        if chat_id:
            spin_by_chat[chat_id] = m
    
    best_chats: List[BestChat] = []
    
    for chat in chats:
        chat_id = str(chat.get("chat_id", ""))
        order_id = str(chat.get("order_id", ""))
        manager_name = str(chat.get("manager_name", ""))
        
        if not chat_id:
            continue
        
        # Ищем чаты с заказами (конверсия) и хорошим SPIN
        has_order = bool(order_id and order_id.strip())
        spin_data = spin_by_chat.get(chat_id, {})
        spin_completeness = _to_float(spin_data.get("spin_completeness", "0%").replace("%", "")) or 0
        
        # Берём чаты с заказом и полнотой SPIN > 50%
        if has_order and spin_completeness >= 50:
            chat_messages = messages_by_chat.get(chat_id, [])
            manager_messages = [
                str(m.get("text", "")) for m in chat_messages
                if m.get("direction") == "out" and str(m.get("manager_id", "")) == str(chat.get("manager_id", ""))
            ]
            
            questions_count = sum(1 for m in manager_messages if "?" in m)
            
            spin_stages = {
                "S": spin_data.get("has_situation") == "Да",
                "P": spin_data.get("has_problem") == "Да",
                "I": spin_data.get("has_implication") == "Да",
                "N": spin_data.get("has_need_payoff") == "Да",
            }
            
            # Извлекаем ключевые фразы
            key_phrases = extract_key_phrases(manager_messages)
            
            best_chats.append(BestChat(
                chat_id=chat_id,
                manager_name=manager_name,
                has_order=has_order,
                spin_completeness=spin_completeness,
                questions_count=questions_count,
                messages=manager_messages[:10],  # Первые 10 сообщений
                spin_stages=spin_stages,
                key_phrases=key_phrases,
            ))
    
    # Сортируем по полноте SPIN
    best_chats.sort(key=lambda x: x.spin_completeness, reverse=True)
    
    return best_chats[:20]  # Топ-20 лучших чатов


def extract_key_phrases(messages: List[str]) -> List[str]:
    """Извлекает ключевые фразы из сообщений."""
    all_text = " ".join(messages).lower()
    
    # Паттерны для хороших фраз
    patterns = [
        r"подскажите[^.!?]*\?",
        r"какой[^.!?]*\?",
        r"что[^.!?]*\?",
        r"как[^.!?]*\?",
        r"для кого[^.!?]*\?",
        r"когда[^.!?]*\?",
        r"какие[^.!?]*\?",
        r"что не устраивает[^.!?]*\?",
        r"как это поможет[^.!?]*\?",
        r"что это даст[^.!?]*\?",
    ]
    
    phrases: List[str] = []
    for pattern in patterns:
        matches = re.findall(pattern, all_text, re.IGNORECASE)
        phrases.extend(matches[:3])  # Максимум 3 примера каждого типа
    
    return phrases[:15]  # Топ-15 фраз


def generate_scripts(best_chats: List[BestChat]) -> Dict[str, List[Dict[str, Any]]]:
    """Генерирует скрипты на основе лучших практик."""
    
    scripts_by_stage: Dict[str, List[Dict[str, Any]]] = {
        "greeting": [],
        "situation": [],
        "problem": [],
        "implication": [],
        "need_payoff": [],
        "closing": [],
    }
    
    # Анализируем фразы по этапам
    all_phrases = []
    for chat in best_chats:
        all_phrases.extend(chat.key_phrases)
    
    # Группируем по типам
    situation_phrases = [p for p in all_phrases if any(word in p.lower() for word in ["какой", "какая", "сколько", "когда", "где", "для кого"])]
    problem_phrases = [p for p in all_phrases if any(word in p.lower() for word in ["не устраивает", "проблем", "сложност", "не нравится"])]
    implication_phrases = [p for p in all_phrases if any(word in p.lower() for word in ["приводит", "влияет", "будет если"])]
    need_payoff_phrases = [p for p in all_phrases if any(word in p.lower() for word in ["поможет", "даст", "зачем", "выгода"])]
    
    # Создаём скрипты
    scripts_by_stage["greeting"] = [
        {
            "script": "Здравствуйте! Подскажите, пожалуйста, какой размер вам нужен?",
            "usage_count": sum(1 for p in situation_phrases if "размер" in p.lower()),
            "stage": "S",
        },
        {
            "script": "Добрый день! Для кого выбираете?",
            "usage_count": sum(1 for p in situation_phrases if "для кого" in p.lower()),
            "stage": "S",
        },
    ]
    
    scripts_by_stage["situation"] = [
        {
            "script": "Подскажите, какой размер вам нужен?",
            "usage_count": sum(1 for p in situation_phrases if "размер" in p.lower()),
            "stage": "S",
        },
        {
            "script": "Для кого выбираете?",
            "usage_count": sum(1 for p in situation_phrases if "для кого" in p.lower()),
            "stage": "S",
        },
        {
            "script": "Когда планируете использовать?",
            "usage_count": sum(1 for p in situation_phrases if "когда" in p.lower()),
            "stage": "S",
        },
    ]
    
    scripts_by_stage["problem"] = [
        {
            "script": "Что не устраивает в текущем белье?",
            "usage_count": sum(1 for p in problem_phrases if "не устраивает" in p.lower()),
            "stage": "P",
        },
        {
            "script": "Какие сложности возникают при выборе?",
            "usage_count": sum(1 for p in problem_phrases if "сложност" in p.lower()),
            "stage": "P",
        },
    ]
    
    scripts_by_stage["implication"] = [
        {
            "script": "К чему это приводит?",
            "usage_count": sum(1 for p in implication_phrases if "приводит" in p.lower()),
            "stage": "I",
        },
        {
            "script": "Как это влияет на вас?",
            "usage_count": sum(1 for p in implication_phrases if "влияет" in p.lower()),
            "stage": "I",
        },
    ]
    
    scripts_by_stage["need_payoff"] = [
        {
            "script": "Как это поможет вам?",
            "usage_count": sum(1 for p in need_payoff_phrases if "поможет" in p.lower()),
            "stage": "N",
        },
        {
            "script": "Что это даст?",
            "usage_count": sum(1 for p in need_payoff_phrases if "даст" in p.lower()),
            "stage": "N",
        },
    ]
    
    scripts_by_stage["closing"] = [
        {
            "script": "Отлично! Оформим заказ?",
            "usage_count": 0,
            "stage": "Closing",
        },
        {
            "script": "Готовы оформить? Отправлю ссылку на оплату.",
            "usage_count": 0,
            "stage": "Closing",
        },
    ]
    
    return scripts_by_stage


def main() -> None:
    base = Path(__file__).resolve().parent
    env = _load_env(base / "env")
    
    ss = open_spreadsheet(
        spreadsheet_id=env["GOOGLE_SHEETS_ID"],
        service_account_json_path=env["GOOGLE_SERVICE_ACCOUNT_JSON"],
    )
    
    print("Читаю данные из Google Sheets...")
    
    chats = _read_table(ss.worksheet("chats_raw"))
    messages = _read_table(ss.worksheet("messages_raw"))
    spin_metrics = _read_table(ss.worksheet("spin_chat_metrics"))
    
    print(f"Загружено: {len(chats)} чатов, {len(messages)} сообщений, {len(spin_metrics)} SPIN-метрик")
    
    # Анализируем лучшие чаты
    print("Анализирую лучшие чаты...")
    best_chats = analyze_best_chats(chats, messages, spin_metrics)
    
    print(f"Найдено {len(best_chats)} лучших чатов (с заказами и хорошим SPIN)")
    
    # Генерируем скрипты
    scripts = generate_scripts(best_chats)
    
    # Записываем результаты
    print("Записываю результаты в Google Sheets...")
    
    # Лучшие чаты
    best_chats_rows = []
    for chat in best_chats:
        best_chats_rows.append({
            "chat_id": chat.chat_id,
            "manager_name": chat.manager_name,
            "has_order": "Да" if chat.has_order else "Нет",
            "spin_completeness": f"{chat.spin_completeness:.1f}%",
            "questions_count": chat.questions_count,
            "has_situation": "Да" if chat.spin_stages.get("S") else "Нет",
            "has_problem": "Да" if chat.spin_stages.get("P") else "Нет",
            "has_implication": "Да" if chat.spin_stages.get("I") else "Нет",
            "has_need_payoff": "Да" if chat.spin_stages.get("N") else "Нет",
            "key_phrases": " | ".join(chat.key_phrases[:5]),
            "sample_messages": " | ".join(chat.messages[:3]),
        })
    
    upsert_worksheet(
        ss,
        "best_chats_analysis",
        rows=dicts_to_table(
            best_chats_rows,
            header=[
                "chat_id", "manager_name", "has_order", "spin_completeness",
                "questions_count", "has_situation", "has_problem",
                "has_implication", "has_need_payoff", "key_phrases", "sample_messages",
            ],
        ),
    )
    
    # Скрипты
    scripts_rows = []
    for stage, stage_scripts in scripts.items():
        for script in stage_scripts:
            scripts_rows.append({
                "stage": stage,
                "script": script["script"],
                "usage_count": script.get("usage_count", 0),
                "spin_stage": script.get("stage", ""),
            })
    
    upsert_worksheet(
        ss,
        "best_scripts",
        rows=dicts_to_table(
            scripts_rows,
            header=["stage", "script", "usage_count", "spin_stage"],
        ),
    )
    
    print("\n✅ Анализ завершён! Результаты записаны в Google Sheets:")
    print("   - best_chats_analysis (топ-20 лучших чатов)")
    print("   - best_scripts (готовые скрипты по этапам)")
    
    # Выводим примеры скриптов
    print("\n" + "="*80)
    print("ГОТОВЫЕ СКРИПТЫ (на основе лучших практик)")
    print("="*80 + "\n")
    
    for stage, stage_scripts in scripts.items():
        if stage_scripts:
            print(f"\n{stage.upper()}:")
            for script in stage_scripts[:3]:
                print(f"  - {script['script']}")


if __name__ == "__main__":
    main()




