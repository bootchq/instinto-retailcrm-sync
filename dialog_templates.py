from __future__ import annotations

"""
Генерация полных шаблонов диалогов на основе успешных чатов.

Создаёт готовые шаблоны от приветствия до закрытия сделки.
"""

import re
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Tuple

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


def extract_dialog_flow(messages: List[str]) -> List[Dict[str, str]]:
    """Извлекает структуру диалога из сообщений."""
    flow = []
    
    for i, msg in enumerate(messages[:15]):  # Первые 15 сообщений
        msg_lower = msg.lower()
        
        # Определяем этап
        stage = "unknown"
        if i == 0:
            stage = "greeting"
        elif any(word in msg_lower for word in ["какой", "какая", "сколько", "когда", "где", "для кого"]):
            stage = "situation"
        elif any(word in msg_lower for word in ["не устраивает", "проблем", "сложност", "не нравится"]):
            stage = "problem"
        elif any(word in msg_lower for word in ["приводит", "влияет", "будет если"]):
            stage = "implication"
        elif any(word in msg_lower for word in ["поможет", "даст", "зачем", "выгода"]):
            stage = "need_payoff"
        elif any(word in msg_lower for word in ["оформим", "заказ", "ссылка", "оплат"]):
            stage = "closing"
        
        flow.append({
            "step": i + 1,
            "stage": stage,
            "message": msg[:200],  # Первые 200 символов
        })
    
    return flow


def generate_full_template(flows: List[List[Dict[str, str]]]) -> Dict[str, Any]:
    """Генерирует полный шаблон диалога на основе лучших практик."""
    
    # Собираем наиболее частые последовательности
    stage_sequences = []
    for flow in flows:
        stages = [step["stage"] for step in flow]
        stage_sequences.append(" -> ".join(stages))
    
    most_common = Counter(stage_sequences).most_common(1)
    if not most_common:
        return {}
    
    best_sequence = most_common[0][0].split(" -> ")
    
    # Собираем лучшие фразы для каждого этапа
    stage_phrases: Dict[str, List[str]] = {}
    for flow in flows:
        for step in flow:
            stage = step["stage"]
            message = step["message"]
            if stage not in stage_phrases:
                stage_phrases[stage] = []
            if message and len(message) > 10:
                stage_phrases[stage].append(message)
    
    # Берем топ-3 фразы для каждого этапа
    templates: Dict[str, List[str]] = {}
    for stage in ["greeting", "situation", "problem", "implication", "need_payoff", "closing"]:
        phrases = stage_phrases.get(stage, [])
        if phrases:
            # Убираем дубликаты и берем уникальные
            unique_phrases = []
            seen = set()
            for p in phrases:
                p_clean = p.lower().strip()
                if p_clean not in seen and len(p_clean) > 10:
                    seen.add(p_clean)
                    unique_phrases.append(p)
                    if len(unique_phrases) >= 3:
                        break
            templates[stage] = unique_phrases
    
    return {
        "sequence": best_sequence,
        "templates": templates,
    }


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
    spin_metrics = _read_table(ss.worksheet("spin_chat_metrics"))
    
    # Группируем сообщения по чатам
    messages_by_chat: Dict[str, List[Dict[str, Any]]] = {}
    for msg in messages:
        chat_id = str(msg.get("chat_id", ""))
        if chat_id:
            messages_by_chat.setdefault(chat_id, []).append(msg)
    
    # Находим лучшие чаты (с хорошим SPIN)
    spin_by_chat: Dict[str, Dict[str, Any]] = {}
    for m in spin_metrics:
        chat_id = str(m.get("chat_id", ""))
        if chat_id:
            spin_by_chat[chat_id] = m
    
    best_flows = []
    
    for chat in chats:
        chat_id = str(chat.get("chat_id", ""))
        manager_id = str(chat.get("manager_id", ""))
        
        if not chat_id:
            continue
        
        spin_data = spin_by_chat.get(chat_id)
        if not spin_data:
            continue
        
        spin_completeness = float(str(spin_data.get("spin_completeness", "0%")).replace("%", "")) if spin_data.get("spin_completeness") else 0
        
        # Берем чаты с полнотой SPIN >= 50%
        if spin_completeness >= 50:
            chat_messages = messages_by_chat.get(chat_id, [])
            manager_messages = [
                str(m.get("text", "")) for m in chat_messages
                if m.get("direction") == "out" and str(m.get("manager_id", "")) == manager_id
            ]
            
            if len(manager_messages) >= 5:  # Минимум 5 сообщений
                flow = extract_dialog_flow(manager_messages)
                if flow:
                    best_flows.append(flow)
    
    print(f"Найдено {len(best_flows)} успешных диалогов для анализа")
    
    if not best_flows:
        print("⚠️ Не найдено достаточно успешных диалогов. Используем базовые шаблоны.")
        best_flows = []  # Используем базовые шаблоны
    
    # Генерируем шаблоны
    if best_flows:
        template = generate_full_template(best_flows)
    else:
        # Базовые шаблоны
        template = {
            "sequence": ["greeting", "situation", "problem", "implication", "need_payoff", "closing"],
            "templates": {
                "greeting": [
                    "Здравствуйте! Подскажите, пожалуйста, какой размер вам нужен?",
                    "Добрый день! Для кого выбираете?",
                ],
                "situation": [
                    "Какой размер вам нужен?",
                    "Для кого выбираете?",
                    "Когда планируете использовать?",
                ],
                "problem": [
                    "Что не устраивает в текущем белье?",
                    "Какие сложности возникают при выборе?",
                ],
                "implication": [
                    "К чему это приводит?",
                    "Как это влияет на вас?",
                ],
                "need_payoff": [
                    "Как это поможет вам?",
                    "Что это даст?",
                ],
                "closing": [
                    "Отлично! Оформим заказ?",
                    "Готовы оформить? Отправлю ссылку на оплату.",
                ],
            },
        }
    
    # Формируем полные шаблоны диалогов
    dialog_templates = []
    
    for stage in template["sequence"]:
        phrases = template["templates"].get(stage, [])
        for i, phrase in enumerate(phrases):
            dialog_templates.append({
                "stage": stage,
                "step_in_sequence": template["sequence"].index(stage) + 1,
                "phrase_number": i + 1,
                "phrase": phrase,
                "when_to_use": get_stage_description(stage),
                "next_stage": get_next_stage(template["sequence"], stage),
            })
    
    # Записываем в Google Sheets
    print("Записываю шаблоны диалогов в Google Sheets...")
    
    upsert_worksheet(
        ss,
        "dialog_templates_full",
        rows=dicts_to_table(
            dialog_templates,
            header=["stage", "step_in_sequence", "phrase_number", "phrase", "when_to_use", "next_stage"],
        ),
    )
    
    # Выводим шаблон
    print("\n" + "="*80)
    print("ПОЛНЫЙ ШАБЛОН ДИАЛОГА")
    print("="*80 + "\n")
    
    current_step = 0
    for stage in template["sequence"]:
        phrases = template["templates"].get(stage, [])
        if phrases:
            current_step += 1
            print(f"\n{current_step}. {stage.upper()}:")
            for phrase in phrases:
                print(f"   - {phrase}")
    
    print("\n✅ Шаблоны диалогов сохранены в Google Sheets (лист 'dialog_templates_full')")


def get_stage_description(stage: str) -> str:
    """Возвращает описание, когда использовать этап."""
    descriptions = {
        "greeting": "В самом начале диалога, при первом контакте",
        "situation": "После приветствия, чтобы понять ситуацию клиента",
        "problem": "После ситуационных вопросов, чтобы выявить проблемы",
        "implication": "После выявления проблемы, чтобы усилить её значимость",
        "need_payoff": "Перед предложением, чтобы показать выгоды",
        "closing": "Когда клиент готов к покупке, для завершения сделки",
    }
    return descriptions.get(stage, "")


def get_next_stage(sequence: List[str], current_stage: str) -> str:
    """Возвращает следующий этап в последовательности."""
    try:
        idx = sequence.index(current_stage)
        if idx < len(sequence) - 1:
            return sequence[idx + 1]
    except ValueError:
        pass
    return ""


if __name__ == "__main__":
    main()

