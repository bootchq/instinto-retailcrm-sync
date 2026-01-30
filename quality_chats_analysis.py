"""
Анализ качественных чатов по этапам продаж.

Критерии качественного чата:
1. Все этапы продаж присутствуют (приветствие, выявление потребностей, презентация, возражения, закрытие)
2. Достаточная длина диалога (не простой заказ "дайте белье")
3. Качественная консультация
4. Скорость ответа хорошая
5. Достаточно вопросов от менеджера
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
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


def _to_int(v: Any) -> Optional[int]:
    try:
        if v is None or v == "":
            return None
        return int(float(str(v)))
    except Exception:
        return None


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
class SalesStage:
    """Этап продаж."""
    name: str
    detected: bool
    examples: List[str]  # Примеры фраз/сообщений на этом этапе


@dataclass
class QualityChat:
    """Качественный чат с анализом."""
    chat_id: str
    manager_name: str
    client_id: str
    order_id: str
    is_successful: bool
    payment_status: str
    
    # Метрики качества
    total_messages: int
    manager_messages: int
    client_messages: int
    questions_count: int
    first_response_sec: Optional[int]
    dialog_length_chars: int
    
    # Этапы продаж
    has_greeting: bool
    has_needs_identification: bool
    has_presentation: bool
    has_objections_handling: bool
    has_closing: bool
    
    # Примеры на каждом этапе
    greeting_examples: List[str]
    needs_examples: List[str]
    presentation_examples: List[str]
    objections_examples: List[str]
    closing_examples: List[str]
    
    # Полные сообщения для анализа
    all_messages: List[Dict[str, Any]]  # direction, text, sent_at
    
    # Оценка качества
    quality_score: float  # 0-100
    is_consultation: bool  # True если это консультация, а не простой заказ


def detect_sales_stages(messages: List[Dict[str, Any]]) -> Dict[str, SalesStage]:
    """Определяет этапы продаж в диалоге."""
    
    stages = {
        "greeting": SalesStage("Приветствие", False, []),
        "needs_identification": SalesStage("Выявление потребностей", False, []),
        "presentation": SalesStage("Презентация", False, []),
        "objections_handling": SalesStage("Работа с возражениями", False, []),
        "closing": SalesStage("Закрытие сделки", False, []),
    }
    
    manager_messages = [m for m in messages if m.get("direction") == "out"]
    client_messages = [m for m in messages if m.get("direction") == "in"]
    
    # 1. Приветствие (первые 2-3 сообщения менеджера)
    for msg in manager_messages[:3]:
        text = str(msg.get("text", "")).lower()
        if any(word in text for word in ["здравствуйте", "добрый", "привет", "день", "вечер", "утро"]):
            stages["greeting"].detected = True
            stages["greeting"].examples.append(msg.get("text", "")[:200])
            break
    
    # 2. Выявление потребностей (вопросы менеджера)
    question_patterns = [
        r"\?",
        r"какой\s+",
        r"какая\s+",
        r"сколько\s+",
        r"когда\s+",
        r"где\s+",
        r"для\s+кого",
        r"какие\s+",
        r"что\s+вас\s+интересует",
        r"что\s+нужно",
        r"расскажите",
        r"подскажите",
    ]
    
    for msg in manager_messages:
        text = str(msg.get("text", "")).lower()
        if any(re.search(pattern, text) for pattern in question_patterns):
            stages["needs_identification"].detected = True
            if len(stages["needs_identification"].examples) < 5:
                stages["needs_identification"].examples.append(msg.get("text", "")[:200])
    
    # 3. Презентация (описание товара/услуги, преимущества)
    presentation_patterns = [
        r"у\s+нас\s+",
        r"мы\s+предлагаем",
        r"это\s+",
        r"характеристики",
        r"преимущества",
        r"подходит\s+для",
        r"идеально\s+для",
        r"рекомендую",
        r"советую",
        r"состоит\s+из",
        r"материал",
        r"размер",
        r"цвет",
    ]
    
    for msg in manager_messages:
        text = str(msg.get("text", "")).lower()
        if any(re.search(pattern, text) for pattern in presentation_patterns):
            stages["presentation"].detected = True
            if len(stages["presentation"].examples) < 5:
                stages["presentation"].examples.append(msg.get("text", "")[:200])
    
    # 4. Работа с возражениями (ответы на сомнения клиента)
    objection_patterns = [
        r"но\s+",
        r"однако\s+",
        r"понимаю\s+ваши\s+сомнения",
        r"да,\s+но",
        r"согласен",
        r"вы\s+правы",
        r"однако",
        r"но\s+учитывайте",
        r"есть\s+решение",
    ]
    
    # Ищем возражения клиента
    client_objections = False
    for msg in client_messages:
        text = str(msg.get("text", "")).lower()
        if any(word in text for word in ["дорого", "не подходит", "сомневаюсь", "не уверен", "не знаю", "может быть", "подумаю"]):
            client_objections = True
            break
    
    # Если были возражения, ищем ответы менеджера после них
    if client_objections:
        for msg in manager_messages:
            text = str(msg.get("text", "")).lower()
            if any(re.search(pattern, text) for pattern in objection_patterns):
                stages["objections_handling"].detected = True
                if len(stages["objections_handling"].examples) < 5:
                    stages["objections_handling"].examples.append(msg.get("text", "")[:200])
    
    # 5. Закрытие сделки (предложение оформить заказ)
    closing_patterns = [
        r"оформим\s+заказ",
        r"оформить\s+заказ",
        r"можем\s+оформить",
        r"ссылка\s+на\s+оплату",
        r"оплат",
        r"заказ",
        r"готов\s+оформить",
        r"перейти\s+к\s+оформлению",
        r"оформить",
    ]
    
    for msg in manager_messages:
        text = str(msg.get("text", "")).lower()
        if any(re.search(pattern, text) for pattern in closing_patterns):
            stages["closing"].detected = True
            if len(stages["closing"].examples) < 5:
                stages["closing"].examples.append(msg.get("text", "")[:200])
    
    return stages


def is_consultation(messages: List[Dict[str, Any]]) -> bool:
    """Определяет, является ли чат консультацией или простым заказом."""
    
    manager_messages = [m for m in messages if m.get("direction") == "out"]
    client_messages = [m for m in messages if m.get("direction") == "in"]
    
    # Простой заказ: клиент сразу говорит "дайте X" или "нужен X"
    simple_order_patterns = [
        r"дайте\s+мне",
        r"нужно\s+",
        r"нужен\s+",
        r"хочу\s+купить",
        r"куплю\s+",
    ]
    
    # Если клиент в первых 2 сообщениях сразу просит товар без вопросов
    if len(client_messages) >= 1:
        first_client_msg = str(client_messages[0].get("text", "")).lower()
        if any(re.search(pattern, first_client_msg) for pattern in simple_order_patterns):
            # Проверяем, были ли вопросы от менеджера
            if len(manager_messages) < 3:
                return False  # Простой заказ
    
    # Если менеджер задал меньше 2 вопросов - это не консультация
    questions_count = sum(1 for m in manager_messages if "?" in str(m.get("text", "")))
    if questions_count < 2:
        return False
    
    # Если диалог очень короткий (< 5 сообщений) - скорее всего простой заказ
    if len(messages) < 5:
        return False
    
    return True


def calculate_quality_score(
    stages: Dict[str, SalesStage],
    total_messages: int,
    questions_count: int,
    first_response_sec: Optional[int],
    is_consultation: bool,
) -> float:
    """Вычисляет оценку качества чата (0-100)."""
    
    score = 0.0
    
    # Этапы продаж (максимум 50 баллов)
    stage_weights = {
        "greeting": 5,
        "needs_identification": 15,
        "presentation": 15,
        "objections_handling": 10,
        "closing": 5,
    }
    
    for stage_name, weight in stage_weights.items():
        if stages[stage_name].detected:
            score += weight
    
    # Длина диалога (максимум 20 баллов)
    if total_messages >= 20:
        score += 20
    elif total_messages >= 15:
        score += 15
    elif total_messages >= 10:
        score += 10
    elif total_messages >= 5:
        score += 5
    
    # Количество вопросов (максимум 15 баллов)
    if questions_count >= 8:
        score += 15
    elif questions_count >= 5:
        score += 10
    elif questions_count >= 3:
        score += 5
    
    # Скорость ответа (максимум 10 баллов)
    if first_response_sec is not None:
        if first_response_sec <= 60:  # До 1 минуты
            score += 10
        elif first_response_sec <= 300:  # До 5 минут
            score += 5
    
    # Консультация vs простой заказ (максимум 5 баллов)
    if is_consultation:
        score += 5
    
    return min(100.0, score)


def analyze_quality_chats(
    chats: List[Dict[str, Any]],
    messages: List[Dict[str, Any]],
) -> List[QualityChat]:
    """Анализирует качественные чаты."""
    
    # Группируем сообщения по чатам
    messages_by_chat: Dict[str, List[Dict[str, Any]]] = {}
    for msg in messages:
        chat_id = str(msg.get("chat_id", ""))
        if chat_id:
            messages_by_chat.setdefault(chat_id, []).append(msg)
    
    quality_chats: List[QualityChat] = []
    
    for chat in chats:
        chat_id = str(chat.get("chat_id", ""))
        if not chat_id:
            continue
        
        chat_messages = messages_by_chat.get(chat_id, [])
        if not chat_messages:
            continue
        
        # Сортируем сообщения по времени
        chat_messages.sort(key=lambda m: m.get("sent_at", ""))
        
        manager_messages = [m for m in chat_messages if m.get("direction") == "out"]
        client_messages = [m for m in chat_messages if m.get("direction") == "in"]
        
        # Определяем этапы продаж
        stages = detect_sales_stages(chat_messages)
        
        # Подсчитываем метрики
        questions_count = sum(1 for m in manager_messages if "?" in str(m.get("text", "")))
        first_response_sec = _to_int(chat.get("first_response_sec"))
        total_chars = sum(len(str(m.get("text", ""))) for m in chat_messages)
        
        # Определяем, консультация ли это
        is_consult = is_consultation(chat_messages)
        
        # Вычисляем оценку качества
        quality_score = calculate_quality_score(
            stages,
            len(chat_messages),
            questions_count,
            first_response_sec,
            is_consult,
        )
        
        # Берём только качественные чаты (оценка >= 60 и это консультация)
        if quality_score >= 60 and is_consult:
            quality_chats.append(QualityChat(
                chat_id=chat_id,
                manager_name=str(chat.get("manager_name", "")),
                client_id=str(chat.get("client_id", "")),
                order_id=str(chat.get("order_id", "")),
                is_successful=str(chat.get("is_successful", "Нет")),
                payment_status=str(chat.get("payment_status_ru", "")),
                total_messages=len(chat_messages),
                manager_messages=len(manager_messages),
                client_messages=len(client_messages),
                questions_count=questions_count,
                first_response_sec=first_response_sec,
                dialog_length_chars=total_chars,
                has_greeting=stages["greeting"].detected,
                has_needs_identification=stages["needs_identification"].detected,
                has_presentation=stages["presentation"].detected,
                has_objections_handling=stages["objections_handling"].detected,
                has_closing=stages["closing"].detected,
                greeting_examples=stages["greeting"].examples[:3],
                needs_examples=stages["needs_identification"].examples[:5],
                presentation_examples=stages["presentation"].examples[:5],
                objections_examples=stages["objections_handling"].examples[:5],
                closing_examples=stages["closing"].examples[:3],
                all_messages=chat_messages,
                quality_score=quality_score,
                is_consultation=is_consult,
            ))
    
    # Сортируем по оценке качества (лучшие первыми)
    quality_chats.sort(key=lambda c: c.quality_score, reverse=True)
    
    return quality_chats


def main() -> None:
    base = Path(__file__).resolve().parent
    env = _load_env(base / "env")
    
    ss = open_spreadsheet(
        spreadsheet_id=env["GOOGLE_SHEETS_ID"],
        service_account_json_path=env["GOOGLE_SERVICE_ACCOUNT_JSON"],
    )
    
    print("📖 Читаю данные из Google Sheets...")
    
    chats = _read_table(ss.worksheet("chats_raw"))
    messages = _read_table(ss.worksheet("messages_raw"))
    
    print(f"   Загружено: {len(chats)} чатов, {len(messages)} сообщений")
    
    print("\n🔍 Анализирую качественные чаты...")
    quality_chats = analyze_quality_chats(chats, messages)
    
    print(f"✅ Найдено {len(quality_chats)} качественных чатов (оценка >= 60, консультация)")
    
    # Записываем результаты
    print("\n💾 Записываю результаты в Google Sheets...")
    
    quality_rows = []
    for chat in quality_chats:
        quality_rows.append({
            "chat_id": chat.chat_id,
            "manager_name": chat.manager_name,
            "client_id": chat.client_id,
            "order_id": chat.order_id,
            "is_successful": chat.is_successful,
            "payment_status": chat.payment_status,
            "quality_score": f"{chat.quality_score:.1f}",
            "is_consultation": "Да" if chat.is_consultation else "Нет",
            "total_messages": chat.total_messages,
            "manager_messages": chat.manager_messages,
            "client_messages": chat.client_messages,
            "questions_count": chat.questions_count,
            "first_response_sec": chat.first_response_sec if chat.first_response_sec else "",
            "has_greeting": "Да" if chat.has_greeting else "Нет",
            "has_needs_identification": "Да" if chat.has_needs_identification else "Нет",
            "has_presentation": "Да" if chat.has_presentation else "Нет",
            "has_objections_handling": "Да" if chat.has_objections_handling else "Нет",
            "has_closing": "Да" if chat.has_closing else "Нет",
        })
    
    upsert_worksheet(
        ss,
        "quality_chats",
        rows=dicts_to_table(
            quality_rows,
            header=[
                "chat_id", "manager_name", "client_id", "order_id",
                "is_successful", "payment_status", "quality_score", "is_consultation",
                "total_messages", "manager_messages", "client_messages", "questions_count",
                "first_response_sec",
                "has_greeting", "has_needs_identification", "has_presentation",
                "has_objections_handling", "has_closing",
            ],
        ),
    )
    
    print(f"✅ Записано {len(quality_rows)} качественных чатов в лист 'quality_chats'")
    print("\n📊 Статистика:")
    
    if quality_chats:
        avg_score = sum(c.quality_score for c in quality_chats) / len(quality_chats)
        avg_messages = sum(c.total_messages for c in quality_chats) / len(quality_chats)
        avg_questions = sum(c.questions_count for c in quality_chats) / len(quality_chats)
        
        print(f"   Средняя оценка качества: {avg_score:.1f}")
        print(f"   Среднее количество сообщений: {avg_messages:.1f}")
        print(f"   Среднее количество вопросов: {avg_questions:.1f}")
        
        stages_stats = {
            "Приветствие": sum(1 for c in quality_chats if c.has_greeting),
            "Выявление потребностей": sum(1 for c in quality_chats if c.has_needs_identification),
            "Презентация": sum(1 for c in quality_chats if c.has_presentation),
            "Работа с возражениями": sum(1 for c in quality_chats if c.has_objections_handling),
            "Закрытие сделки": sum(1 for c in quality_chats if c.has_closing),
        }
        
        print("\n   Этапы продаж (сколько чатов содержат этап):")
        for stage, count in stages_stats.items():
            pct = (count / len(quality_chats)) * 100 if quality_chats else 0
            print(f"     {stage}: {count} ({pct:.1f}%)")


if __name__ == "__main__":
    main()

