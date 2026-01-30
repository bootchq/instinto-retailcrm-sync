"""
Создание "источника истины" - эталона работы менеджера.

Анализирует успешные чаты (с оплаченными заказами) и создаёт:
1. Книгу продаж (этапы + типы клиентов)
2. Книгу Q&A (вопросы и ответы)
3. Целевые показатели (конверсия по этапам)
"""

from __future__ import annotations

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
    """Читает таблицу, автоматически определяя заголовки."""
    values = ws.get_all_values()
    if not values:
        return []
    
    # Определяем заголовки
    # Если первая строка пустая или не похожа на заголовки, используем стандартные
    first_row = values[0] if values else []
    is_header_empty = not any(first_row) or all(not str(cell).strip() for cell in first_row[:5])
    
    if is_header_empty:
        # Используем стандартные заголовки из export_to_sheets.py
        header = [
            "chat_id", "channel", "manager_id", "manager_name", "client_id", "order_id",
            "has_order", "payment_status", "payment_status_ru", "is_successful",
            "created_at", "updated_at", "status",
            "inbound_count", "outbound_count", "first_response_sec", "unanswered_inbound",
        ]
        data_start = 0  # Данные начинаются с первой строки
    else:
        header = [str(cell).strip() for cell in first_row]
        data_start = 1  # Данные начинаются со второй строки
    
    out: List[Dict[str, Any]] = []
    for row in values[data_start:]:
        d: Dict[str, Any] = {}
        for i, h in enumerate(header):
            d[h] = row[i] if i < len(row) else ""
        out.append(d)
    return out


@dataclass
class SalesStageExample:
    """Пример этапа продаж."""
    stage: str
    example_text: str
    chat_id: str
    manager_name: str
    usage_count: int = 1


@dataclass
class QAPair:
    """Пара вопрос-ответ."""
    question: str
    answer: str
    category: str
    chat_id: str
    manager_name: str
    usage_count: int = 1


def detect_sales_stage(text: str, position_in_chat: int, total_messages: int = 0) -> Optional[str]:
    """Определяет этап продаж по тексту сообщения."""
    text_lower = text.lower().strip()
    
    if not text_lower or len(text_lower) < 3:
        return None
    
    # Приветствие (первые 2-3 сообщения или начало диалога)
    if position_in_chat <= 2 or (total_messages > 0 and position_in_chat / total_messages < 0.1):
        greeting_words = [
            "здравствуйте", "здравствуй", "добрый", "привет", "день", "вечер", "утро",
            "добро пожаловать", "рады", "помогу", "чем могу помочь", "как дела"
        ]
        if any(word in text_lower for word in greeting_words):
            return "greeting"
    
    # Закрытие сделки (проверяем в первую очередь, так как может содержать другие слова)
    closing_words = [
        "оформим", "оформить", "заказ", "оплат", "готов оформить", "можем оформить",
        "ссылка", "оплата", "перейти к оплате", "оформить заказ", "купить",
        "приобрести", "заказать", "доставка", "адрес доставки"
    ]
    if any(word in text_lower for word in closing_words):
        return "closing"
    
    # Работа с возражениями
    objection_words = [
        "но", "однако", "понимаю", "сомнения", "согласен", "вы правы",
        "есть решение", "можно", "альтернатива", "вариант", "если", "хотя"
    ]
    if any(word in text_lower for word in objection_words) and len(text_lower) > 20:
        return "objections_handling"
    
    # Выявление потребностей (вопросы)
    if "?" in text_lower:
        question_words = [
            "какой", "какая", "какие", "сколько", "когда", "где", "для кого",
            "что вас интересует", "что нужно", "расскажите", "подскажите",
            "как", "почему", "зачем", "какого", "какую", "каких"
        ]
        if any(word in text_lower for word in question_words) or len(text_lower) < 100:
            return "needs_identification"
    
    # Презентация (описание товара/услуги)
    presentation_words = [
        "у нас", "мы предлагаем", "это", "характеристики", "преимущества",
        "подходит для", "идеально для", "рекомендую", "советую", "состав",
        "материал", "размер", "цвет", "цена", "стоимость", "модель"
    ]
    if any(word in text_lower for word in presentation_words) and len(text_lower) > 15:
        return "presentation"
    
    return None


def extract_qa_pairs(messages: List[Dict[str, Any]]) -> List[QAPair]:
    """Извлекает пары вопрос-ответ из диалога."""
    qa_pairs: List[QAPair] = []
    
    for i in range(len(messages) - 1):
        current = messages[i]
        next_msg = messages[i + 1]
        
        # Если текущее сообщение от клиента (вопрос), а следующее от менеджера (ответ)
        if current.get("direction") == "in" and next_msg.get("direction") == "out":
            question = str(current.get("text", "")).strip()
            answer = str(next_msg.get("text", "")).strip()
            
            if question and answer and len(question) > 5 and len(answer) > 10:
                # Определяем категорию вопроса
                question_lower = question.lower()
                category = "другое"
                
                if any(word in question_lower for word in ["цена", "стоит", "стоимость", "сколько", "дорого", "дешево"]):
                    category = "цена"
                elif any(word in question_lower for word in ["доставка", "доставят", "привез", "курьер", "почта"]):
                    category = "доставка"
                elif any(word in question_lower for word in ["размер", "размеры", "s", "m", "l", "xl"]):
                    category = "размеры"
                elif any(word in question_lower for word in ["материал", "состав", "ткань", "хлопок", "синтетика"]):
                    category = "характеристики"
                elif any(word in question_lower for word in ["гарантия", "возврат", "обмен", "вернуть"]):
                    category = "гарантии_и_возвраты"
                elif any(word in question_lower for word in ["цвет", "цвета", "какой цвет"]):
                    category = "цвета"
                elif any(word in question_lower for word in ["есть", "наличие", "в наличии", "доступен"]):
                    category = "наличие"
                
                qa_pairs.append(QAPair(
                    question=question[:500],  # Ограничиваем длину
                    answer=answer[:500],
                    category=category,
                    chat_id=str(current.get("chat_id", "")),
                    manager_name="",  # Заполним позже
                ))
    
    return qa_pairs


def analyze_chat_quality(chat_messages: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Анализирует качество обработки чата по этапам продаж."""
    
    # Сортируем сообщения по времени
    chat_messages.sort(key=lambda m: m.get("sent_at", ""))
    
    # Разделяем сообщения менеджера и клиента
    # Сообщения менеджера: direction="out" ИЛИ author_type="User"
    manager_messages = [
        m for m in chat_messages 
        if (
            str(m.get("direction", "")).strip() == "out" or
            str(m.get("author_type", "")).strip() == "User" or
            (m.get("manager_id") and str(m.get("manager_id", "")).strip() and str(m.get("manager_id", "")).strip() != "")
        )
    ]
    client_messages = [
        m for m in chat_messages 
        if (
            str(m.get("direction", "")).strip() == "in" or
            str(m.get("author_type", "")).strip() in ["Customer", "Channel"]
        )
    ]
    
    if len(manager_messages) < 2:
        return None
    
    # Анализируем этапы продаж
    has_greeting = False
    has_needs_identification = False
    has_presentation = False
    has_objections_handling = False
    has_closing = False
    
    questions_count = 0
    stage_examples_found = []
    
    for i, msg in enumerate(manager_messages):
        text = str(msg.get("text", "")).strip()
        if not text or len(text) < 5:
            continue
        
        # Определяем этап
        stage = detect_sales_stage(text, i, len(manager_messages))
        
        if stage == "greeting":
            has_greeting = True
            stage_examples_found.append(("greeting", text[:300], i))
        elif stage == "needs_identification":
            has_needs_identification = True
            questions_count += 1
            stage_examples_found.append(("needs_identification", text[:300], i))
        elif stage == "presentation":
            has_presentation = True
            stage_examples_found.append(("presentation", text[:300], i))
        elif stage == "objections_handling":
            has_objections_handling = True
            stage_examples_found.append(("objections_handling", text[:300], i))
        elif stage == "closing":
            has_closing = True
            stage_examples_found.append(("closing", text[:300], i))
        
        # Дополнительная проверка: если есть "?" - это выявление потребностей
        if "?" in text and i < len(manager_messages) * 0.6:  # В первой половине диалога
            has_needs_identification = True
            questions_count += 1
    
    # Считаем коэффициент качества (0-100)
    quality_score = 0
    if has_greeting:
        quality_score += 20
    if has_needs_identification:
        quality_score += 20
        # Бонус за количество вопросов (макс 5)
        quality_score += min(questions_count * 2, 10)
    if has_presentation:
        quality_score += 20
    if has_objections_handling:
        quality_score += 15
    if has_closing:
        quality_score += 15
    
    # Бонус за длину диалога (хороший диалог обычно длиннее)
    if len(chat_messages) >= 10:
        quality_score += 5
    if len(chat_messages) >= 20:
        quality_score += 5
    
    return {
        "has_greeting": has_greeting,
        "has_needs_identification": has_needs_identification,
        "has_presentation": has_presentation,
        "has_objections_handling": has_objections_handling,
        "has_closing": has_closing,
        "questions_count": questions_count,
        "quality_score": min(quality_score, 100),
        "total_messages": len(chat_messages),
        "manager_messages": len(manager_messages),
        "client_messages": len(client_messages),
        "stage_examples": stage_examples_found,
    }


def analyze_successful_chats(
    chats: List[Dict[str, Any]],
    messages: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[SalesStageExample], List[QAPair]]:
    """Анализирует успешные чаты по качеству обработки (коэффициент обработки)."""
    
    # Группируем сообщения по чатам
    messages_by_chat: Dict[str, List[Dict[str, Any]]] = {}
    for msg in messages:
        chat_id = str(msg.get("chat_id", ""))
        if chat_id:
            messages_by_chat.setdefault(chat_id, []).append(msg)
    
    # Анализируем качество всех чатов
    chats_with_quality: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    
    for chat in chats:
        chat_id = str(chat.get("chat_id", ""))
        chat_messages = messages_by_chat.get(chat_id, [])
        
        if not chat_messages:
            continue
        
        # Анализируем качество обработки
        quality = analyze_chat_quality(chat_messages)
        
        if quality and quality["quality_score"] > 0:
            chats_with_quality.append((chat, quality))
    
    # Сортируем по коэффициенту качества (лучшие первыми)
    chats_with_quality.sort(key=lambda x: x[1]["quality_score"], reverse=True)
    
    # Берём топ 20% чатов с наилучшим качеством обработки
    top_count = max(100, int(len(chats_with_quality) * 0.2))  # Минимум 100 чатов
    top_chats = chats_with_quality[:top_count]
    
    print(f"   Проанализировано чатов: {len(chats_with_quality)}")
    print(f"   Отобрано топ чатов по качеству: {len(top_chats)}")
    if top_chats:
        print(f"   Диапазон качества: {top_chats[-1][1]['quality_score']:.1f} - {top_chats[0][1]['quality_score']:.1f}")
    
    # Формируем результаты
    successful_chats: List[Dict[str, Any]] = []
    stage_examples: List[SalesStageExample] = []
    all_qa_pairs: List[QAPair] = []
    
    for chat, quality in top_chats:
        chat_id = str(chat.get("chat_id", ""))
        chat_messages = messages_by_chat.get(chat_id, [])
        chat_messages.sort(key=lambda m: m.get("sent_at", ""))
        
        manager_messages = [
            m for m in chat_messages 
            if (
                str(m.get("direction", "")).strip() == "out" or
                str(m.get("author_type", "")).strip() == "User" or
                (m.get("manager_id") and str(m.get("manager_id", "")).strip() and str(m.get("manager_id", "")).strip() != "")
            )
        ]
        
        successful_chats.append({
            "chat_id": chat_id,
            "manager_name": str(chat.get("manager_name", "")),
            "total_messages": quality["total_messages"],
            "manager_messages": quality["manager_messages"],
            "questions_count": quality["questions_count"],
            "quality_score": quality["quality_score"],
            "first_response_sec": _to_int(chat.get("first_response_sec")),
            "has_greeting": quality["has_greeting"],
            "has_needs_identification": quality["has_needs_identification"],
            "has_presentation": quality["has_presentation"],
            "has_objections_handling": quality["has_objections_handling"],
            "has_closing": quality["has_closing"],
        })
        
        # Извлекаем примеры этапов продаж
        for stage, example_text, position in quality["stage_examples"]:
            stage_examples.append(SalesStageExample(
                stage=stage,
                example_text=example_text,
                chat_id=chat_id,
                manager_name=str(chat.get("manager_name", "")),
            ))
        
        # Извлекаем Q&A пары
        qa_pairs = extract_qa_pairs(chat_messages)
        for qa in qa_pairs:
            qa.manager_name = str(chat.get("manager_name", ""))
        all_qa_pairs.extend(qa_pairs)
    
    return successful_chats, stage_examples, all_qa_pairs


def create_sales_book(stage_examples: List[SalesStageExample]) -> List[Dict[str, Any]]:
    """Создаёт книгу продаж на основе примеров."""
    
    # Группируем примеры по этапам
    by_stage: Dict[str, List[SalesStageExample]] = {}
    for ex in stage_examples:
        by_stage.setdefault(ex.stage, []).append(ex)
    
    # Подсчитываем частоту использования
    for stage, examples in by_stage.items():
        # Группируем похожие примеры
        seen = set()
        for ex in examples:
            text_key = ex.example_text.lower().strip()[:100]
            if text_key not in seen:
                seen.add(text_key)
            else:
                # Находим существующий и увеличиваем счётчик
                for existing in by_stage[stage]:
                    if existing.example_text.lower().strip()[:100] == text_key:
                        existing.usage_count += 1
                        break
    
    # Сортируем по частоте использования
    for stage in by_stage:
        by_stage[stage].sort(key=lambda x: x.usage_count, reverse=True)
    
    # Создаём структуру книги продаж
    stage_names = {
        "greeting": "1. Приветствие",
        "needs_identification": "2. Выявление потребностей",
        "presentation": "3. Презентация товара/услуги",
        "objections_handling": "4. Работа с возражениями",
        "closing": "5. Закрытие сделки",
    }
    
    book_rows: List[Dict[str, Any]] = []
    
    for stage_key, stage_name in stage_names.items():
        examples = by_stage.get(stage_key, [])
        
        # Добавляем заголовок этапа
        book_rows.append({
            "этап": stage_name,
            "пример_фразы": "",
            "описание": f"Этап: {stage_name}",
            "когда_использовать": "",
            "частота_использования": "",
            "источник": "",
        })
        
        # Добавляем топ-10 примеров
        for ex in examples[:10]:
            book_rows.append({
                "этап": "",
                "пример_фразы": ex.example_text,
                "описание": "",
                "когда_использовать": f"Использовать в начале диалога" if stage_key == "greeting" else f"Использовать на этапе {stage_name}",
                "частота_использования": str(ex.usage_count),
                "источник": f"Чат {ex.chat_id}, менеджер: {ex.manager_name}",
            })
    
    return book_rows


def create_qa_book(qa_pairs: List[QAPair]) -> List[Dict[str, Any]]:
    """Создаёт книгу Q&A на основе пар вопрос-ответ."""
    
    # Группируем по категориям
    by_category: Dict[str, List[QAPair]] = {}
    for qa in qa_pairs:
        by_category.setdefault(qa.category, []).append(qa)
    
    # Подсчитываем частоту вопросов
    question_counter: Dict[str, int] = Counter()
    for qa in qa_pairs:
        question_key = qa.question.lower().strip()[:100]
        question_counter[question_key] += 1
    
    # Сортируем по частоте
    for category in by_category:
        by_category[category].sort(
            key=lambda x: question_counter.get(x.question.lower().strip()[:100], 0),
            reverse=True
        )
    
    # Создаём структуру Q&A книги
    category_names = {
        "цена": "💰 Цена и стоимость",
        "доставка": "🚚 Доставка",
        "размеры": "📏 Размеры",
        "характеристики": "📋 Характеристики товара",
        "гарантии_и_возвраты": "🔄 Гарантии и возвраты",
        "цвета": "🎨 Цвета",
        "наличие": "📦 Наличие товара",
        "другое": "❓ Другие вопросы",
    }
    
    qa_rows: List[Dict[str, Any]] = []
    
    for category_key, category_name in category_names.items():
        pairs = by_category.get(category_key, [])
        
        if not pairs:
            continue
        
        # Добавляем заголовок категории
        qa_rows.append({
            "категория": category_name,
            "вопрос": "",
            "ответ_шаблон": "",
            "частота": "",
            "источник": "",
        })
        
        # Добавляем топ-20 вопросов по категории
        unique_questions = {}
        for qa in pairs[:50]:  # Берём больше, чтобы отфильтровать уникальные
            question_key = qa.question.lower().strip()[:100]
            if question_key not in unique_questions:
                unique_questions[question_key] = qa
                if len(unique_questions) >= 20:
                    break
        
        for qa in list(unique_questions.values())[:20]:
            frequency = question_counter.get(qa.question.lower().strip()[:100], 1)
            qa_rows.append({
                "категория": "",
                "вопрос": qa.question,
                "ответ_шаблон": qa.answer,
                "частота": str(frequency),
                "источник": f"Чат {qa.chat_id}, менеджер: {qa.manager_name}",
            })
    
    return qa_rows


def create_conversion_by_stages(
    successful_chats: List[Dict[str, Any]],
    stage_examples: List[SalesStageExample],
) -> List[Dict[str, Any]]:
    """Создаёт анализ конверсии по этапам продаж."""
    
    # Подсчитываем использование этапов в успешных чатах
    stage_usage: Dict[str, int] = Counter()
    for ex in stage_examples:
        stage_usage[ex.stage] += 1
    
    total_successful = len(successful_chats)
    
    stage_names = {
        "greeting": "Приветствие",
        "needs_identification": "Выявление потребностей",
        "presentation": "Презентация",
        "objections_handling": "Работа с возражениями",
        "closing": "Закрытие сделки",
    }
    
    conversion_rows: List[Dict[str, Any]] = []
    
    for stage_key, stage_name in stage_names.items():
        usage_count = stage_usage.get(stage_key, 0)
        usage_rate = (usage_count / total_successful * 100) if total_successful > 0 else 0
        
        # Определяем приоритет (чем выше использование, тем выше приоритет)
        priority = "Высокий" if usage_rate >= 80 else "Средний" if usage_rate >= 50 else "Низкий"
        
        conversion_rows.append({
            "этап": stage_name,
            "использование_в_успешных_чатах": usage_count,
            "процент_использования": f"{usage_rate:.1f}%",
            "приоритет": priority,
            "рекомендация": f"Обязательно использовать на этапе {stage_name}" if usage_rate >= 80 else f"Желательно использовать на этапе {stage_name}",
        })
    
    return conversion_rows


def main() -> None:
    base = Path(__file__).resolve().parent
    env = _load_env(base / "env")
    
    ss = open_spreadsheet(
        spreadsheet_id=env["GOOGLE_SHEETS_ID"],
        service_account_json_path=env["GOOGLE_SERVICE_ACCOUNT_JSON"],
    )
    
    print("=" * 60)
    print("СОЗДАНИЕ ИСТОЧНИКА ИСТИНЫ - ЭТАЛОНА РАБОТЫ МЕНЕДЖЕРА")
    print("=" * 60)
    
    print("\n📖 Читаю данные из Google Sheets...")
    chats = _read_table(ss.worksheet("chats_raw"))
    
    # messages_raw может не иметь заголовков, используем стандартные
    messages_ws = ss.worksheet("messages_raw")
    messages_values = messages_ws.get_all_values()
    messages_header = ["chat_id", "message_id", "sent_at", "direction", "manager_id", "message_type", "author_type", "text"]
    
    # Если первая строка пустая или не похожа на заголовки, начинаем с первой строки
    if not messages_values or not any(messages_values[0]):
        messages_data_start = 0
    else:
        # Проверяем, похожа ли первая строка на заголовки
        first_row_lower = [str(cell).lower().strip() for cell in messages_values[0][:5]]
        if "chat_id" in first_row_lower or "message_id" in first_row_lower:
            messages_data_start = 1
            messages_header = [str(cell).strip() for cell in messages_values[0]]
        else:
            messages_data_start = 0
    
    messages = []
    for row in messages_values[messages_data_start:]:
        d: Dict[str, Any] = {}
        for i, h in enumerate(messages_header):
            d[h] = row[i] if i < len(row) else ""
        messages.append(d)
    
    print(f"   Загружено: {len(chats)} чатов, {len(messages)} сообщений")
    
    print("\n🔍 Анализирую чаты по качеству обработки (коэффициент обработки)...")
    successful_chats, stage_examples, qa_pairs = analyze_successful_chats(chats, messages)
    
    print(f"✅ Найдено {len(successful_chats)} чатов с наилучшим качеством обработки")
    print(f"   Примеров этапов продаж: {len(stage_examples)}")
    print(f"   Пар вопрос-ответ: {len(qa_pairs)}")
    
    if not successful_chats:
        print("\n⚠️ Не найдено успешных чатов для анализа!")
        print("   Попробуйте снизить критерии или проверить данные о заказах.")
        return
    
    # Создаём книги
    print("\n📚 Создаю книги...")
    
    # 1. Книга продаж
    print("   1. Книга продаж...")
    sales_book = create_sales_book(stage_examples)
    upsert_worksheet(
        ss,
        "Книга_продаж",
        rows=dicts_to_table(
            sales_book,
            header=["этап", "пример_фразы", "описание", "когда_использовать", "частота_использования", "источник"],
        ),
    )
    print(f"      ✅ Создано {len(sales_book)} строк")
    
    # 2. Книга Q&A
    print("   2. Книга Q&A...")
    qa_book = create_qa_book(qa_pairs)
    upsert_worksheet(
        ss,
        "Книга_Q_A",
        rows=dicts_to_table(
            qa_book,
            header=["категория", "вопрос", "ответ_шаблон", "частота", "источник"],
        ),
    )
    print(f"      ✅ Создано {len(qa_book)} строк")
    
    # 3. Конверсия по этапам
    print("   3. Конверсия по этапам...")
    conversion_by_stages = create_conversion_by_stages(successful_chats, stage_examples)
    upsert_worksheet(
        ss,
        "Конверсия_по_этапам",
        rows=dicts_to_table(
            conversion_by_stages,
            header=["этап", "использование_в_успешных_чатах", "процент_использования", "приоритет", "рекомендация"],
        ),
    )
    print(f"      ✅ Создано {len(conversion_by_stages)} строк")
    
    # 4. Целевые показатели (эталон)
    print("   4. Целевые показатели (эталон)...")
    
    # Вычисляем средние показатели успешных чатов
    avg_messages = sum(c["total_messages"] for c in successful_chats) / len(successful_chats) if successful_chats else 0
    avg_manager_messages = sum(c["manager_messages"] for c in successful_chats) / len(successful_chats) if successful_chats else 0
    avg_questions = sum(c["questions_count"] for c in successful_chats) / len(successful_chats) if successful_chats else 0
    avg_response_sec = sum(c["first_response_sec"] or 300 for c in successful_chats) / len(successful_chats) if successful_chats else 300
    
    target_metrics = [
        {
            "метрика": "Общее количество сообщений в чате",
            "целевое_значение": f"{avg_messages:.0f}",
            "единица_измерения": "сообщений",
            "описание": "Среднее количество сообщений в успешных чатах",
        },
        {
            "метрика": "Сообщений от менеджера",
            "целевое_значение": f"{avg_manager_messages:.0f}",
            "единица_измерения": "сообщений",
            "описание": "Среднее количество сообщений менеджера в успешных чатах",
        },
        {
            "метрика": "Количество вопросов от менеджера",
            "целевое_значение": f"{avg_questions:.0f}",
            "единица_измерения": "вопросов",
            "описание": "Среднее количество вопросов менеджера в успешных чатах",
        },
        {
            "метрика": "Скорость первого ответа",
            "целевое_значение": f"{avg_response_sec / 60:.1f}",
            "единица_измерения": "минут",
            "описание": "Средняя скорость первого ответа в успешных чатах",
        },
        {
            "метрика": "Использование этапа 'Приветствие'",
            "целевое_значение": "100%",
            "единица_измерения": "%",
            "описание": "Все успешные чаты начинаются с приветствия",
        },
        {
            "метрика": "Использование этапа 'Выявление потребностей'",
            "целевое_значение": "100%",
            "единица_измерения": "%",
            "описание": "Все успешные чаты содержат вопросы для выявления потребностей",
        },
        {
            "метрика": "Использование этапа 'Презентация'",
            "целевое_значение": "80%",
            "единица_измерения": "%",
            "описание": "Большинство успешных чатов содержат презентацию товара",
        },
        {
            "метрика": "Использование этапа 'Работа с возражениями'",
            "целевое_значение": "60%",
            "единица_измерения": "%",
            "описание": "При наличии возражений - обязательно работа с ними",
        },
        {
            "метрика": "Использование этапа 'Закрытие сделки'",
            "целевое_значение": "100%",
            "единица_измерения": "%",
            "описание": "Все успешные чаты заканчиваются предложением оформить заказ",
        },
    ]
    
    upsert_worksheet(
        ss,
        "Целевые_показатели_эталон",
        rows=dicts_to_table(
            target_metrics,
            header=["метрика", "целевое_значение", "единица_измерения", "описание"],
        ),
    )
    print(f"      ✅ Создано {len(target_metrics)} целевых показателей")
    
    print("\n" + "=" * 60)
    print("✅ ИСТОЧНИК ИСТИНЫ СОЗДАН!")
    print("=" * 60)
    print("\nСозданные листы:")
    print("  1. Книга_продаж - эталон работы по этапам")
    print("  2. Книга_Q_A - эталонные ответы на вопросы")
    print("  3. Конверсия_по_этапам - приоритеты этапов")
    print("  4. Целевые_показатели_эталон - к чему стремиться")


if __name__ == "__main__":
    main()

