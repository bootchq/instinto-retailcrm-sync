from __future__ import annotations

"""
SPIN-анализ продаж по методике Нила Рэкхема.

Анализирует переписки менеджеров по 4 этапам SPIN:
- S (Situation) - Ситуационные вопросы
- P (Problem) - Проблемные вопросы  
- I (Implication) - Извлекающие вопросы
- N (Need-payoff) - Вопросы о выгодах

Сравнивает менеджеров и выявляет провалы по каждому этапу.
Создаёт план улучшения для каждого менеджера.
"""

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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


def _parse_dt(v: Any) -> Optional[datetime]:
    if not v:
        return None
    try:
        return dtparser.isoparse(str(v))
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


# ========== SPIN-паттерны ==========

# S (Situation) - Ситуационные вопросы
# Собираем информацию о текущей ситуации клиента
_RE_SPIN_S = re.compile(
    r"\b(какой|какая|какие|как|сколько|когда|куда|где|откуда|"
    r"размер|рост|вес|параметр|характеристик|"
    r"для кого|кому|кто|"
    r"как часто|как долго|как давно|"
    r"какой у вас|какая у вас|какие у вас|"
    r"расскажите|подскажите|уточните)\b",
    re.IGNORECASE,
)

# P (Problem) - Проблемные вопросы
# Выявляем проблемы и неудовлетворённость
_RE_SPIN_P = re.compile(
    r"\b(что не устраивает|что не нравится|что не подходит|"
    r"какие сложности|какие проблемы|какие трудности|"
    r"что беспокоит|что волнует|что тревожит|"
    r"не устраивает|не подходит|не нравится|"
    r"проблема|сложность|трудность|неудобство|"
    r"что мешает|что мешало|что мешало бы|"
    r"не хватает|недостаток|не хватало)\b",
    re.IGNORECASE,
)

# I (Implication) - Извлекающие вопросы
# Усиливаем понимание последствий проблемы
_RE_SPIN_I = re.compile(
    r"\b(к чему это приводит|к чему приводит|что это значит|"
    r"как это влияет|как влияет|как это сказывается|"
    r"что будет если|что будет когда|что произойдёт|"
    r"последствия|влияние|результат|"
    r"как это отражается|как отражается|"
    r"из-за этого|поэтому|в результате|"
    r"это означает|это значит|это приведёт)\b",
    re.IGNORECASE,
)

# N (Need-payoff) - Вопросы о выгодах
# Помогаем клиенту увидеть выгоды решения
_RE_SPIN_N = re.compile(
    r"\b(как это поможет|как поможет|что это даст|что даст|"
    r"зачем это нужно|зачем нужно|для чего|"
    r"выгода|преимущество|польза|полезность|"
    r"это позволит|это даст возможность|это поможет|"
    r"важно для вас|важно ли|нужно ли|"
    r"будет удобнее|будет лучше|будет проще|"
    r"решит проблему|решит вопрос|поможет решить|"
    r"сэкономит|упростит|ускорит|улучшит)\b",
    re.IGNORECASE,
)


def _count_spin_stage(text: str, pattern: re.Pattern) -> int:
    """Считает количество упоминаний этапа SPIN в тексте."""
    if not text:
        return 0
    matches = pattern.findall(text)
    return len(matches)


def _has_question_mark(text: str) -> bool:
    """Проверяет наличие знака вопроса."""
    return "?" in text if text else False


def _detect_spin_stage(text: str) -> Tuple[int, int, int, int]:
    """
    Определяет этапы SPIN в тексте.
    Возвращает (S, P, I, N) - количество упоминаний каждого этапа.
    """
    s = _count_spin_stage(text, _RE_SPIN_S)
    p = _count_spin_stage(text, _RE_SPIN_P)
    i = _count_spin_stage(text, _RE_SPIN_I)
    n = _count_spin_stage(text, _RE_SPIN_N)
    return (s, p, i, n)


@dataclass
class ChatSpinMetrics:
    """Метрики SPIN для одного чата."""
    chat_id: str
    manager_id: str
    manager_name: str
    total_messages: int
    total_questions: int
    spin_s_count: int  # ситуационные вопросы
    spin_p_count: int  # проблемные вопросы
    spin_i_count: int  # извлекающие вопросы
    spin_n_count: int  # вопросы о выгодах
    has_situation: bool
    has_problem: bool
    has_implication: bool
    has_need_payoff: bool
    spin_completeness: float  # 0-1, насколько полный SPIN-цикл


def analyze_chat_spin(
    chat_id: str,
    messages: List[Dict[str, Any]],
    manager_id: str,
    manager_name: str,
) -> ChatSpinMetrics:
    """Анализирует один чат по SPIN-методике."""
    manager_messages = [
        m for m in messages
        if m.get("direction") == "out" and str(m.get("manager_id", "")) == str(manager_id)
    ]
    
    total_messages = len(manager_messages)
    total_questions = sum(1 for m in manager_messages if _has_question_mark(str(m.get("text", ""))))
    
    # Собираем все тексты менеджера
    all_text = " ".join([str(m.get("text", "")) for m in manager_messages])
    
    # Определяем этапы SPIN
    s, p, i, n = _detect_spin_stage(all_text)
    
    has_situation = s > 0
    has_problem = p > 0
    has_implication = i > 0
    has_need_payoff = n > 0
    
    # Полнота SPIN-цикла: сколько этапов использовано из 4
    stages_used = sum([has_situation, has_problem, has_implication, has_need_payoff])
    spin_completeness = stages_used / 4.0 if stages_used > 0 else 0.0
    
    return ChatSpinMetrics(
        chat_id=chat_id,
        manager_id=manager_id,
        manager_name=manager_name,
        total_messages=total_messages,
        total_questions=total_questions,
        spin_s_count=s,
        spin_p_count=p,
        spin_i_count=i,
        spin_n_count=n,
        has_situation=has_situation,
        has_problem=has_problem,
        has_implication=has_implication,
        has_need_payoff=has_need_payoff,
        spin_completeness=spin_completeness,
    )


def aggregate_manager_spin(chat_metrics: List[ChatSpinMetrics]) -> Dict[str, Dict[str, Any]]:
    """Агрегирует метрики SPIN по менеджерам."""
    by_manager: Dict[str, List[ChatSpinMetrics]] = {}
    for m in chat_metrics:
        key = f"{m.manager_id}|{m.manager_name}"
        by_manager.setdefault(key, []).append(m)
    
    result: Dict[str, Dict[str, Any]] = {}
    
    for key, metrics in by_manager.items():
        manager_id, manager_name = key.split("|", 1)
        
        total_chats = len(metrics)
        total_messages = sum(m.total_messages for m in metrics)
        total_questions = sum(m.total_questions for m in metrics)
        
        # SPIN-метрики
        total_s = sum(m.spin_s_count for m in metrics)
        total_p = sum(m.spin_p_count for m in metrics)
        total_i = sum(m.spin_i_count for m in metrics)
        total_n = sum(m.spin_n_count for m in metrics)
        
        # Доли чатов с использованием каждого этапа
        chats_with_s = sum(1 for m in metrics if m.has_situation)
        chats_with_p = sum(1 for m in metrics if m.has_problem)
        chats_with_i = sum(1 for m in metrics if m.has_implication)
        chats_with_n = sum(1 for m in metrics if m.has_need_payoff)
        
        # Средняя полнота SPIN-цикла
        avg_completeness = sum(m.spin_completeness for m in metrics) / total_chats if total_chats > 0 else 0.0
        
        # Среднее количество вопросов на чат
        avg_questions_per_chat = total_questions / total_chats if total_chats > 0 else 0.0
        
        result[key] = {
            "manager_id": manager_id,
            "manager_name": manager_name,
            "total_chats": total_chats,
            "total_messages": total_messages,
            "total_questions": total_questions,
            "avg_questions_per_chat": round(avg_questions_per_chat, 2),
            "spin_s_total": total_s,
            "spin_p_total": total_p,
            "spin_i_total": total_i,
            "spin_n_total": total_n,
            "spin_s_per_chat": round(total_s / total_chats, 2) if total_chats > 0 else 0.0,
            "spin_p_per_chat": round(total_p / total_chats, 2) if total_chats > 0 else 0.0,
            "spin_i_per_chat": round(total_i / total_chats, 2) if total_chats > 0 else 0.0,
            "spin_n_per_chat": round(total_n / total_chats, 2) if total_chats > 0 else 0.0,
            "chats_with_s": chats_with_s,
            "chats_with_p": chats_with_p,
            "chats_with_i": chats_with_i,
            "chats_with_n": chats_with_n,
            "s_usage_rate": round(chats_with_s / total_chats * 100, 1) if total_chats > 0 else 0.0,
            "p_usage_rate": round(chats_with_p / total_chats * 100, 1) if total_chats > 0 else 0.0,
            "i_usage_rate": round(chats_with_i / total_chats * 100, 1) if total_chats > 0 else 0.0,
            "n_usage_rate": round(chats_with_n / total_chats * 100, 1) if total_chats > 0 else 0.0,
            "avg_spin_completeness": round(avg_completeness * 100, 1),
            "full_spin_chats": sum(1 for m in metrics if m.spin_completeness == 1.0),
            "full_spin_rate": round(sum(1 for m in metrics if m.spin_completeness == 1.0) / total_chats * 100, 1) if total_chats > 0 else 0.0,
        }
    
    return result


def generate_improvement_plan(manager_stats: Dict[str, Any], benchmark: Optional[Dict[str, Any]] = None) -> List[str]:
    """Генерирует план улучшения для менеджера на основе SPIN-метрик."""
    plan: List[str] = []
    
    name = manager_stats.get("manager_name", "Менеджер")
    s_rate = manager_stats.get("s_usage_rate", 0)
    p_rate = manager_stats.get("p_usage_rate", 0)
    i_rate = manager_stats.get("i_usage_rate", 0)
    n_rate = manager_stats.get("n_usage_rate", 0)
    completeness = manager_stats.get("avg_spin_completeness", 0)
    avg_questions = manager_stats.get("avg_questions_per_chat", 0)
    
    plan.append(f"📊 Анализ работы {name}")
    plan.append("")
    
    # Этап S (Situation)
    if s_rate < 50:
        plan.append("🔴 ЭТАП S (Ситуационные вопросы) — КРИТИЧЕСКИЙ ПРОВАЛ")
        plan.append(f"   Используется только в {s_rate}% чатов (цель: ≥80%)")
        plan.append("   ШАГИ УЛУЧШЕНИЯ:")
        plan.append("   1. В начале каждого диалога задавать 2-3 ситуационных вопроса:")
        plan.append("      - 'Какой размер вам нужен?'")
        plan.append("      - 'Для кого выбираете?'")
        plan.append("      - 'Когда планируете использовать?'")
        plan.append("   2. Использовать шаблон: 'Подскажите, [ситуационный вопрос]?'")
        plan.append("   3. Не переходить к предложению без понимания ситуации клиента")
        plan.append("")
    elif s_rate < 70:
        plan.append("🟡 ЭТАП S (Ситуационные вопросы) — НУЖНО УЛУЧШИТЬ")
        plan.append(f"   Используется в {s_rate}% чатов (цель: ≥80%)")
        plan.append("   ШАГИ УЛУЧШЕНИЯ:")
        plan.append("   1. Увеличить частоту ситуационных вопросов в начале диалога")
        plan.append("   2. Задавать минимум 2 ситуационных вопроса перед предложением")
        plan.append("")
    else:
        plan.append("🟢 ЭТАП S (Ситуационные вопросы) — ХОРОШО")
        plan.append(f"   Используется в {s_rate}% чатов")
        plan.append("")
    
    # Этап P (Problem)
    if p_rate < 30:
        plan.append("🔴 ЭТАП P (Проблемные вопросы) — КРИТИЧЕСКИЙ ПРОВАЛ")
        plan.append(f"   Используется только в {p_rate}% чатов (цель: ≥60%)")
        plan.append("   ШАГИ УЛУЧШЕНИЯ:")
        plan.append("   1. После ситуационных вопросов выявлять проблемы:")
        plan.append("      - 'Что не устраивает в текущем?'")
        plan.append("      - 'Какие сложности возникают?'")
        plan.append("      - 'Что хотелось бы улучшить?'")
        plan.append("   2. Не предлагать решение без понимания проблемы")
        plan.append("   3. Использовать формулировки: 'Что вас беспокоит?', 'Какие трудности?'")
        plan.append("")
    elif p_rate < 50:
        plan.append("🟡 ЭТАП P (Проблемные вопросы) — НУЖНО УЛУЧШИТЬ")
        plan.append(f"   Используется в {p_rate}% чатов (цель: ≥60%)")
        plan.append("   ШАГИ УЛУЧШЕНИЯ:")
        plan.append("   1. Чаще задавать вопросы о проблемах и неудовлетворённости")
        plan.append("   2. Глубже копать: 'А что именно не устраивает?'")
        plan.append("")
    else:
        plan.append("🟢 ЭТАП P (Проблемные вопросы) — ХОРОШО")
        plan.append(f"   Используется в {p_rate}% чатов")
        plan.append("")
    
    # Этап I (Implication)
    if i_rate < 20:
        plan.append("🔴 ЭТАП I (Извлекающие вопросы) — КРИТИЧЕСКИЙ ПРОВАЛ")
        plan.append(f"   Используется только в {i_rate}% чатов (цель: ≥40%)")
        plan.append("   ШАГИ УЛУЧШЕНИЯ:")
        plan.append("   1. После выявления проблемы усиливать её значимость:")
        plan.append("      - 'К чему это приводит?'")
        plan.append("      - 'Как это влияет на вас?'")
        plan.append("      - 'Что будет, если не решить?'")
        plan.append("   2. Помогать клиенту увидеть последствия проблемы")
        plan.append("   3. Использовать: 'Из-за этого...', 'Это значит, что...'")
        plan.append("")
    elif i_rate < 35:
        plan.append("🟡 ЭТАП I (Извлекающие вопросы) — НУЖНО УЛУЧШИТЬ")
        plan.append(f"   Используется в {i_rate}% чатов (цель: ≥40%)")
        plan.append("   ШАГИ УЛУЧШЕНИЯ:")
        plan.append("   1. Чаще задавать вопросы о последствиях проблем")
        plan.append("   2. Усиливать значимость выявленных проблем")
        plan.append("")
    else:
        plan.append("🟢 ЭТАП I (Извлекающие вопросы) — ХОРОШО")
        plan.append(f"   Используется в {i_rate}% чатов")
        plan.append("")
    
    # Этап N (Need-payoff)
    if n_rate < 30:
        plan.append("🔴 ЭТАП N (Вопросы о выгодах) — КРИТИЧЕСКИЙ ПРОВАЛ")
        plan.append(f"   Используется только в {n_rate}% чатов (цель: ≥60%)")
        plan.append("   ШАГИ УЛУЧШЕНИЯ:")
        plan.append("   1. Перед предложением показывать выгоды решения:")
        plan.append("      - 'Как это поможет вам?'")
        plan.append("      - 'Что это даст?'")
        plan.append("      - 'Зачем это нужно?'")
        plan.append("   2. Помогать клиенту самому назвать выгоды")
        plan.append("   3. Использовать: 'Это позволит...', 'Вы сможете...'")
        plan.append("")
    elif n_rate < 50:
        plan.append("🟡 ЭТАП N (Вопросы о выгодах) — НУЖНО УЛУЧШИТЬ")
        plan.append(f"   Используется в {n_rate}% чатов (цель: ≥60%)")
        plan.append("   ШАГИ УЛУЧШЕНИЯ:")
        plan.append("   1. Чаще показывать выгоды решения")
        plan.append("   2. Помогать клиенту увидеть ценность предложения")
        plan.append("")
    else:
        plan.append("🟢 ЭТАП N (Вопросы о выгодах) — ХОРОШО")
        plan.append(f"   Используется в {n_rate}% чатов")
        plan.append("")
    
    # Общая полнота SPIN
    if completeness < 50:
        plan.append("🔴 ПОЛНОТА SPIN-ЦИКЛА — КРИТИЧЕСКИЙ ПРОВАЛ")
        plan.append(f"   Средняя полнота: {completeness}% (цель: ≥75%)")
        plan.append("   ПРОБЛЕМА: Менеджер не проходит полный цикл SPIN")
        plan.append("   ШАГИ УЛУЧШЕНИЯ:")
        plan.append("   1. Следовать последовательности: S → P → I → N")
        plan.append("   2. Не пропускать этапы")
        plan.append("   3. Использовать чек-лист перед каждым диалогом")
        plan.append("")
    elif completeness < 70:
        plan.append("🟡 ПОЛНОТА SPIN-ЦИКЛА — НУЖНО УЛУЧШИТЬ")
        plan.append(f"   Средняя полнота: {completeness}% (цель: ≥75%)")
        plan.append("   ШАГИ УЛУЧШЕНИЯ:")
        plan.append("   1. Стараться использовать все 4 этапа в каждом диалоге")
        plan.append("   2. Не пропускать этапы I и N")
        plan.append("")
    else:
        plan.append("🟢 ПОЛНОТА SPIN-ЦИКЛА — ХОРОШО")
        plan.append(f"   Средняя полнота: {completeness}%")
        plan.append("")
    
    # Количество вопросов
    if avg_questions < 3:
        plan.append("🔴 КОЛИЧЕСТВО ВОПРОСОВ — КРИТИЧЕСКИЙ ПРОВАЛ")
        plan.append(f"   Среднее количество вопросов на чат: {avg_questions} (цель: ≥5)")
        plan.append("   ШАГИ УЛУЧШЕНИЯ:")
        plan.append("   1. Задавать минимум 5 вопросов в каждом диалоге")
        plan.append("   2. Больше слушать, меньше говорить")
        plan.append("   3. Использовать открытые вопросы вместо закрытых")
        plan.append("")
    elif avg_questions < 4:
        plan.append("🟡 КОЛИЧЕСТВО ВОПРОСОВ — НУЖНО УЛУЧШИТЬ")
        plan.append(f"   Среднее количество вопросов на чат: {avg_questions} (цель: ≥5)")
        plan.append("   ШАГИ УЛУЧШЕНИЯ:")
        plan.append("   1. Увеличить количество вопросов в диалоге")
        plan.append("   2. Задавать уточняющие вопросы")
        plan.append("")
    
    return plan


def main() -> None:
    base = Path(__file__).resolve().parent
    env = _load_env(base / "env")
    
    ss = open_spreadsheet(
        spreadsheet_id=env["GOOGLE_SHEETS_ID"],
        service_account_json_path=env["GOOGLE_SERVICE_ACCOUNT_JSON"],
    )
    
    # Читаем данные
    print("Читаю данные из Google Sheets...")
    chats = _read_table(ss.worksheet("chats_raw"))
    messages = _read_table(ss.worksheet("messages_raw"))
    
    print(f"Загружено чатов: {len(chats)}, сообщений: {len(messages)}")
    
    # Группируем сообщения по чатам
    messages_by_chat: Dict[str, List[Dict[str, Any]]] = {}
    for msg in messages:
        chat_id = str(msg.get("chat_id", ""))
        if chat_id:
            messages_by_chat.setdefault(chat_id, []).append(msg)
    
    # Анализируем каждый чат
    print("Анализирую чаты по SPIN-методике...")
    chat_metrics: List[ChatSpinMetrics] = []
    
    for chat in chats:
        chat_id = str(chat.get("chat_id", ""))
        manager_id = str(chat.get("manager_id", ""))
        manager_name = str(chat.get("manager_name", ""))
        
        if not chat_id or not manager_id:
            continue
        
        chat_messages = messages_by_chat.get(chat_id, [])
        if not chat_messages:
            continue
        
        metrics = analyze_chat_spin(chat_id, chat_messages, manager_id, manager_name)
        chat_metrics.append(metrics)
    
    print(f"Проанализировано чатов: {len(chat_metrics)}")
    
    # Агрегируем по менеджерам
    manager_stats = aggregate_manager_spin(chat_metrics)
    
    # Выводим результаты
    print("\n" + "="*80)
    print("РЕЗУЛЬТАТЫ SPIN-АНАЛИЗА")
    print("="*80 + "\n")
    
    manager_list = list(manager_stats.values())
    manager_list.sort(key=lambda x: x.get("total_chats", 0), reverse=True)
    
    for stats in manager_list:
        name = stats.get("manager_name", "Неизвестно")
        print(f"\n{'='*80}")
        print(f"МЕНЕДЖЕР: {name}")
        print(f"{'='*80}")
        print(f"Всего чатов: {stats.get('total_chats', 0)}")
        print(f"Всего сообщений: {stats.get('total_messages', 0)}")
        print(f"Среднее вопросов на чат: {stats.get('avg_questions_per_chat', 0)}")
        print(f"\nSPIN-метрики:")
        print(f"  S (Ситуационные): {stats.get('s_usage_rate', 0)}% чатов")
        print(f"  P (Проблемные): {stats.get('p_usage_rate', 0)}% чатов")
        print(f"  I (Извлекающие): {stats.get('i_usage_rate', 0)}% чатов")
        print(f"  N (Выгоды): {stats.get('n_usage_rate', 0)}% чатов")
        print(f"  Полнота SPIN-цикла: {stats.get('avg_spin_completeness', 0)}%")
        print(f"  Полных SPIN-циклов: {stats.get('full_spin_rate', 0)}%")
    
    # Сравнение менеджеров
    if len(manager_list) >= 2:
        print(f"\n{'='*80}")
        print("СРАВНЕНИЕ МЕНЕДЖЕРОВ")
        print(f"{'='*80}\n")
        
        m1, m2 = manager_list[0], manager_list[1]
        print(f"{m1.get('manager_name', 'М1'):<30} | {m2.get('manager_name', 'М2'):<30}")
        print("-" * 65)
        print(f"S (Ситуационные): {m1.get('s_usage_rate', 0):>5.1f}% | {m2.get('s_usage_rate', 0):>5.1f}%")
        print(f"P (Проблемные):   {m1.get('p_usage_rate', 0):>5.1f}% | {m2.get('p_usage_rate', 0):>5.1f}%")
        print(f"I (Извлекающие):  {m1.get('i_usage_rate', 0):>5.1f}% | {m2.get('i_usage_rate', 0):>5.1f}%")
        print(f"N (Выгоды):       {m1.get('n_usage_rate', 0):>5.1f}% | {m2.get('n_usage_rate', 0):>5.1f}%")
        print(f"Полнота SPIN:     {m1.get('avg_spin_completeness', 0):>5.1f}% | {m2.get('avg_spin_completeness', 0):>5.1f}%")
        print(f"Вопросов/чат:     {m1.get('avg_questions_per_chat', 0):>5.2f} | {m2.get('avg_questions_per_chat', 0):>5.2f}")
    
    # Генерируем планы улучшения
    print(f"\n{'='*80}")
    print("ПЛАНЫ УЛУЧШЕНИЯ")
    print(f"{'='*80}\n")
    
    improvement_plans: List[Dict[str, Any]] = []
    
    for stats in manager_list:
        name = stats.get("manager_name", "Неизвестно")
        plan_lines = generate_improvement_plan(stats)
        improvement_plans.append({
            "manager_name": name,
            "plan": "\n".join(plan_lines),
        })
        
        print("\n".join(plan_lines))
        print("\n" + "-"*80 + "\n")
    
    # Записываем в Google Sheets
    print("Записываю результаты в Google Sheets...")
    
    # Лист с метриками по менеджерам
    manager_rows = []
    for stats in manager_list:
        manager_rows.append({
            "manager_id": stats.get("manager_id", ""),
            "manager_name": stats.get("manager_name", ""),
            "total_chats": stats.get("total_chats", 0),
            "total_messages": stats.get("total_messages", 0),
            "total_questions": stats.get("total_questions", 0),
            "avg_questions_per_chat": stats.get("avg_questions_per_chat", 0),
            "spin_s_total": stats.get("spin_s_total", 0),
            "spin_p_total": stats.get("spin_p_total", 0),
            "spin_i_total": stats.get("spin_i_total", 0),
            "spin_n_total": stats.get("spin_n_total", 0),
            "spin_s_per_chat": stats.get("spin_s_per_chat", 0),
            "spin_p_per_chat": stats.get("spin_p_per_chat", 0),
            "spin_i_per_chat": stats.get("spin_i_per_chat", 0),
            "spin_n_per_chat": stats.get("spin_n_per_chat", 0),
            "s_usage_rate": stats.get("s_usage_rate", 0),
            "p_usage_rate": stats.get("p_usage_rate", 0),
            "i_usage_rate": stats.get("i_usage_rate", 0),
            "n_usage_rate": stats.get("n_usage_rate", 0),
            "avg_spin_completeness": stats.get("avg_spin_completeness", 0),
            "full_spin_chats": stats.get("full_spin_chats", 0),
            "full_spin_rate": stats.get("full_spin_rate", 0),
        })
    
    upsert_worksheet(
        ss,
        "spin_manager_metrics",
        rows=dicts_to_table(
            manager_rows,
            header=[
                "manager_id", "manager_name", "total_chats", "total_messages",
                "total_questions", "avg_questions_per_chat",
                "spin_s_total", "spin_p_total", "spin_i_total", "spin_n_total",
                "spin_s_per_chat", "spin_p_per_chat", "spin_i_per_chat", "spin_n_per_chat",
                "s_usage_rate", "p_usage_rate", "i_usage_rate", "n_usage_rate",
                "avg_spin_completeness", "full_spin_chats", "full_spin_rate",
            ],
        ),
    )
    
    # Лист с метриками по чатам
    chat_rows = []
    for m in chat_metrics:
        chat_rows.append({
            "chat_id": m.chat_id,
            "manager_id": m.manager_id,
            "manager_name": m.manager_name,
            "total_messages": m.total_messages,
            "total_questions": m.total_questions,
            "spin_s_count": m.spin_s_count,
            "spin_p_count": m.spin_p_count,
            "spin_i_count": m.spin_i_count,
            "spin_n_count": m.spin_n_count,
            "has_situation": "Да" if m.has_situation else "Нет",
            "has_problem": "Да" if m.has_problem else "Нет",
            "has_implication": "Да" if m.has_implication else "Нет",
            "has_need_payoff": "Да" if m.has_need_payoff else "Нет",
            "spin_completeness": f"{m.spin_completeness * 100:.1f}%",
        })
    
    upsert_worksheet(
        ss,
        "spin_chat_metrics",
        rows=dicts_to_table(
            chat_rows,
            header=[
                "chat_id", "manager_id", "manager_name",
                "total_messages", "total_questions",
                "spin_s_count", "spin_p_count", "spin_i_count", "spin_n_count",
                "has_situation", "has_problem", "has_implication", "has_need_payoff",
                "spin_completeness",
            ],
        ),
    )
    
    # Лист с планами улучшения
    plan_rows = []
    for plan_data in improvement_plans:
        plan_rows.append({
            "manager_name": plan_data["manager_name"],
            "improvement_plan": plan_data["plan"],
        })
    
    upsert_worksheet(
        ss,
        "spin_improvement_plans",
        rows=dicts_to_table(
            plan_rows,
            header=["manager_name", "improvement_plan"],
        ),
    )
    
    print("\n✅ Анализ завершён! Результаты записаны в Google Sheets:")
    print("   - spin_manager_metrics (метрики по менеджерам)")
    print("   - spin_chat_metrics (метрики по чатам)")
    print("   - spin_improvement_plans (планы улучшения)")


if __name__ == "__main__":
    main()




