from __future__ import annotations

"""
Анализ основных проблем менеджеров и создание системы еженедельных метрик.

Выделяет 3-5 ключевых проблем для каждого менеджера на основе:
- SPIN-анализа
- Скорости ответа
- Количества вопросов
- Follow-up
- Конверсии (если доступна)
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


@dataclass
class ManagerProblems:
    """Основные проблемы менеджера с приоритетами."""
    manager_id: str
    manager_name: str
    problems: List[Dict[str, Any]]  # [{priority, name, current_value, target_value, impact, description}]
    weekly_metrics: List[Dict[str, Any]]  # [{metric_name, current, target, formula, check_frequency}]
    improvement_steps: List[Dict[str, Any]]  # [{step_number, description, expected_result, timeframe}]


def analyze_manager_problems(
    manager_name: str,
    manager_id: str,
    spin_stats: Dict[str, Any],
    manager_summary: Optional[Dict[str, Any]] = None,
    chats_data: Optional[List[Dict[str, Any]]] = None,
) -> ManagerProblems:
    """Анализирует проблемы менеджера и создаёт план улучшения."""
    
    problems: List[Dict[str, Any]] = []
    weekly_metrics: List[Dict[str, Any]] = []
    improvement_steps: List[Dict[str, Any]] = []
    
    # Проблема 1: Критически низкое использование этапов P, I, N
    p_rate = _to_float(spin_stats.get("p_usage_rate", 0)) or 0
    i_rate = _to_float(spin_stats.get("i_usage_rate", 0)) or 0
    n_rate = _to_float(spin_stats.get("n_usage_rate", 0)) or 0
    spin_completeness = _to_float(spin_stats.get("avg_spin_completeness", 0)) or 0
    
    if p_rate < 10 or i_rate < 10 or n_rate < 10 or spin_completeness < 30:
        problems.append({
            "priority": 1,
            "name": "Неполный SPIN-цикл (пропуск этапов P, I, N)",
            "current_value": f"P:{p_rate}%, I:{i_rate}%, N:{n_rate}%, Полнота:{spin_completeness}%",
            "target_value": "P:≥60%, I:≥40%, N:≥60%, Полнота:≥75%",
            "impact": "КРИТИЧЕСКИЙ",
            "description": "Менеджер не использует проблемные, извлекающие вопросы и вопросы о выгодах. Это снижает понимание потребностей клиента и конверсию.",
            "severity_score": 100 - spin_completeness,  # Чем ниже полнота, тем выше приоритет
        })
        
        weekly_metrics.append({
            "metric_name": "SPIN Полнота цикла (%)",
            "current": spin_completeness,
            "target_week1": spin_completeness + 10,
            "target_week2": spin_completeness + 20,
            "target_week4": 75,
            "formula": "Средняя полнота SPIN-цикла по всем чатам",
            "check_frequency": "Еженедельно",
            "measurement": "spin_analysis.py → avg_spin_completeness",
        })
        
        weekly_metrics.append({
            "metric_name": "Использование этапа P (%)",
            "current": p_rate,
            "target_week1": max(10, p_rate + 5),
            "target_week2": max(20, p_rate + 10),
            "target_week4": 60,
            "formula": "Доля чатов с проблемными вопросами",
            "check_frequency": "Еженедельно",
        })
        
        weekly_metrics.append({
            "metric_name": "Использование этапа I (%)",
            "current": i_rate,
            "target_week1": max(5, i_rate + 3),
            "target_week2": max(10, i_rate + 6),
            "target_week4": 40,
            "formula": "Доля чатов с извлекающими вопросами",
            "check_frequency": "Еженедельно",
        })
        
        weekly_metrics.append({
            "metric_name": "Использование этапа N (%)",
            "current": n_rate,
            "target_week1": max(10, n_rate + 5),
            "target_week2": max(20, n_rate + 10),
            "target_week4": 60,
            "formula": "Доля чатов с вопросами о выгодах",
            "check_frequency": "Еженедельно",
        })
        
        improvement_steps.extend([
            {
                "step_number": 1,
                "description": "Неделя 1-2: Внедрить этап P (Проблемные вопросы)",
                "actions": [
                    "После ситуационных вопросов задавать: 'Что не устраивает в текущем?'",
                    "Использовать минимум 1 проблемный вопрос в 30% чатов",
                    "Шаблон: 'Какие сложности возникают?', 'Что хотелось бы улучшить?'",
                ],
                "expected_result": "P-этап используется в ≥30% чатов",
                "timeframe": "2 недели",
                "success_criteria": "p_usage_rate ≥ 30%",
            },
            {
                "step_number": 2,
                "description": "Неделя 3-4: Внедрить этап I (Извлекающие вопросы)",
                "actions": [
                    "После выявления проблемы усиливать её: 'К чему это приводит?'",
                    "Использовать минимум 1 извлекающий вопрос в 20% чатов",
                    "Шаблон: 'Как это влияет на вас?', 'Что будет, если не решить?'",
                ],
                "expected_result": "I-этап используется в ≥20% чатов",
                "timeframe": "2 недели",
                "success_criteria": "i_usage_rate ≥ 20%",
            },
            {
                "step_number": 3,
                "description": "Неделя 5-6: Внедрить этап N (Вопросы о выгодах)",
                "actions": [
                    "Перед предложением показывать выгоды: 'Как это поможет вам?'",
                    "Использовать минимум 1 вопрос о выгодах в 30% чатов",
                    "Шаблон: 'Что это даст?', 'Зачем это нужно?'",
                ],
                "expected_result": "N-этап используется в ≥30% чатов",
                "timeframe": "2 недели",
                "success_criteria": "n_usage_rate ≥ 30%",
            },
            {
                "step_number": 4,
                "description": "Неделя 7-8: Полный SPIN-цикл в каждом диалоге",
                "actions": [
                    "Использовать все 4 этапа (S→P→I→N) в каждом диалоге",
                    "Использовать чек-лист перед каждым чатом",
                    "Не пропускать этапы",
                ],
                "expected_result": "Полнота SPIN-цикла ≥75%",
                "timeframe": "2 недели",
                "success_criteria": "avg_spin_completeness ≥ 75%",
            },
        ])
    
    # Проблема 2: Мало вопросов в диалогах
    avg_questions = _to_float(spin_stats.get("avg_questions_per_chat", 0)) or 0
    if avg_questions < 4:
        problems.append({
            "priority": 2,
            "name": "Недостаточно вопросов в диалогах",
            "current_value": f"{avg_questions:.2f} вопросов на чат",
            "target_value": "≥5 вопросов на чат",
            "impact": "ВЫСОКИЙ",
            "description": "Менеджер задаёт мало вопросов, что снижает понимание потребностей клиента и качество консультации.",
            "severity_score": (5 - avg_questions) * 20,  # Чем меньше вопросов, тем выше приоритет
        })
        
        weekly_metrics.append({
            "metric_name": "Среднее количество вопросов на чат",
            "current": avg_questions,
            "target_week1": avg_questions + 0.5,
            "target_week2": avg_questions + 1.0,
            "target_week4": 5.0,
            "formula": "Сумма вопросов / Количество чатов",
            "check_frequency": "Еженедельно",
        })
        
        improvement_steps.append({
            "step_number": len(improvement_steps) + 1,
            "description": "Увеличить количество вопросов в каждом диалоге",
            "actions": [
                "Задавать минимум 5 вопросов в каждом диалоге",
                "После каждого ответа клиента задавать уточняющий вопрос",
                "Использовать открытые вопросы вместо закрытых",
                "Разбивать длинные монологи на вопросы",
            ],
            "expected_result": "≥5 вопросов на чат",
            "timeframe": "4 недели",
            "success_criteria": "avg_questions_per_chat ≥ 5.0",
        })
    
    # Проблема 3: Недостаточное использование ситуационных вопросов
    s_rate = _to_float(spin_stats.get("s_usage_rate", 0)) or 0
    if s_rate < 70:
        problems.append({
            "priority": 3,
            "name": "Недостаточно ситуационных вопросов",
            "current_value": f"{s_rate}% чатов",
            "target_value": "≥80% чатов",
            "impact": "СРЕДНИЙ",
            "description": "Менеджер не всегда выясняет ситуацию клиента перед предложением, что снижает точность рекомендаций.",
            "severity_score": (80 - s_rate) * 2,
        })
        
        weekly_metrics.append({
            "metric_name": "Использование этапа S (%)",
            "current": s_rate,
            "target_week1": min(80, s_rate + 5),
            "target_week2": min(80, s_rate + 10),
            "target_week4": 80,
            "formula": "Доля чатов с ситуационными вопросами",
            "check_frequency": "Еженедельно",
        })
        
        improvement_steps.append({
            "step_number": len(improvement_steps) + 1,
            "description": "Увеличить использование ситуационных вопросов",
            "actions": [
                "В начале каждого диалога задавать 2-3 ситуационных вопроса",
                "Не переходить к предложению без понимания ситуации",
                "Шаблоны: 'Какой размер?', 'Для кого?', 'Когда?'",
            ],
            "expected_result": "S-этап используется в ≥80% чатов",
            "timeframe": "2 недели",
            "success_criteria": "s_usage_rate ≥ 80%",
        })
    
    # Проблема 4: Скорость ответа (если есть данные)
    if manager_summary:
        median_response = _to_int(manager_summary.get("median_first_reply_sec"))
        response_rate = _to_float(manager_summary.get("response_rate"))
        no_reply_chats = _to_int(manager_summary.get("no_reply_chats", 0))
        total_chats = _to_int(manager_summary.get("chats", 0))
        
        if median_response and median_response > 10 * 60:  # > 10 минут
            problems.append({
                "priority": 4,
                "name": "Медленная скорость первого ответа",
                "current_value": f"{median_response / 60:.1f} минут (медиана)",
                "target_value": "≤10 минут (медиана)",
                "impact": "ВЫСОКИЙ",
                "description": "Менеджер отвечает слишком медленно, что снижает конверсию и удовлетворённость клиентов.",
                "severity_score": max(0, (median_response - 600) / 60 * 10),  # За каждую минуту сверх 10
            })
            
            weekly_metrics.append({
                "metric_name": "Медианное время первого ответа (минуты)",
                "current": median_response / 60 if median_response else None,
                "target_week1": max(10, (median_response / 60) - 2) if median_response else None,
                "target_week2": max(10, (median_response / 60) - 4) if median_response else None,
                "target_week4": 10.0,
                "formula": "Медианное время между первым входящим и первым исходящим сообщением",
                "check_frequency": "Еженедельно",
            })
            
            improvement_steps.append({
                "step_number": len(improvement_steps) + 1,
                "description": "Сократить время первого ответа",
                "actions": [
                    "Использовать быстрый шаблон приветствия",
                    "Отвечать в течение 5 минут на новые сообщения",
                    "Настроить уведомления",
                    "Использовать готовые ответы для частых вопросов",
                ],
                "expected_result": "Медианное время ответа ≤10 минут",
                "timeframe": "2 недели",
                "success_criteria": "median_first_reply_sec ≤ 600",
            })
        
        if response_rate and response_rate < 0.95:  # < 95%
            no_reply_rate = (no_reply_chats / total_chats * 100) if total_chats else 0
            if no_reply_rate > 5:
                problems.append({
                    "priority": 5,
                    "name": "Высокий процент чатов без ответа",
                    "current_value": f"{no_reply_rate:.1f}% чатов без ответа",
                    "target_value": "≤5% чатов без ответа",
                    "impact": "КРИТИЧЕСКИЙ",
                    "description": "Менеджер не отвечает на часть чатов, что приводит к потере клиентов.",
                    "severity_score": no_reply_rate * 2,  # Чем выше %, тем выше приоритет
                })
                
                weekly_metrics.append({
                    "metric_name": "Доля чатов без ответа (%)",
                    "current": no_reply_rate,
                    "target_week1": max(5, no_reply_rate - 2),
                    "target_week2": max(5, no_reply_rate - 4),
                    "target_week4": 5.0,
                    "formula": "(Количество чатов без ответа / Всего чатов) * 100",
                    "check_frequency": "Еженедельно",
                })
                
                improvement_steps.append({
                    "step_number": len(improvement_steps) + 1,
                    "description": "Снизить количество чатов без ответа",
                    "actions": [
                        "Проверять все новые чаты каждый час",
                        "Использовать систему напоминаний",
                        "Отвечать на все входящие сообщения",
                        "Настроить автоматические уведомления",
                    ],
                    "expected_result": "≤5% чатов без ответа",
                    "timeframe": "2 недели",
                    "success_criteria": "no_reply_rate ≤ 5%",
                })
    
    # Сортируем проблемы по приоритету
    problems.sort(key=lambda x: (x["priority"], -x.get("severity_score", 0)))
    
    # Ограничиваем до топ-5
    problems = problems[:5]
    
    return ManagerProblems(
        manager_id=manager_id,
        manager_name=manager_name,
        problems=problems,
        weekly_metrics=weekly_metrics,
        improvement_steps=improvement_steps,
    )


def main() -> None:
    base = Path(__file__).resolve().parent
    env = _load_env(base / "env")
    
    ss = open_spreadsheet(
        spreadsheet_id=env["GOOGLE_SHEETS_ID"],
        service_account_json_path=env["GOOGLE_SERVICE_ACCOUNT_JSON"],
    )
    
    print("Читаю данные из Google Sheets...")
    
    # Читаем SPIN-метрики
    spin_metrics = _read_table(ss.worksheet("spin_manager_metrics"))
    manager_summary_data = _read_table(ss.worksheet("manager_summary"))
    
    # Создаём словарь для быстрого доступа
    manager_summary_by_id: Dict[str, Dict[str, Any]] = {}
    for m in manager_summary_data:
        mid = str(m.get("manager_id", ""))
        if mid:
            manager_summary_by_id[mid] = m
    
    print(f"Найдено менеджеров в SPIN-метриках: {len(spin_metrics)}")
    
    all_problems: List[Dict[str, Any]] = []
    all_weekly_metrics: List[Dict[str, Any]] = []
    all_improvement_steps: List[Dict[str, Any]] = []
    
    for spin_stat in spin_metrics:
        manager_id = str(spin_stat.get("manager_id", ""))
        manager_name = str(spin_stat.get("manager_name", ""))
        
        if not manager_id or not manager_name:
            continue
        
        manager_summary = manager_summary_by_id.get(manager_id)
        
        print(f"\nАнализирую менеджера: {manager_name}")
        
        problems_obj = analyze_manager_problems(
            manager_name=manager_name,
            manager_id=manager_id,
            spin_stats=spin_stat,
            manager_summary=manager_summary,
        )
        
        # Формируем строки для таблиц
        for i, problem in enumerate(problems_obj.problems, 1):
            all_problems.append({
                "manager_name": manager_name,
                "manager_id": manager_id,
                "problem_rank": i,
                "priority": problem["priority"],
                "problem_name": problem["name"],
                "current_value": problem["current_value"],
                "target_value": problem["target_value"],
                "impact": problem["impact"],
                "description": problem["description"],
                "severity_score": problem.get("severity_score", 0),
            })
        
        for metric in problems_obj.weekly_metrics:
            all_weekly_metrics.append({
                "manager_name": manager_name,
                "manager_id": manager_id,
                "metric_name": metric["metric_name"],
                "current": metric.get("current"),
                "target_week1": metric.get("target_week1"),
                "target_week2": metric.get("target_week2"),
                "target_week4": metric.get("target_week4"),
                "formula": metric.get("formula", ""),
                "check_frequency": metric.get("check_frequency", "Еженедельно"),
                "measurement": metric.get("measurement", ""),
            })
        
        for step in problems_obj.improvement_steps:
            all_improvement_steps.append({
                "manager_name": manager_name,
                "manager_id": manager_id,
                "step_number": step["step_number"],
                "description": step["description"],
                "actions": " | ".join(step.get("actions", [])),
                "expected_result": step.get("expected_result", ""),
                "timeframe": step.get("timeframe", ""),
                "success_criteria": step.get("success_criteria", ""),
            })
        
        # Выводим результаты
        print(f"\n{'='*80}")
        print(f"МЕНЕДЖЕР: {manager_name}")
        print(f"{'='*80}")
        print(f"\nТОП-{len(problems_obj.problems)} ПРОБЛЕМ:")
        for i, p in enumerate(problems_obj.problems, 1):
            print(f"\n{i}. {p['name']} [{p['impact']}]")
            print(f"   Текущее: {p['current_value']}")
            print(f"   Цель: {p['target_value']}")
            print(f"   {p['description']}")
        
        print(f"\nЕЖЕНЕДЕЛЬНЫЕ МЕТРИКИ ({len(problems_obj.weekly_metrics)}):")
        for m in problems_obj.weekly_metrics:
            print(f"  - {m['metric_name']}: {m.get('current')} → Неделя 1: {m.get('target_week1')}, Неделя 4: {m.get('target_week4')}")
        
        print(f"\nШАГИ УЛУЧШЕНИЯ ({len(problems_obj.improvement_steps)}):")
        for s in problems_obj.improvement_steps:
            print(f"  {s['step_number']}. {s['description']} ({s.get('timeframe', 'N/A')})")
    
    # Записываем в Google Sheets
    print("\nЗаписываю результаты в Google Sheets...")
    
    upsert_worksheet(
        ss,
        "manager_top_problems",
        rows=dicts_to_table(
            all_problems,
            header=[
                "manager_name", "manager_id", "problem_rank", "priority",
                "problem_name", "current_value", "target_value", "impact",
                "description", "severity_score",
            ],
        ),
    )
    
    upsert_worksheet(
        ss,
        "weekly_metrics_tracking",
        rows=dicts_to_table(
            all_weekly_metrics,
            header=[
                "manager_name", "manager_id", "metric_name", "current",
                "target_week1", "target_week2", "target_week4",
                "formula", "check_frequency", "measurement",
            ],
        ),
    )
    
    upsert_worksheet(
        ss,
        "improvement_steps_detailed",
        rows=dicts_to_table(
            all_improvement_steps,
            header=[
                "manager_name", "manager_id", "step_number", "description",
                "actions", "expected_result", "timeframe", "success_criteria",
            ],
        ),
    )
    
    print("\n✅ Анализ завершён! Результаты записаны в Google Sheets:")
    print("   - manager_top_problems (топ-5 проблем каждого менеджера)")
    print("   - weekly_metrics_tracking (еженедельные метрики для отслеживания)")
    print("   - improvement_steps_detailed (детальные шаги улучшения)")


if __name__ == "__main__":
    main()

