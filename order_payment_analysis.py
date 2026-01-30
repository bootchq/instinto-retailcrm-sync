from __future__ import annotations

"""
Анализ связи чатов с заказами и статусом оплаты.

Определяет:
1. Привел ли чат к заказу (есть ли order_id)
2. Оплачен ли заказ (статус оплаты из RetailCRM)
3. Успешность чата (заказ + оплата)
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*args: Any, **kwargs: Any) -> bool:
        return False

from retailcrm_client import RetailCrmClient, RetailCrmError
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


def get_order_info(client: RetailCrmClient, order_id: str) -> Optional[Dict[str, Any]]:
    """Получает информацию о заказе через RetailCRM API."""
    try:
        # RetailCRM API для заказов: /api/v5/orders/{id}
        data = client._request("GET", f"/api/v5/orders/{order_id}")
        # Формат ответа: {"success": true, "order": {...}}
        order = data.get("order") or data.get("data")
        return order
    except RetailCrmError as e:
        # Если заказ не найден или нет доступа
        print(f"⚠️ Не удалось получить заказ {order_id}: {e}")
        return None
    except Exception as e:
        print(f"⚠️ Ошибка при запросе заказа {order_id}: {e}")
        return None


def determine_payment_status(order: Dict[str, Any]) -> str:
    """Определяет статус оплаты заказа."""
    if not order:
        return "unknown"
    
    # В RetailCRM статус оплаты может быть в разных полях
    # Проверяем несколько вариантов
    
    # Вариант 1: поле paymentStatus
    payment_status = order.get("paymentStatus") or order.get("payment_status")
    if payment_status:
        status_lower = str(payment_status).lower()
        if any(word in status_lower for word in ["оплачен", "paid", "paid_full", "оплата"]):
            return "paid"
        if any(word in status_lower for word in ["не оплачен", "unpaid", "not_paid", "неоплачен"]):
            return "unpaid"
        if any(word in status_lower for word in ["частично", "partial", "частичная"]):
            return "partial"
    
    # Вариант 2: поле paymentStatusId (ID статуса)
    payment_status_id = order.get("paymentStatusId") or order.get("payment_status_id")
    if payment_status_id:
        # Обычно 1 = оплачен, 0 = не оплачен, но зависит от настроек
        if payment_status_id == 1 or payment_status_id == "1":
            return "paid"
        if payment_status_id == 0 or payment_status_id == "0":
            return "unpaid"
    
    # Вариант 3: поле status (общий статус заказа)
    status = order.get("status") or order.get("orderStatus")
    if status:
        status_lower = str(status).lower()
        if any(word in status_lower for word in ["оплачен", "paid", "выполнен", "completed"]):
            return "paid"
        if any(word in status_lower for word in ["новый", "new", "неоплачен", "unpaid"]):
            return "unpaid"
    
    # Вариант 4: поле paidAt (дата оплаты)
    paid_at = order.get("paidAt") or order.get("paid_at") or order.get("paymentDate")
    if paid_at:
        return "paid"
    
    # Вариант 5: сравнение суммы оплаты с суммой заказа
    total_sum = order.get("totalSumm") or order.get("total_summ") or order.get("totalSum") or 0
    paid_sum = order.get("paidSumm") or order.get("paid_summ") or order.get("paidSum") or 0
    
    try:
        total = float(str(total_sum))
        paid = float(str(paid_sum))
        if paid >= total and total > 0:
            return "paid"
        elif paid > 0:
            return "partial"
        else:
            return "unpaid"
    except (ValueError, TypeError):
        pass
    
    return "unknown"


def main() -> None:
    base = Path(__file__).resolve().parent
    env = _load_env(base / "env")
    
    # Загружаем переменные окружения
    env_file = os.environ.get("RETAILCRM_ENV_FILE", "env")
    load_dotenv(env_file, override=False)
    
    # Устанавливаем переменные окружения из файла
    for k, v in env.items():
        os.environ.setdefault(k, v)
    
    ss = open_spreadsheet(
        spreadsheet_id=env["GOOGLE_SHEETS_ID"],
        service_account_json_path=env["GOOGLE_SERVICE_ACCOUNT_JSON"],
    )
    
    print("Читаю данные из Google Sheets...")
    
    chats = _read_table(ss.worksheet("chats_raw"))
    
    print(f"Загружено чатов: {len(chats)}")
    
    # Подключаемся к RetailCRM
    print("\nПодключаюсь к RetailCRM API...")
    client = RetailCrmClient.from_env()
    
    # Анализируем чаты
    results = []
    unique_order_ids = set()
    
    for chat in chats:
        chat_id = str(chat.get("chat_id", ""))
        order_id = str(chat.get("order_id", ""))
        manager_name = str(chat.get("manager_name", ""))
        
        if not chat_id:
            continue
        
        result = {
            "chat_id": chat_id,
            "manager_name": manager_name,
            "order_id": order_id,
            "has_order": "Да" if (order_id and order_id.strip()) else "Нет",
            "payment_status": "N/A",
            "payment_status_ru": "N/A",
            "is_successful": "Нет",
            "order_total": "",
            "order_paid": "",
        }
        
        # Если есть order_id, получаем информацию о заказе
        if order_id and order_id.strip():
            unique_order_ids.add(order_id)
            order_info = get_order_info(client, order_id)
            
            if order_info:
                payment_status = determine_payment_status(order_info)
                result["payment_status"] = payment_status
                
                # Русские названия
                status_ru = {
                    "paid": "Оплачен",
                    "unpaid": "Не оплачен",
                    "partial": "Частично оплачен",
                    "unknown": "Неизвестно",
                }
                result["payment_status_ru"] = status_ru.get(payment_status, "Неизвестно")
                
                # Успешность: заказ + оплата
                result["is_successful"] = "Да" if payment_status == "paid" else "Нет"
                
                # Суммы заказа
                total_sum = order_info.get("totalSumm") or order_info.get("total_summ") or order_info.get("totalSum") or 0
                paid_sum = order_info.get("paidSumm") or order_info.get("paid_summ") or order_info.get("paidSum") or 0
                result["order_total"] = str(total_sum)
                result["order_paid"] = str(paid_sum)
            else:
                result["payment_status"] = "not_found"
                result["payment_status_ru"] = "Заказ не найден"
        
        results.append(result)
    
    print(f"\nНайдено уникальных order_id: {len(unique_order_ids)}")
    
    # Статистика
    total_chats = len(results)
    chats_with_order = sum(1 for r in results if r["has_order"] == "Да")
    chats_paid = sum(1 for r in results if r["payment_status"] == "paid")
    chats_unpaid = sum(1 for r in results if r["payment_status"] == "unpaid")
    chats_successful = sum(1 for r in results if r["is_successful"] == "Да")
    
    print(f"\nСтатистика:")
    print(f"  Всего чатов: {total_chats}")
    print(f"  Чатов с заказами: {chats_with_order} ({chats_with_order/total_chats*100:.1f}%)")
    print(f"  Заказов оплачено: {chats_paid}")
    print(f"  Заказов не оплачено: {chats_unpaid}")
    print(f"  Успешных чатов (заказ + оплата): {chats_successful} ({chats_successful/total_chats*100:.1f}%)")
    
    # Записываем результаты
    print("\nЗаписываю результаты в Google Sheets...")
    
    upsert_worksheet(
        ss,
        "chat_order_payment",
        rows=dicts_to_table(
            results,
            header=[
                "chat_id", "manager_name", "order_id", "has_order",
                "payment_status", "payment_status_ru", "is_successful",
                "order_total", "order_paid",
            ],
        ),
    )
    
    # Агрегируем по менеджерам
    manager_stats = {}
    for r in results:
        manager_name = r["manager_name"]
        if not manager_name:
            continue
        
        if manager_name not in manager_stats:
            manager_stats[manager_name] = {
                "manager_name": manager_name,
                "total_chats": 0,
                "chats_with_order": 0,
                "chats_paid": 0,
                "chats_unpaid": 0,
                "chats_successful": 0,
            }
        
        stats = manager_stats[manager_name]
        stats["total_chats"] += 1
        
        if r["has_order"] == "Да":
            stats["chats_with_order"] += 1
            if r["payment_status"] == "paid":
                stats["chats_paid"] += 1
            elif r["payment_status"] == "unpaid":
                stats["chats_unpaid"] += 1
        
        if r["is_successful"] == "Да":
            stats["chats_successful"] += 1
    
    # Вычисляем проценты
    manager_rows = []
    for stats in manager_stats.values():
        total = stats["total_chats"]
        manager_rows.append({
            "manager_name": stats["manager_name"],
            "total_chats": total,
            "chats_with_order": stats["chats_with_order"],
            "order_rate": f"{stats['chats_with_order']/total*100:.1f}%" if total > 0 else "0%",
            "chats_paid": stats["chats_paid"],
            "chats_unpaid": stats["chats_unpaid"],
            "chats_successful": stats["chats_successful"],
            "success_rate": f"{stats['chats_successful']/total*100:.1f}%" if total > 0 else "0%",
        })
    
    upsert_worksheet(
        ss,
        "manager_order_payment_stats",
        rows=dicts_to_table(
            manager_rows,
            header=[
                "manager_name", "total_chats", "chats_with_order", "order_rate",
                "chats_paid", "chats_unpaid", "chats_successful", "success_rate",
            ],
        ),
    )
    
    print("\n✅ Анализ завершён! Результаты записаны в Google Sheets:")
    print("   - chat_order_payment (связь чатов с заказами и оплатой)")
    print("   - manager_order_payment_stats (статистика по менеджерам)")
    
    # Выводим статистику по менеджерам
    print("\n" + "="*80)
    print("СТАТИСТИКА ПО МЕНЕДЖЕРАМ")
    print("="*80 + "\n")
    
    for row in manager_rows:
        print(f"{row['manager_name']}:")
        print(f"  Всего чатов: {row['total_chats']}")
        print(f"  Чатов с заказами: {row['chats_with_order']} ({row['order_rate']})")
        print(f"  Оплачено: {row['chats_paid']}")
        print(f"  Не оплачено: {row['chats_unpaid']}")
        print(f"  Успешных (заказ + оплата): {row['chats_successful']} ({row['success_rate']})")
        print()


if __name__ == "__main__":
    main()

