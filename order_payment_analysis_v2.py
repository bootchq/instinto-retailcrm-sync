from __future__ import annotations

"""
Анализ связи чатов с заказами и статусом оплаты (версия 2).

Определяет:
1. Привел ли чат к заказу (через client_id)
2. Оплачен ли заказ (статус оплаты из RetailCRM)
3. Успешность чата (заказ + оплата)
"""

import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*args: Any, **kwargs: Any) -> bool:
        return False

from dateutil import parser as dtparser
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


def _parse_dt(v: Any) -> Optional[datetime]:
    if not v:
        return None
    try:
        return dtparser.parse(str(v))
    except Exception:
        return None


def get_orders_by_customer(client: RetailCrmClient, customer_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    """Получает заказы клиента через RetailCRM API."""
    all_orders = []
    page = 1
    
    while True:
        try:
            data = client._request(
                "GET",
                "/api/v5/orders",
                params={
                    "customerId": customer_id,
                    "limit": limit,
                    "page": page,
                }
            )
            orders = data.get("orders") or data.get("data") or []
            if not orders:
                break
            all_orders.extend(orders)
            
            pagination = data.get("pagination") or {}
            total_pages = pagination.get("totalPageCount") or pagination.get("total_pages")
            if total_pages and page >= int(total_pages):
                break
            if not total_pages:
                break
            page += 1
        except RetailCrmError:
            break
        except Exception:
            break
    
    return all_orders


def find_related_order(orders: List[Dict[str, Any]], chat_created_at: Optional[datetime], days_window: int = 30) -> Optional[Dict[str, Any]]:
    """Находит заказ, связанный с чатом (по дате создания)."""
    if not orders or not chat_created_at:
        return None
    
    # Ищем заказ, созданный в течение days_window дней после создания чата
    window_end = chat_created_at + timedelta(days=days_window)
    
    # Сортируем заказы по дате создания (ближайший к дате чата)
    related_orders = []
    for order in orders:
        order_created = _parse_dt(order.get("createdAt") or order.get("created_at"))
        if order_created and chat_created_at <= order_created <= window_end:
            related_orders.append((order_created, order))
    
    if not related_orders:
        return None
    
    # Возвращаем ближайший по дате
    related_orders.sort(key=lambda x: x[0])
    return related_orders[0][1]


def determine_payment_status(order: Dict[str, Any]) -> str:
    """Определяет статус оплаты заказа."""
    if not order:
        return "unknown"
    
    # Вариант 1: поле payments (массив платежей)
    payments = order.get("payments")
    if payments:
        if isinstance(payments, list) and len(payments) > 0:
            # Если есть платежи, проверяем их статус
            paid_total = 0
            for payment in payments:
                payment_status = str(payment.get("status", "")).lower()
                payment_amount = float(payment.get("amount", 0) or 0)
                if "paid" in payment_status or "оплачен" in payment_status or "success" in payment_status:
                    paid_total += payment_amount
        elif isinstance(payments, dict) and payments:
            # Если payments - объект с данными
            paid_total = float(payments.get("paid", 0) or 0)
    
    # Вариант 2: сравнение prepaySum/totalSumm
    total_sum = float(order.get("totalSumm") or order.get("total_summ") or order.get("totalSum") or 0)
    prepay_sum = float(order.get("prepaySum") or order.get("prepay_sum") or order.get("prepay") or 0)
    purchase_sum = float(order.get("purchaseSumm") or order.get("purchase_summ") or 0)
    
    paid_sum = max(prepay_sum, purchase_sum, paid_total if 'paid_total' in locals() else 0)
    
    if total_sum > 0:
        if paid_sum >= total_sum:
            return "paid"
        elif paid_sum > 0:
            return "partial"
        else:
            return "unpaid"
    
    # Вариант 3: поле status заказа
    status = str(order.get("status") or "").lower()
    if any(word in status for word in ["paid", "оплачен", "completed", "выполнен"]):
        return "paid"
    if any(word in status for word in ["new", "новый", "unpaid", "неоплачен"]):
        return "unpaid"
    
    # Вариант 4: поле paymentStatus
    payment_status = str(order.get("paymentStatus") or order.get("payment_status") or "").lower()
    if "paid" in payment_status or "оплачен" in payment_status:
        return "paid"
    if "unpaid" in payment_status or "неоплачен" in payment_status:
        return "unpaid"
    
    return "unknown"


def main() -> None:
    base = Path(__file__).resolve().parent
    env = _load_env(base / "env")
    
    # Загружаем переменные окружения
    env_file = os.environ.get("RETAILCRM_ENV_FILE", "env")
    load_dotenv(env_file, override=False)
    
    for k, v in env.items():
        os.environ.setdefault(k, v)
    
    ss = open_spreadsheet(
        spreadsheet_id=env["GOOGLE_SHEETS_ID"],
        service_account_json_path=env["GOOGLE_SERVICE_ACCOUNT_JSON"],
    )
    
    print("Читаю данные из Google Sheets...")
    
    chats = _read_table(ss.worksheet("chats_raw"))
    
    # Для теста ограничиваем количество (можно убрать для полного анализа)
    TEST_LIMIT = os.environ.get("TEST_LIMIT")
    if TEST_LIMIT:
        chats = chats[:int(TEST_LIMIT)]
        print(f"⚠️ ТЕСТОВЫЙ РЕЖИМ: обрабатываю только первые {len(chats)} чатов")
    
    print(f"Загружено чатов: {len(chats)}")
    
    # Подключаемся к RetailCRM
    print("\nПодключаюсь к RetailCRM API...")
    client = RetailCrmClient.from_env()
    
    # Анализируем чаты
    results = []
    processed = 0
    unique_client_ids = set()
    
    for chat in chats:
        chat_id = str(chat.get("chat_id", ""))
        client_id = str(chat.get("client_id", ""))
        manager_name = str(chat.get("manager_name", ""))
        chat_created_at = _parse_dt(chat.get("created_at"))
        
        if not chat_id:
            continue
        
        result = {
            "chat_id": chat_id,
            "manager_name": manager_name,
            "client_id": client_id,
            "order_id": "",
            "has_order": "Нет",
            "payment_status": "N/A",
            "payment_status_ru": "N/A",
            "is_successful": "Нет",
            "order_total": "",
            "order_paid": "",
            "order_created_at": "",
        }
        
        # Если есть client_id, получаем заказы клиента
        if client_id and client_id.strip():
            unique_client_ids.add(client_id)
            
            try:
                orders = get_orders_by_customer(client, client_id, limit=50)
                
                if orders:
                    # Находим заказ, связанный с чатом
                    related_order = find_related_order(orders, chat_created_at, days_window=30)
                    
                    if related_order:
                        order_id = str(related_order.get("id") or related_order.get("number") or "")
                        result["order_id"] = order_id
                        result["has_order"] = "Да"
                        result["order_created_at"] = str(related_order.get("createdAt") or "")
                        
                        payment_status = determine_payment_status(related_order)
                        result["payment_status"] = payment_status
                        
                        status_ru = {
                            "paid": "Оплачен",
                            "unpaid": "Не оплачен",
                            "partial": "Частично оплачен",
                            "unknown": "Неизвестно",
                        }
                        result["payment_status_ru"] = status_ru.get(payment_status, "Неизвестно")
                        
                        result["is_successful"] = "Да" if payment_status == "paid" else "Нет"
                        
                        total_sum = related_order.get("totalSumm") or related_order.get("total_summ") or 0
                        prepay_sum = related_order.get("prepaySum") or related_order.get("prepay_sum") or 0
                        purchase_sum = related_order.get("purchaseSumm") or related_order.get("purchase_summ") or 0
                        paid_sum = max(float(prepay_sum or 0), float(purchase_sum or 0))
                        
                        result["order_total"] = str(total_sum)
                        result["order_paid"] = str(paid_sum)
            except Exception as e:
                print(f"⚠️ Ошибка при обработке чата {chat_id}: {e}")
        
        results.append(result)
        processed += 1
        
        if processed % 100 == 0:
            print(f"Обработано: {processed}/{len(chats)}")
    
    print(f"\nОбработано чатов: {processed}")
    print(f"Уникальных client_id: {len(unique_client_ids)}")
    
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
                "chat_id", "manager_name", "client_id", "order_id", "has_order",
                "payment_status", "payment_status_ru", "is_successful",
                "order_total", "order_paid", "order_created_at",
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

