from __future__ import annotations

"""
Обновляет только данные о заказах и оплате для существующих чатов в chats_raw.

Не перевыгружает все чаты, только обновляет колонки:
- has_order
- payment_status
- payment_status_ru
- is_successful
- order_id (если найден)
"""

import os
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*args: Any, **kwargs: Any) -> bool:
        return False

from dateutil import parser as dtparser
from retailcrm_client import RetailCrmClient, RetailCrmError
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


def _parse_dt(v: Any) -> Optional[Any]:
    if not v:
        return None
    try:
        dt = dtparser.parse(str(v))
        if dt.tzinfo:
            dt = dt.replace(tzinfo=None)
        return dt
    except Exception:
        return None


def get_orders_by_customer(client: RetailCrmClient, customer_id: str, cache: Dict[str, List[Dict[str, Any]]], limit: int = 20, max_pages: int = 3) -> List[Dict[str, Any]]:
    """Получает заказы клиента с кешированием."""
    if customer_id in cache:
        return cache[customer_id]
    
    all_orders = []
    page = 1
    
    while page <= max_pages:
        try:
            data = client._request(
                "GET",
                "/api/v5/orders",
                params={"customerId": customer_id, "limit": limit, "page": page}
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
        except Exception:
            break
    
    cache[customer_id] = all_orders
    return all_orders


def find_related_order(orders: List[Dict[str, Any]], chat_created_at: Any, days_window: int = 30) -> Optional[Dict[str, Any]]:
    """Находит заказ, связанный с чатом."""
    if not orders or not chat_created_at:
        return None
    
    chat_dt = _parse_dt(chat_created_at)
    if not chat_dt:
        return None
    
    window_end = chat_dt + timedelta(days=days_window)
    
    related_orders = []
    for order in orders:
        order_created = _parse_dt(order.get("createdAt"))
        if order_created and chat_dt <= order_created <= window_end:
            related_orders.append((order_created, order))
    
    if not related_orders:
        return None
    
    related_orders.sort(key=lambda x: x[0])
    return related_orders[0][1]


def determine_payment_status(order: Dict[str, Any]) -> str:
    """Определяет статус оплаты."""
    if not order:
        return "unknown"
    
    total_sum = float(order.get("totalSumm") or order.get("total_summ") or 0)
    prepay_sum = float(order.get("prepaySum") or order.get("prepay_sum") or 0)
    purchase_sum = float(order.get("purchaseSumm") or order.get("purchase_summ") or 0)
    paid_sum = max(prepay_sum, purchase_sum)
    
    if total_sum > 0:
        if paid_sum >= total_sum:
            return "paid"
        elif paid_sum > 0:
            return "partial"
        else:
            return "unpaid"
    
    payments = order.get("payments")
    if payments and isinstance(payments, list) and len(payments) > 0:
        for payment in payments:
            status = str(payment.get("status", "")).lower()
            if "paid" in status or "оплачен" in status:
                return "paid"
    
    return "unknown"


def main() -> None:
    base = Path(__file__).resolve().parent
    env = _load_env(base / "env")
    
    env_file = os.environ.get("RETAILCRM_ENV_FILE", "env")
    load_dotenv(env_file, override=False)
    
    for k, v in env.items():
        os.environ.setdefault(k, v)
    
    ss = open_spreadsheet(
        spreadsheet_id=env["GOOGLE_SHEETS_ID"],
        service_account_json_path=env["GOOGLE_SERVICE_ACCOUNT_JSON"],
    )
    
    print("Читаю существующие чаты из Google Sheets...")
    ws = ss.worksheet("chats_raw")
    
    # Проверяем, есть ли ограничение на количество чатов
    TEST_LIMIT = os.environ.get("TEST_LIMIT")
    chats = _read_table(ws)
    
    if TEST_LIMIT:
        original_count = len(chats)
        chats = chats[:int(TEST_LIMIT)]
        print(f"⚠️ ТЕСТОВЫЙ РЕЖИМ: обрабатываю только первые {len(chats)} из {original_count} чатов")
    
    print(f"Загружено чатов для обработки: {len(chats)}")
    
    # Проверяем, есть ли уже колонки с заказами
    header = ws.get_all_values()[0] if ws.get_all_values() else []
    has_order_columns = "has_order" in header
    
    if not has_order_columns:
        print("\n⚠️ Колонки с заказами отсутствуют. Нужна полная перевыгрузка через export_to_sheets.py")
        return
    
    print("Подключаюсь к RetailCRM...")
    client = RetailCrmClient.from_env()
    client.timeout_s = 30
    
    # Собираем уникальные client_id
    unique_client_ids = set()
    for chat in chats:
        client_id = str(chat.get("client_id", ""))
        if client_id and client_id.strip():
            unique_client_ids.add(client_id)
    
    print(f"Уникальных client_id: {len(unique_client_ids)}")
    print("Загружаю заказы из RetailCRM...")
    
    orders_cache: Dict[str, List[Dict[str, Any]]] = {}
    processed_clients = 0
    
    for idx, client_id in enumerate(unique_client_ids, 1):
        try:
            print(f"  [{idx}/{len(unique_client_ids)}] client_id {client_id}...", end="", flush=True)
            import time
            start_time = time.time()
            orders = get_orders_by_customer(client, client_id, orders_cache, limit=20, max_pages=3)
            elapsed = time.time() - start_time
            print(f" ✅ {len(orders)} заказов ({elapsed:.1f}с)")
            processed_clients += 1
        except Exception as e:
            print(f" ❌ {str(e)[:60]}")
    
    print(f"\nЗагружено заказов для {processed_clients} клиентов")
    
    # Функция для получения буквы колонки (A, B, C, ..., Z, AA, AB, ...)
    def col_letter(col_idx: int) -> str:
        result = ""
        col_idx += 1  # gspread использует 1-based индексы
        while col_idx > 0:
            col_idx -= 1
            result = chr(65 + (col_idx % 26)) + result
            col_idx //= 26
        return result
    
    # Обновляем данные
    print("\nОбновляю данные о заказах...")
    
    updates = []
    for i, chat in enumerate(chats):
        chat_id = str(chat.get("chat_id", ""))
        client_id = str(chat.get("client_id", ""))
        chat_created_at = chat.get("created_at")
        
        if not chat_id:
            continue
        
        # Находим индекс колонок
        try:
            has_order_idx = header.index("has_order")
            payment_status_idx = header.index("payment_status")
            payment_status_ru_idx = header.index("payment_status_ru")
            is_successful_idx = header.index("is_successful")
            order_id_idx = header.index("order_id")
        except ValueError:
            print("⚠️ Не найдены нужные колонки в таблице")
            return
        
        # Получаем текущие значения строки
        row_num = i + 2  # +2 потому что строка 1 = заголовок, строки начинаются с 1
        current_row = ws.row_values(row_num)
        
        # Обновляем значения
        has_order = "Нет"
        payment_status = "N/A"
        payment_status_ru = "N/A"
        is_successful = "Нет"
        order_id = str(chat.get("order_id", "") or "")
        
        if client_id and client_id.strip():
            orders = orders_cache.get(client_id, [])
            if orders:
                related_order = find_related_order(orders, chat_created_at, days_window=30)
                if related_order:
                    order_id = str(related_order.get("id") or related_order.get("number") or "")
                    has_order = "Да"
                    payment_status = determine_payment_status(related_order)
                    status_ru_map = {
                        "paid": "Оплачен",
                        "unpaid": "Не оплачен",
                        "partial": "Частично оплачен",
                        "unknown": "Неизвестно",
                    }
                    payment_status_ru = status_ru_map.get(payment_status, "Неизвестно")
                    is_successful = "Да" if payment_status == "paid" else "Нет"
        
        # Функция для получения буквы колонки (A, B, C, ..., Z, AA, AB, ...)
        def col_letter(col_idx: int) -> str:
            result = ""
            col_idx += 1  # gspread использует 1-based индексы
            while col_idx > 0:
                col_idx -= 1
                result = chr(65 + (col_idx % 26)) + result
                col_idx //= 26
            return result
        
        # Обновляем только нужные ячейки (используем формат для batch_update)
        updates.append({
            "range": f"chats_raw!{col_letter(has_order_idx)}{row_num}",
            "values": [[has_order]]
        })
        updates.append({
            "range": f"chats_raw!{col_letter(payment_status_idx)}{row_num}",
            "values": [[payment_status]]
        })
        updates.append({
            "range": f"chats_raw!{col_letter(payment_status_ru_idx)}{row_num}",
            "values": [[payment_status_ru]]
        })
        updates.append({
            "range": f"chats_raw!{col_letter(is_successful_idx)}{row_num}",
            "values": [[is_successful]]
        })
        if order_id:
            updates.append({
                "range": f"chats_raw!{col_letter(order_id_idx)}{row_num}",
                "values": [[order_id]]
            })
        
        if (i + 1) % 50 == 0:
            # Обновляем батчами по 50 чатов
            if updates:
                try:
                    # Используем values_batch_update через spreadsheet
                    body = {
                        "valueInputOption": "RAW",
                        "data": updates
                    }
                    ws.spreadsheet.values_batch_update(body)
                    print(f"  Обновлено {i + 1} чатов...")
                except Exception as e:
                    print(f"  ⚠️ Ошибка при обновлении батча: {str(e)[:100]}")
                updates = []
    
    # Обновляем оставшиеся
    if updates:
        try:
            body = {
                "valueInputOption": "RAW",
                "data": updates
            }
            ws.spreadsheet.values_batch_update(body)
        except Exception as e:
            print(f"  ⚠️ Ошибка при финальном обновлении: {str(e)[:100]}")
    
    print(f"\n✅ Обновлено {len(chats)} чатов!")
    print("Данные о заказах и оплате обновлены в Google Sheets")


if __name__ == "__main__":
    main()


