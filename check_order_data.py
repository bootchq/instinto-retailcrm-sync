from __future__ import annotations

"""
Проверка доступности данных о заказах в RetailCRM.

Проверяет:
1. Есть ли order_id в чатах
2. Можно ли получить заказы через client_id
3. Какие поля доступны в API заказов
"""

import os
from pathlib import Path
from typing import Any, Dict

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*args: Any, **kwargs: Any) -> bool:
        return False

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


def _read_table(ws) -> list:
    values = ws.get_all_values()
    if not values:
        return []
    header = values[0]
    out = []
    for row in values[1:]:
        d = {}
        for i, h in enumerate(header):
            d[h] = row[i] if i < len(row) else ""
        out.append(d)
    return out


def main() -> None:
    base = Path(__file__).resolve().parent
    env = _load_env(base / "env")
    
    # Загружаем переменные окружения
    env_file = os.environ.get("RETAILCRM_ENV_FILE", "env")
    load_dotenv(env_file, override=False)
    
    for k, v in env.items():
        os.environ.setdefault(k, v)
    
    print("Проверяю данные в Google Sheets...")
    
    ss = open_spreadsheet(
        spreadsheet_id=env["GOOGLE_SHEETS_ID"],
        service_account_json_path=env["GOOGLE_SERVICE_ACCOUNT_JSON"],
    )
    
    chats = _read_table(ss.worksheet("chats_raw"))
    
    print(f"\nЗагружено чатов: {len(chats)}")
    
    # Проверяем наличие order_id и client_id
    chats_with_order_id = sum(1 for c in chats if c.get("order_id") and str(c.get("order_id")).strip())
    chats_with_client_id = sum(1 for c in chats if c.get("client_id") and str(c.get("client_id")).strip())
    
    print(f"Чатов с order_id: {chats_with_order_id}")
    print(f"Чатов с client_id: {chats_with_client_id}")
    
    # Показываем примеры
    print("\nПримеры данных из chats_raw:")
    for i, chat in enumerate(chats[:5]):
        print(f"\nЧат {i+1}:")
        print(f"  chat_id: {chat.get('chat_id')}")
        print(f"  order_id: {chat.get('order_id')}")
        print(f"  client_id: {chat.get('client_id')}")
        print(f"  manager_name: {chat.get('manager_name')}")
    
    # Проверяем API RetailCRM
    print("\n" + "="*80)
    print("Проверка RetailCRM API")
    print("="*80)
    
    client = RetailCrmClient.from_env()
    
    # Пробуем получить список заказов (первые 20)
    print("\n1. Пробую получить список заказов через /api/v5/orders...")
    try:
        data = client._request("GET", "/api/v5/orders", params={"limit": 20})
        orders = data.get("orders") or data.get("data") or []
        if orders:
            print(f"✅ Успешно! Получено {len(orders)} заказов")
            print("\nПример заказа (первые поля):")
            order = orders[0]
            for key in list(order.keys())[:15]:
                print(f"  {key}: {order.get(key)}")
            
            # Проверяем поля оплаты
            print("\nПоля, связанные с оплатой:")
            payment_fields = [k for k in order.keys() if any(word in k.lower() for word in ["pay", "оплат", "status", "статус"])]
            for field in payment_fields:
                print(f"  {field}: {order.get(field)}")
        else:
            print("⚠️ Заказы не найдены или формат ответа другой")
            print(f"Ответ API: {list(data.keys())}")
    except RetailCrmError as e:
        print(f"❌ Ошибка: {e}")
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
    
    # Пробуем получить заказы по client_id (если есть)
    if chats_with_client_id > 0:
        print("\n2. Пробую получить заказы по client_id...")
        sample_chat = next((c for c in chats if c.get("client_id") and str(c.get("client_id")).strip()), None)
        if sample_chat:
            client_id = str(sample_chat.get("client_id"))
            print(f"   Использую client_id: {client_id}")
            try:
                data = client._request("GET", "/api/v5/orders", params={"customerId": client_id, "limit": 20})
                orders = data.get("orders") or data.get("data") or []
                if orders:
                    print(f"✅ Найдено {len(orders)} заказов для этого клиента")
                else:
                    print("⚠️ Заказы не найдены для этого client_id")
            except Exception as e:
                print(f"⚠️ Ошибка при запросе заказов по client_id: {e}")
    
    print("\n" + "="*80)
    print("РЕКОМЕНДАЦИИ")
    print("="*80)
    
    if chats_with_order_id == 0:
        print("\n⚠️ order_id отсутствует в данных чатов.")
        print("Возможные решения:")
        print("1. Проверить, заполняется ли order_id в RetailCRM при создании заказа из чата")
        print("2. Получать заказы через client_id (если он есть)")
        print("3. Использовать связь через дату создания чата и заказа")
    else:
        print(f"\n✅ Найдено {chats_with_order_id} чатов с order_id")
        print("Можно использовать order_payment_analysis.py для анализа")


if __name__ == "__main__":
    main()

