from __future__ import annotations

"""
Быстрый тест API RetailCRM для заказов.
"""

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*args, **kwargs):
        return False

from retailcrm_client import RetailCrmClient, RetailCrmError


def _load_env(env_path: Path):
    env = {}
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def main():
    base = Path(__file__).resolve().parent
    env = _load_env(base / "env")
    
    env_file = os.environ.get("RETAILCRM_ENV_FILE", "env")
    load_dotenv(env_file, override=False)
    
    for k, v in env.items():
        os.environ.setdefault(k, v)
    
    print("Подключаюсь к RetailCRM...")
    client = RetailCrmClient.from_env()
    
    # Тест 1: Получить список заказов
    print("\n1. Тест получения списка заказов...")
    try:
        import time
        start = time.time()
        data = client._request("GET", "/api/v5/orders", params={"limit": 20})
        elapsed = time.time() - start
        orders = data.get("orders") or data.get("data") or []
        print(f"✅ Успешно за {elapsed:.2f} сек. Получено {len(orders)} заказов")
        if orders:
            print(f"   Пример заказа ID: {orders[0].get('id')}")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return
    
    # Тест 2: Получить заказы по customerId
    print("\n2. Тест получения заказов по customerId...")
    test_customer_id = "186387"  # Из вашего примера
    try:
        start = time.time()
        data = client._request(
            "GET",
            "/api/v5/orders",
            params={"customerId": test_customer_id, "limit": 20}
        )
        elapsed = time.time() - start
        orders = data.get("orders") or data.get("data") or []
        print(f"✅ Успешно за {elapsed:.2f} сек. Найдено {len(orders)} заказов для клиента {test_customer_id}")
        if orders:
            order = orders[0]
            print(f"   Заказ ID: {order.get('id')}")
            print(f"   Статус: {order.get('status')}")
            print(f"   totalSumm: {order.get('totalSumm')}")
            print(f"   prepaySum: {order.get('prepaySum')}")
            print(f"   payments: {order.get('payments')}")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()




