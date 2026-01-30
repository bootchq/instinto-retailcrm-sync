"""Детальная проверка реальных данных."""

from pathlib import Path
from sheets import open_spreadsheet


def _load_env(env_path: Path):
    env = {}
    for line in env_path.read_text().splitlines():
        if '=' in line and not line.strip().startswith('#'):
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


base = Path(__file__).resolve().parent
env = _load_env(base / "env")

ss = open_spreadsheet(
    spreadsheet_id=env['GOOGLE_SHEETS_ID'],
    service_account_json_path=env['GOOGLE_SERVICE_ACCOUNT_JSON'],
)

ws = ss.worksheet('chats_raw')
values = ws.get_all_values()

# Стандартные заголовки
header = [
    "chat_id", "channel", "manager_id", "manager_name", "client_id", "order_id",
    "has_order", "payment_status", "payment_status_ru", "is_successful",
    "created_at", "updated_at", "status",
    "inbound_count", "outbound_count", "first_response_sec", "unanswered_inbound",
]

print(f"Всего строк: {len(values)}")
print(f"Используем заголовки: {header[:10]}...")
print()

# Анализируем первые 20 строк
print("Первые 10 строк данных:")
for i, row in enumerate(values[:10], 1):
    print(f"\nСтрока {i}:")
    for j, col_name in enumerate(header[:12]):
        val = row[j] if j < len(row) else ""
        if val or col_name in ["has_order", "payment_status", "payment_status_ru", "is_successful", "order_id"]:
            print(f"  {col_name}: {val}")

# Статистика по ключевым полям
print("\n\nСТАТИСТИКА:")
print("=" * 60)

# Индексы колонок
chat_id_idx = 0
has_order_idx = 6
payment_status_idx = 7
payment_status_ru_idx = 8
is_successful_idx = 9
order_id_idx = 5
manager_name_idx = 3

# Подсчитываем
total = len(values)
has_order_count = 0
payment_statuses = {}
is_successful_count = 0
order_ids_count = 0
managers = {}

for row in values:
    if len(row) > has_order_idx:
        if row[has_order_idx] and str(row[has_order_idx]).strip().lower() in ["да", "yes", "true", "1"]:
            has_order_count += 1
    
    if len(row) > payment_status_ru_idx:
        status = str(row[payment_status_ru_idx]).strip()
        if status and status != "N/A":
            payment_statuses[status] = payment_statuses.get(status, 0) + 1
    
    if len(row) > is_successful_idx:
        if str(row[is_successful_idx]).strip().lower() in ["да", "yes", "true", "1"]:
            is_successful_count += 1
    
    if len(row) > order_id_idx:
        if row[order_id_idx] and str(row[order_id_idx]).strip() and str(row[order_id_idx]).strip() != "N/A":
            order_ids_count += 1
    
    if len(row) > manager_name_idx:
        mgr = str(row[manager_name_idx]).strip()
        if mgr:
            managers[mgr] = managers.get(mgr, 0) + 1

print(f"Всего чатов: {total}")
print(f"Чатов с has_order=Да: {has_order_count}")
print(f"Чатов с is_successful=Да: {is_successful_count}")
print(f"Чатов с order_id: {order_ids_count}")
print(f"\nСтатусы оплаты (payment_status_ru):")
for status, count in sorted(payment_statuses.items(), key=lambda x: x[1], reverse=True)[:10]:
    print(f"  {status}: {count}")
print(f"\nМенеджеры (топ-5):")
for mgr, count in sorted(managers.items(), key=lambda x: x[1], reverse=True)[:5]:
    print(f"  {mgr}: {count}")

