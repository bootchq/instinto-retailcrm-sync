"""Проверка данных в chats_raw для понимания структуры."""

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
header = values[0]

print('Колонки:', header[:15])
print()

# Проверяем первые 5 чатов
print('Примеры данных (первые 5 чатов):')
for i, row in enumerate(values[1:6], 1):
    print(f'\nЧат {i}:')
    for j, col in enumerate(header[:12]):
        val = row[j] if j < len(row) else ''
        if val:
            print(f'  {col}: {val[:60]}')

# Статистика
print('\n\nСтатистика:')
is_successful_col = header.index('is_successful') if 'is_successful' in header else None
payment_status_col = header.index('payment_status') if 'payment_status' in header else None
order_id_col = header.index('order_id') if 'order_id' in header else None

if is_successful_col is not None:
    successful_count = sum(1 for row in values[1:] if is_successful_col < len(row) and row[is_successful_col] == 'Да')
    print(f'is_successful = Да: {successful_count} из {len(values)-1}')
else:
    print('Колонка is_successful не найдена')

if payment_status_col is not None:
    payment_statuses = {}
    for row in values[1:]:
        if payment_status_col < len(row):
            status = row[payment_status_col] or 'пусто'
            payment_statuses[status] = payment_statuses.get(status, 0) + 1
    print(f'payment_status: {payment_statuses}')
else:
    print('Колонка payment_status не найдена')

if order_id_col is not None:
    orders_count = sum(1 for row in values[1:] if order_id_col < len(row) and row[order_id_col])
    print(f'Чатов с order_id: {orders_count} из {len(values)-1}')
else:
    print('Колонка order_id не найдена')

