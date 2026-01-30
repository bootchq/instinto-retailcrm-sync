"""Отладка определения этапов продаж."""

from pathlib import Path
from sheets import open_spreadsheet


def _load_env(env_path: Path):
    env = {}
    for line in env_path.read_text().splitlines():
        if '=' in line and not line.strip().startswith('#'):
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def _read_table(ws):
    values = ws.get_all_values()
    if not values:
        return []
    header = [
        "chat_id", "channel", "manager_id", "manager_name", "client_id", "order_id",
        "has_order", "payment_status", "payment_status_ru", "is_successful",
        "created_at", "updated_at", "status",
        "inbound_count", "outbound_count", "first_response_sec", "unanswered_inbound",
    ]
    out = []
    for row in values:
        d = {}
        for i, h in enumerate(header):
            d[h] = row[i] if i < len(row) else ""
        out.append(d)
    return out


base = Path(__file__).resolve().parent
env = _load_env(base / "env")

ss = open_spreadsheet(
    spreadsheet_id=env['GOOGLE_SHEETS_ID'],
    service_account_json_path=env['GOOGLE_SERVICE_ACCOUNT_JSON'],
)

ws_chats = ss.worksheet('chats_raw')
ws_messages = ss.worksheet('messages_raw')

chats = _read_table(ws_chats)
messages_values = ws_messages.get_all_values()

# Группируем сообщения
messages_by_chat = {}
if len(messages_values) > 1:
    header = messages_values[0] if messages_values[0] else ["chat_id", "message_id", "sent_at", "direction", "manager_id", "message_type", "author_type", "text"]
    for row in messages_values[1:]:
        if len(row) > 0:
            chat_id = row[0] if len(row) > 0 else ""
            if chat_id:
                msg = {}
                for i, h in enumerate(header):
                    msg[h] = row[i] if i < len(row) else ""
                messages_by_chat.setdefault(chat_id, []).append(msg)

# Берём первые 5 чатов с сообщениями
print("Анализ первых 5 чатов с сообщениями:")
print("=" * 60)

count = 0
for chat in chats[:50]:
    chat_id = str(chat.get("chat_id", ""))
    chat_messages = messages_by_chat.get(chat_id, [])
    
    if not chat_messages or len(chat_messages) < 3:
        continue
    
    count += 1
    if count > 5:
        break
    
    chat_messages.sort(key=lambda m: m.get("sent_at", ""))
    manager_messages = [
        m for m in chat_messages 
        if m.get("direction") in ["out", "outbound"] or 
           (m.get("direction") != "in" and m.get("manager_id"))
    ]
    
    print(f"\nЧат {chat_id} (менеджер: {chat.get('manager_name', 'N/A')}):")
    print(f"Всего сообщений: {len(chat_messages)}, от менеджера: {len(manager_messages)}")
    print("\nПервые 5 сообщений менеджера:")
    
    for i, msg in enumerate(manager_messages[:5], 1):
        text = str(msg.get("text", "")).strip()
        direction = msg.get("direction", "")
        print(f"\n  {i}. [{direction}] {text[:100]}")

