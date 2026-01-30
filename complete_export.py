"""
Скрипт для дозагрузки недостающих чатов.
Проверяет, какие чаты уже есть в таблице, и дозагружает только недостающие.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*args: Any, **kwargs: Any) -> bool:
        return False

from sheets import open_spreadsheet, get_existing_chat_ids, append_to_worksheet, dicts_to_table
from export_to_sheets import (
    _env, _normalize_chat, _normalize_message,
    _iter_web_chats, _fetch_web_messages_for_chat,
    compute_chat_metrics, get_orders_by_customer_cached, find_related_order, determine_payment_status
)
from retailcrm_client import RetailCrmClient
from web_graphql import WebGraphQLClient


def main() -> None:
    env_file = os.environ.get("RETAILCRM_ENV_FILE", "env")
    load_dotenv(env_file, override=False)

    # Период
    last_days = os.environ.get("LAST_DAYS")
    if last_days:
        from datetime import datetime, timedelta
        try:
            days = int(last_days)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            start = start_date.strftime("%Y-%m-%d")
            end = end_date.strftime("%Y-%m-%d")
            print(f"📅 Период: последние {days} дней ({start} - {end})")
        except (ValueError, TypeError):
            start = _env("START_DATE")
            end = _env("END_DATE")
    else:
        start = _env("START_DATE")
        end = _env("END_DATE")

    tz_name = os.environ.get("TZ", "Europe/Moscow")
    work_hours = os.environ.get("WORK_HOURS", "10:00-23:00")
    channels_env = os.environ.get("CHANNELS", "whatsapp,instagram")
    channel_allow = {c.strip().lower() for c in channels_env.split(",") if c.strip()}
    web_curl_file = os.environ.get("WEB_CURL_FILE", "web_curl.txt")
    web_messages_curl_file = os.environ.get("WEB_MESSAGES_CURL_FILE", "web_messages_curl.txt")
    max_messages_per_chat = int(os.environ.get("MAX_MESSAGES_PER_CHAT", "500"))
    batch_size = int(os.environ.get("BATCH_SIZE", "500"))
    web_timeout_s = int(os.environ.get("WEB_TIMEOUT_S", "180"))
    web_retries = int(os.environ.get("WEB_RETRIES", "5"))

    ss = open_spreadsheet(
        spreadsheet_id=_env("GOOGLE_SHEETS_ID"),
        service_account_json_path=_env("GOOGLE_SERVICE_ACCOUNT_JSON"),
    )

    # Проверяем уже обработанные чаты
    print("📖 Проверяю уже обработанные чаты...")
    existing_chat_ids = get_existing_chat_ids(ss, "chats_raw")
    print(f"   Найдено уже обработанных чатов: {len(existing_chat_ids)}")

    # Увеличиваем размер листов, если нужно
    print("\n🔧 Проверяю размер листов...")
    try:
        chats_ws = ss.worksheet("chats_raw")
        chats_ws.resize(rows=10000, cols=50)
        print("   ✅ Лист 'chats_raw' увеличен до 10000 строк")
    except Exception as e:
        print(f"   ⚠️ Ошибка при изменении размера chats_raw: {e}")

    try:
        messages_ws = ss.worksheet("messages_raw")
        messages_ws.resize(rows=100000, cols=50)
        print("   ✅ Лист 'messages_raw' увеличен до 100000 строк")
    except Exception as e:
        print(f"   ⚠️ Ошибка при изменении размера messages_raw: {e}")

    # Подключаемся к RetailCRM
    client = RetailCrmClient.from_env()
    users = client.get_users()
    
    orders_cache: Dict[str, List[Dict[str, Any]]] = {}
    enable_order_check = os.environ.get("ENABLE_ORDER_CHECK", "1") == "1"

    # Получаем все чаты
    print("\n📥 Загружаю чаты из RetailCRM...")
    web_chats: List[Dict[str, Any]] = []
    try:
        if os.path.exists(os.path.join(os.path.dirname(__file__), web_curl_file)):
            web_curl_file = os.path.join(os.path.dirname(__file__), web_curl_file)
        if os.path.exists(web_curl_file):
            wg = WebGraphQLClient(curl_file=web_curl_file)
            start_iso = f"{start}T00:00:00Z"
            end_iso = f"{end}T23:59:59Z"
            wanted_types: List[str] = []
            if "instagram" in channel_allow:
                wanted_types.append("INSTAGRAM")
            if "whatsapp" in channel_allow:
                wanted_types.append("WHATSAPP")
            if not wanted_types:
                wanted_types = ["INSTAGRAM", "WHATSAPP"]

            web_chats = _iter_web_chats(wg, start_iso=start_iso, end_iso=end_iso, channel_types=wanted_types)
    except Exception as e:
        print(f"WEB chats disabled / failed: {e}")
        return

    if not web_chats:
        print("❌ Не удалось получить чаты")
        return

    # Фильтруем уже обработанные
    new_chats = [c for c in web_chats if str(c.get("id", "")) not in existing_chat_ids]
    print(f"   Всего чатов: {len(web_chats)}")
    print(f"   Новых чатов для обработки: {len(new_chats)}")

    if not new_chats:
        print("\n✅ Все чаты уже обработаны!")
        return

    # Маппинг пользователей
    web_users_by_id: Dict[int, Dict[str, Any]] = {}
    for rc in web_chats:
        resp = ((rc.get("lastDialog") or {}).get("responsible") or {})
        if isinstance(resp, dict):
            rid = resp.get("id")
            rname = resp.get("name")
            if rid is not None and rname:
                try:
                    web_users_by_id[int(rid)] = {"id": int(rid), "name": str(rname)}
                except Exception:
                    pass

    users_combined = dict(users)
    users_combined.update(web_users_by_id)

    # Подключаемся к web messages
    wg_messages = None
    try:
        if os.path.exists(os.path.join(os.path.dirname(__file__), web_messages_curl_file)):
            web_messages_curl_file = os.path.join(os.path.dirname(__file__), web_messages_curl_file)
        if os.path.exists(web_messages_curl_file):
            wg_messages = WebGraphQLClient(curl_file=web_messages_curl_file, timeout_s=web_timeout_s, max_retries=web_retries)
    except Exception as e:
        print(f"WEB messages disabled / failed: {e}")

    if wg_messages is None:
        print("WEB messages: OFF -> using lastMessage fallback")
    else:
        print(f"WEB messages: ON")

    # Обрабатываем новые чаты
    chats_rows: List[Dict[str, Any]] = []
    messages_rows: List[Dict[str, Any]] = []
    processed = 0
    skipped = 0
    batch_num = 0

    print(f"\n🔄 Начинаю обработку {len(new_chats)} новых чатов...\n")

    for raw_chat in new_chats:
        chat = _normalize_chat({
            "id": raw_chat.get("id"),
            "channel": (raw_chat.get("channel") or {}).get("type") or (raw_chat.get("channel") or {}).get("name"),
            "clientId": (raw_chat.get("customer") or {}).get("id"),
            "managerId": ((raw_chat.get("lastDialog") or {}).get("responsible") or {}).get("id"),
            "createdAt": (raw_chat.get("lastDialog") or {}).get("createdAt") or raw_chat.get("lastActivity"),
            "updatedAt": raw_chat.get("lastActivity"),
            "status": (raw_chat.get("lastDialog") or {}).get("closedAt") and "CLOSED" or "ACTIVE",
            "raw": raw_chat,
        })

        chat_id = chat["id"]
        if not str(chat_id).strip():
            skipped += 1
            continue

        try:
            # Получаем сообщения
            messages: List[Dict[str, Any]] = []
            if wg_messages is not None and wg_messages.has_op("messages"):
                start_iso = f"{start}T00:00:00Z"
                end_iso = f"{end}T23:59:59Z"
                try:
                    msgs = _fetch_web_messages_for_chat(
                        wg_messages,
                        chat_id=str(chat_id),
                        start_iso=start_iso,
                        end_iso=end_iso,
                        page_size=50,
                        max_messages=max_messages_per_chat,
                    )
                    messages = [_normalize_message(m) for m in msgs]
                except Exception as e:
                    messages = []
            else:
                # Fallback на lastMessage
                pseudo_messages = []
                for key in ("lastNotSystemMessage", "lastMessage"):
                    m = raw_chat.get(key)
                    if not isinstance(m, dict):
                        continue
                    author = m.get("author") or {}
                    direction = "in"
                    manager_id = None
                    if isinstance(author, dict) and author.get("__typename") == "User":
                        direction = "out"
                        manager_id = author.get("id")
                    pseudo_messages.append({
                        "id": m.get("id") or f"{chat_id}:{key}",
                        "chatId": chat_id,
                        "direction": direction,
                        "sentAt": m.get("time"),
                        "text": m.get("content") or "",
                        "managerId": manager_id,
                    })
                seen_ids = set()
                normed = []
                for m in pseudo_messages:
                    mid = str(m.get("id") or "")
                    if mid and mid not in seen_ids:
                        seen_ids.add(mid)
                        normed.append(m)
                messages = [_normalize_message(m) for m in normed]

            # Вычисляем метрики
            metrics = compute_chat_metrics(
                chat=chat,
                messages=messages,
                users_by_id=users_combined,
                tz_name=tz_name,
                work_hours=work_hours,
            )

            # Проверяем заказы
            has_order = "Нет"
            payment_status = "N/A"
            payment_status_ru = "N/A"
            is_successful = "Нет"
            related_order_id = chat.get("orderId", "")

            if enable_order_check:
                client_id = chat.get("clientId", "")
                if client_id and str(client_id).strip():
                    try:
                        orders = get_orders_by_customer_cached(
                            client,
                            str(client_id),
                            orders_cache,
                            limit=20,
                            max_pages=3,
                        )
                        if orders:
                            related_order = find_related_order(orders, chat.get("createdAt"), days_window=30)
                            if related_order:
                                related_order_id = str(related_order.get("id") or related_order.get("number") or "")
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
                    except Exception:
                        pass

            # Добавляем в списки
            chats_rows.append({
                "chat_id": chat_id,
                "channel": chat.get("channel", ""),
                "manager_id": metrics.manager_id or chat.get("managerId", ""),
                "manager_name": metrics.manager_name,
                "client_id": chat.get("clientId", ""),
                "order_id": related_order_id,
                "has_order": has_order,
                "payment_status": payment_status,
                "payment_status_ru": payment_status_ru,
                "is_successful": is_successful,
                "created_at": chat.get("createdAt", ""),
                "updated_at": chat.get("updatedAt", ""),
                "status": chat.get("status", ""),
                "inbound_count": metrics.inbound_count,
                "outbound_count": metrics.outbound_count,
                "first_response_sec": metrics.first_response_sec if metrics.first_response_sec is not None else "",
                "unanswered_inbound": metrics.unanswered_inbound,
            })

            for m in messages:
                messages_rows.append({
                    "chat_id": chat_id,
                    "message_id": m.get("id", ""),
                    "sent_at": m.get("sentAt", ""),
                    "direction": m.get("direction", ""),
                    "manager_id": m.get("managerId", ""),
                    "message_type": (m.get("raw") or {}).get("messageType") if isinstance(m.get("raw"), dict) else "",
                    "author_type": (m.get("raw") or {}).get("authorType") if isinstance(m.get("raw"), dict) else "",
                    "text": (m.get("text") or "").replace("\n", " ").strip(),
                })

            processed += 1

            # Записываем партию
            if processed % batch_size == 0:
                batch_num += 1
                print(f"{'='*60}")
                print(f"💾 Записываю партию {batch_num} ({batch_size} чатов)...")
                
                if chats_rows:
                    chats_header = [
                        "chat_id", "channel", "manager_id", "manager_name", "client_id", "order_id",
                        "has_order", "payment_status", "payment_status_ru", "is_successful",
                        "created_at", "updated_at", "status",
                        "inbound_count", "outbound_count", "first_response_sec", "unanswered_inbound",
                    ]
                    append_to_worksheet(
                        ss,
                        "chats_raw",
                        rows=dicts_to_table(chats_rows, header=chats_header)[1:],
                        header=None,
                    )
                    print(f"   ✅ Записано {len(chats_rows)} чатов")
                    chats_rows = []

                if messages_rows:
                    messages_header = ["chat_id", "message_id", "sent_at", "direction", "manager_id", "message_type", "author_type", "text"]
                    append_to_worksheet(
                        ss,
                        "messages_raw",
                        rows=dicts_to_table(messages_rows, header=messages_header)[1:],
                        header=None,
                    )
                    print(f"   ✅ Записано {len(messages_rows)} сообщений")
                    messages_rows = []

                print(f"   📊 Всего обработано: {processed}, пропущено: {skipped}")
                print(f"{'='*60}\n")

            if processed % 50 == 0:
                print(f"Processed chats: {processed} (skipped: {skipped})")

        except Exception as e:
            print(f"⚠️ Ошибка при обработке чата {chat_id}: {e}")
            skipped += 1
            continue

    # Записываем оставшиеся данные
    if chats_rows or messages_rows:
        print(f"\n💾 Записываю последнюю партию...")
        
        if chats_rows:
            chats_header = [
                "chat_id", "channel", "manager_id", "manager_name", "client_id", "order_id",
                "has_order", "payment_status", "payment_status_ru", "is_successful",
                "created_at", "updated_at", "status",
                "inbound_count", "outbound_count", "first_response_sec", "unanswered_inbound",
            ]
            append_to_worksheet(
                ss,
                "chats_raw",
                rows=dicts_to_table(chats_rows, header=chats_header)[1:],
                header=None,
            )
            print(f"   ✅ Записано {len(chats_rows)} чатов")

        if messages_rows:
            messages_header = ["chat_id", "message_id", "sent_at", "direction", "manager_id", "message_type", "author_type", "text"]
            append_to_worksheet(
                ss,
                "messages_raw",
                rows=dicts_to_table(messages_rows, header=messages_header)[1:],
                header=None,
            )
            print(f"   ✅ Записано {len(messages_rows)} сообщений")

    print(f"\n{'='*60}")
    print(f"✅ ДОЗАГРУЗКА ЗАВЕРШЕНА!")
    print(f"   Обработано: {processed}")
    print(f"   Пропущено: {skipped}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

