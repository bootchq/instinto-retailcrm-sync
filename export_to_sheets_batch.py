"""
Выгрузка чатов с пакетной обработкой и промежуточным сохранением.

Обрабатывает чаты партиями по 500 и записывает в Google Sheets после каждой партии,
чтобы не потерять прогресс при сбоях.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

try:
    from dotenv import load_dotenv  # type: ignore[import-not-found]
except Exception:
    def load_dotenv(*args: Any, **kwargs: Any) -> bool:
        return False

from analysis_rules import aggregate_channel_summary, aggregate_manager_summary, compute_chat_metrics
from dateutil import parser as dtparser
from datetime import timedelta
from retailcrm_client import RetailCrmClient, RetailCrmError
from sheets import dicts_to_table, open_spreadsheet, upsert_worksheet
from web_graphql import WebGraphQLClient, WebGraphQLError


def _env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, default)
    if v is None or not str(v).strip():
        raise RuntimeError(f"Missing env var: {name}")
    return str(v).strip()


def _normalize_message(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Приводит сообщение к минимальному общему формату."""
    direction = raw.get("direction") or raw.get("type") or raw.get("inOut")
    if direction in ("incoming", "inbound", "in"):
        direction = "in"
    if direction in ("outgoing", "outbound", "out"):
        direction = "out"

    sent_at = raw.get("sentAt") or raw.get("createdAt") or raw.get("date") or raw.get("time")
    text = raw.get("text") or raw.get("message") or raw.get("body") or ""
    manager_id = raw.get("managerId") or raw.get("userId") or raw.get("operatorId")

    return {
        "id": raw.get("id") or raw.get("messageId") or "",
        "chatId": raw.get("chatId") or raw.get("chat_id") or "",
        "direction": direction,
        "sentAt": sent_at,
        "text": text,
        "managerId": manager_id,
        "raw": raw,
    }


def _normalize_chat(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Приводит чат к минимальному общему формату."""
    return {
        "id": raw.get("id") or raw.get("chatId") or "",
        "channel": raw.get("channel") or raw.get("source") or raw.get("type") or "",
        "clientId": raw.get("clientId") or raw.get("customerId") or raw.get("contactId") or "",
        "orderId": raw.get("orderId") or raw.get("dealId") or "",
        "managerId": raw.get("managerId") or raw.get("userId") or "",
        "createdAt": raw.get("createdAt") or raw.get("dateCreate") or "",
        "updatedAt": raw.get("updatedAt") or raw.get("dateUpdate") or "",
        "status": raw.get("status") or "",
        "raw": raw,
    }


def _read_existing_chat_ids(ws) -> set:
    """Читает chat_id уже обработанных чатов из таблицы."""
    try:
        values = ws.get_all_values()
        if not values or len(values) < 2:
            return set()
        header = values[0]
        chat_id_idx = header.index("chat_id") if "chat_id" in header else None
        if chat_id_idx is None:
            return set()
        existing_ids = set()
        for row in values[1:]:
            if chat_id_idx < len(row) and row[chat_id_idx]:
                existing_ids.add(str(row[chat_id_idx]).strip())
        return existing_ids
    except Exception as e:
        print(f"⚠️ Ошибка при чтении существующих чатов: {e}")
        return set()


def _append_to_worksheet(ss, worksheet_name: str, rows: List[List[Any]], header: List[str]) -> None:
    """Добавляет строки в существующий лист (не очищает его)."""
    try:
        ws = ss.worksheet(worksheet_name)
    except Exception:
        # Создаём новый лист
        ws = ss.add_worksheet(title=worksheet_name, rows=200, cols=40)
        # Записываем заголовок
        ws.update(values=[header], range_name="A1")
    
    # Получаем текущие данные
    existing_values = ws.get_all_values()
    
    # Если лист пустой, добавляем заголовок
    if not existing_values:
        ws.update(values=[header], range_name="A1")
        existing_values = [header]
    
    # Добавляем новые строки
    if rows:
        # Находим первую пустую строку
        next_row = len(existing_values) + 1
        ws.update(values=rows, range_name=f"A{next_row}")


def main() -> None:
    env_file = os.environ.get("RETAILCRM_ENV_FILE", "env")
    load_dotenv(env_file, override=False)

    # Поддержка Railway: создаём curl-файлы из переменных окружения
    base_dir = Path(__file__).parent
    if os.environ.get("WEB_CURL_CONTENT"):
        web_curl_path = base_dir / "web_curl.txt"
        web_curl_path.write_text(os.environ["WEB_CURL_CONTENT"], encoding="utf-8")
        print("✅ Создан web_curl.txt из переменной окружения")
    
    if os.environ.get("WEB_MESSAGES_CURL_CONTENT"):
        web_messages_curl_path = base_dir / "web_messages_curl.txt"
        web_messages_curl_path.write_text(os.environ["WEB_MESSAGES_CURL_CONTENT"], encoding="utf-8")
        print("✅ Создан web_messages_curl.txt из переменной окружения")

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
            print(f"📅 Автоматический период: последние {days} дней ({start} - {end})")
        except (ValueError, TypeError):
            print(f"⚠️ LAST_DAYS должен быть числом, используем START_DATE/END_DATE")
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
    max_total_messages = int(os.environ.get("MAX_TOTAL_MESSAGES", "200000"))
    batch_size = int(os.environ.get("BATCH_SIZE", "500"))  # Размер партии
    web_timeout_s = int(os.environ.get("WEB_TIMEOUT_S", "180"))
    web_retries = int(os.environ.get("WEB_RETRIES", "5"))

    ss = open_spreadsheet(
        spreadsheet_id=_env("GOOGLE_SHEETS_ID"),
        service_account_json_path=_env("GOOGLE_SERVICE_ACCOUNT_JSON"),
    )

    # Читаем уже обработанные чаты
    print("📖 Проверяю уже обработанные чаты...")
    try:
        chats_ws = ss.worksheet("chats_raw")
        existing_chat_ids = _read_existing_chat_ids(chats_ws)
        print(f"   Найдено уже обработанных чатов: {len(existing_chat_ids)}")
    except Exception:
        existing_chat_ids = set()
        chats_ws = None

    # Подключаемся к RetailCRM
    client = RetailCrmClient.from_env()
    users = client.get_users()
    
    orders_cache: Dict[str, List[Dict[str, Any]]] = {}
    enable_order_check = os.environ.get("ENABLE_ORDER_CHECK", "1") == "1"

    # Получаем чаты
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

            # Импортируем функцию из export_to_sheets.py
            from export_to_sheets import _iter_web_chats
            web_chats = _iter_web_chats(wg, start_iso=start_iso, end_iso=end_iso, channel_types=wanted_types)
    except Exception as e:
        print(f"WEB chats disabled / failed: {e}")

    if not web_chats:
        raise RuntimeError("Не удалось получить чаты")

    # Фильтруем уже обработанные
    new_chats = [c for c in web_chats if str(c.get("id", "")) not in existing_chat_ids]
    print(f"   Всего чатов: {len(web_chats)}")
    print(f"   Новых чатов для обработки: {len(new_chats)}")

    if not new_chats:
        print("✅ Все чаты уже обработаны!")
        return

    # Обрабатываем партиями
    total_processed = len(existing_chat_ids)
    total_skipped = 0
    batch_num = 0

    for batch_start in range(0, len(new_chats), batch_size):
        batch_num += 1
        batch_end = min(batch_start + batch_size, len(new_chats))
        batch = new_chats[batch_start:batch_end]
        
        print(f"\n{'='*60}")
        print(f"📦 ПАРТИЯ {batch_num}: обработка чатов {batch_start+1}-{batch_end} из {len(new_chats)}")
        print(f"{'='*60}")

        batch_chats_rows: List[Dict[str, Any]] = []
        batch_messages_rows: List[Dict[str, Any]] = []
        batch_metrics_rows = []
        batch_processed = 0
        batch_skipped = 0

        # Импортируем функции из export_to_sheets.py
        from export_to_sheets import (
            _fetch_web_messages_for_chat,
            _web_message_to_minimal,
            _parse_iso,
            find_related_order,
            _get_payment_status,
        )

        for raw_chat in batch:
            chat_id = str(raw_chat.get("id", ""))
            if not chat_id:
                continue

            try:
                # Нормализуем чат
                chat = _normalize_chat(raw_chat)
                
                # Получаем сообщения
                messages: List[Dict[str, Any]] = []
                # ... (логика получения сообщений из export_to_sheets.py)
                
                # Вычисляем метрики
                metrics = compute_chat_metrics(
                    chat,
                    messages,
                    users_by_id={u.get("id"): u for u in users},
                    tz_name=tz_name,
                    work_hours=work_hours,
                )

                # Получаем данные о заказе
                client_id = chat.get("clientId", "")
                has_order = "Нет"
                payment_status = "N/A"
                payment_status_ru = "N/A"
                is_successful = "Нет"
                order_id = ""

                if client_id and enable_order_check:
                    if client_id not in orders_cache:
                        # Загружаем заказы для клиента
                        orders_cache[client_id] = []  # Упрощённо, нужна полная логика
                    
                    # ... (логика определения заказа)

                batch_chats_rows.append({
                    "chat_id": chat_id,
                    "channel": chat.get("channel", ""),
                    "manager_id": metrics.manager_id or chat.get("managerId", ""),
                    "manager_name": metrics.manager_name,
                    "client_id": client_id,
                    "order_id": order_id,
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

                # Добавляем сообщения
                for msg in messages:
                    batch_messages_rows.append({
                        "chat_id": chat_id,
                        "message_id": msg.get("id", ""),
                        "sent_at": msg.get("sentAt", ""),
                        "direction": msg.get("direction", ""),
                        "manager_id": msg.get("managerId", ""),
                        "text": msg.get("text", ""),
                    })

                batch_metrics_rows.append(metrics)
                batch_processed += 1

            except Exception as e:
                print(f"⚠️ Ошибка при обработке чата {chat_id}: {e}")
                batch_skipped += 1
                continue

        # Записываем партию в Google Sheets
        print(f"\n💾 Записываю партию {batch_num} в Google Sheets...")
        
        if batch_chats_rows:
            chats_header = [
                "chat_id", "channel", "manager_id", "manager_name", "client_id", "order_id",
                "has_order", "payment_status", "payment_status_ru", "is_successful",
                "created_at", "updated_at", "status",
                "inbound_count", "outbound_count", "first_response_sec", "unanswered_inbound",
            ]
            _append_to_worksheet(ss, "chats_raw", dicts_to_table(batch_chats_rows, header=chats_header)[1:], chats_header)

        if batch_messages_rows:
            messages_header = ["chat_id", "message_id", "sent_at", "direction", "manager_id", "text"]
            _append_to_worksheet(ss, "messages_raw", dicts_to_table(batch_messages_rows, header=messages_header)[1:], messages_header)

        total_processed += batch_processed
        total_skipped += batch_skipped

        print(f"✅ Партия {batch_num} записана: обработано {batch_processed}, пропущено {batch_skipped}")
        print(f"📊 Всего обработано: {total_processed}, пропущено: {total_skipped}")

    print(f"\n{'='*60}")
    print(f"✅ ВЫГРУЗКА ЗАВЕРШЕНА!")
    print(f"   Всего обработано: {total_processed}")
    print(f"   Пропущено: {total_skipped}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

