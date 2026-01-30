from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

try:
    from dotenv import load_dotenv  # type: ignore[import-not-found]
except Exception:
    # Фолбэк: если python-dotenv не установлен, просто не загружаем env-файл.
    def load_dotenv(*args: Any, **kwargs: Any) -> bool:  # type: ignore[no-redef]
        return False

from analysis_rules import aggregate_channel_summary, aggregate_manager_summary, compute_chat_metrics
from dateutil import parser as dtparser
from datetime import timedelta
from retailcrm_client import RetailCrmClient, RetailCrmError
from sheets import dicts_to_table, open_spreadsheet, upsert_worksheet, append_to_worksheet, get_existing_chat_ids
from web_graphql import WebGraphQLClient, WebGraphQLError


def _env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, default)
    if v is None or not str(v).strip():
        raise RuntimeError(f"Missing env var: {name}")
    return str(v).strip()


def _normalize_message(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Приводит сообщение к минимальному общему формату.

    Если в вашем API поля называются иначе — правьте здесь (1 место).
    """
    # direction: in/out
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
    """
    Приводит чат к минимальному общему формату.

    Если в вашем API поля называются иначе — правьте здесь (1 место).
    """
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


def _web_channel_ids(client: WebGraphQLClient) -> Dict[str, List[int]]:
    """
    Возвращает ids каналов по type (INSTAGRAM/WHATSAPP/...)
    """
    if not client.has_op("ChannelsList"):
        raise WebGraphQLError("Operation ChannelsList not found in batch. Capture a 'batch' request that includes ChannelsList.")
    data = client.request_batch([client.build_op("ChannelsList")])
    # batch response is list; first item has data.channels.edges
    item = data[0] if isinstance(data, list) and data else {}
    channels = (((item or {}).get("data") or {}).get("channels") or {}).get("edges") or []
    by_type: Dict[str, List[int]] = {}
    for e in channels:
        node = (e or {}).get("node") or {}
        t = str(node.get("type") or "").upper()
        cid = node.get("id")
        if not t or cid is None:
            continue
        try:
            by_type.setdefault(t, []).append(int(cid))
        except Exception:
            continue
    return by_type


def _iter_web_chats(
    client: WebGraphQLClient,
    *,
    start_iso: str,
    end_iso: str,
    channel_types: List[str],
) -> List[Dict[str, Any]]:
    """
    Выгружает список чатов через web GraphQL (операция chatsList).
    Возвращает список node из edges.
    """
    if not client.has_op("chatsList"):
        raise WebGraphQLError("Operation chatsList not found in batch. Capture a 'batch' request that includes chatsList.")

    by_type = _web_channel_ids(client)
    channel_ids: List[int] = []
    for t in channel_types:
        channel_ids.extend(by_type.get(t.upper(), []))

    # filter format встречается в payload из UI; поддержка communicationTime есть в шаблонах фильтра
    filter_obj: Dict[str, Any] = {
        "channelIds": channel_ids,
        "tags": [],
        "userIds": [],
        "botIds": [],
        "tagsFilteringMode": "ALL",
        "communicationTime": {"since": start_iso, "until": end_iso},
    }

    out: List[Dict[str, Any]] = []
    after: Any = None
    while True:
        variables = {
            "sort": "BY_LAST_ACTIVITY",
            "filter": filter_obj,
            "first": 50,
        }
        if after:
            variables["after"] = after
        data = client.request_batch([client.build_op("chatsList", variables=variables)])
        item = data[0] if isinstance(data, list) and data else {}
        chats = (((item or {}).get("data") or {}).get("chats") or {})
        edges = chats.get("edges") or []
        for e in edges:
            node = (e or {}).get("node")
            if node:
                out.append(node)
        page_info = chats.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        after = page_info.get("endCursor")
        if not after:
            break
    return out


def _parse_iso(dt_str: Any) -> Any:
    if not dt_str:
        return None
    try:
        return dtparser.isoparse(str(dt_str))
    except Exception:
        return None


def _parse_dt(v: Any) -> Any:
    """Парсит дату из строки."""
    if not v:
        return None
    try:
        return dtparser.parse(str(v))
    except Exception:
        return None


def get_orders_by_customer_cached(
    client: RetailCrmClient,
    customer_id: str,
    cache: Dict[str, List[Dict[str, Any]]],
    limit: int = 20,
    max_pages: int = 3,
) -> List[Dict[str, Any]]:
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
    
    cache[customer_id] = all_orders
    return all_orders


def find_related_order(
    orders: List[Dict[str, Any]],
    chat_created_at: Any,
    days_window: int = 30,
) -> Optional[Dict[str, Any]]:
    """Находит заказ, связанный с чатом по дате."""
    if not orders or not chat_created_at:
        return None
    
    chat_dt = _parse_dt(chat_created_at)
    if not chat_dt:
        return None
    
    if chat_dt.tzinfo:
        chat_dt = chat_dt.replace(tzinfo=None)
    
    window_end = chat_dt + timedelta(days=days_window)
    
    related_orders = []
    for order in orders:
        order_created = _parse_dt(order.get("createdAt") or order.get("created_at"))
        if order_created:
            if order_created.tzinfo:
                order_created = order_created.replace(tzinfo=None)
            
            if chat_dt <= order_created <= window_end:
                related_orders.append((order_created, order))
    
    if not related_orders:
        return None
    
    related_orders.sort(key=lambda x: x[0])
    return related_orders[0][1]


def determine_payment_status(order: Dict[str, Any]) -> str:
    """Определяет статус оплаты заказа."""
    if not order:
        return "unknown"
    
    total_sum = float(order.get("totalSumm") or order.get("total_summ") or order.get("totalSum") or 0)
    prepay_sum = float(order.get("prepaySum") or order.get("prepay_sum") or order.get("prepay") or 0)
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
            if "paid" in status or "оплачен" in status or "success" in status:
                return "paid"
    
    status = str(order.get("status") or "").lower()
    if any(word in status for word in ["paid", "оплачен", "completed", "выполнен"]):
        return "paid"
    
    return "unknown"


def _web_message_to_minimal(chat_id: str, node: Dict[str, Any]) -> Dict[str, Any]:
    """
    Привести GraphQL Message node к минимальному формату _normalize_message.
    """
    mtype = node.get("type") or node.get("__typename") or ""
    sent_at = node.get("time")

    author = node.get("author") or {}
    author_type = author.get("__typename") if isinstance(author, dict) else None
    author_id = author.get("id") if isinstance(author, dict) else None

    # direction
    direction = "in"
    manager_id = None
    if author_type == "User":
        direction = "out"
        manager_id = author_id
    elif author_type == "Bot":
        direction = "out"
        manager_id = None
    elif author_type == "Customer":
        direction = "in"

    # text/content
    text = (
        node.get("content")
        or node.get("note")
        or node.get("action")
        or ""
    )

    return {
        "id": node.get("id") or "",
        "chatId": chat_id,
        "direction": direction,
        "sentAt": sent_at,
        "text": text,
        "managerId": manager_id,
        "messageType": mtype,
        "authorType": author_type or "",
    }


def _fetch_web_messages_for_chat(
    wg_messages: WebGraphQLClient,
    *,
    chat_id: str,
    start_iso: str,
    end_iso: str,
    page_size: int = 50,
    max_messages: int = 500,
) -> List[Dict[str, Any]]:
    """
    Забираем сообщения чата через operationName 'messages' (пагинация назад).
    Возвращаем минимальные dict'ы (для messages_raw + метрик).
    """
    if not wg_messages.has_op("messages"):
        raise WebGraphQLError("Operation 'messages' not found in messages curl batch")

    start_dt = _parse_iso(start_iso)
    end_dt = _parse_iso(end_iso)

    out: List[Dict[str, Any]] = []
    before = None

    while True:
        variables: Dict[str, Any] = {
            "filter": {"chatId": str(chat_id), "withDeleted": True},
            "last": int(page_size),
        }
        if before:
            variables["before"] = before

        data = wg_messages.request_batch([wg_messages.build_op("messages", variables=variables)])
        item = data[0] if isinstance(data, list) and data else {}
        conn = ((item or {}).get("data") or {}).get("messages") or {}
        edges = conn.get("edges") or []
        page_info = conn.get("pageInfo") or {}

        if not edges:
            break

        # edges идут от старых к новым внутри страницы? не полагаемся; соберём и отсортируем
        nodes = [(e or {}).get("node") for e in edges]
        nodes = [n for n in nodes if isinstance(n, dict)]

        # фильтрация по времени
        page_times = []
        for n in nodes:
            t = _parse_iso(n.get("time"))
            if t:
                page_times.append(t)

        for n in nodes:
            t = _parse_iso(n.get("time"))
            if start_dt and t and t < start_dt:
                continue
            if end_dt and t and t > end_dt:
                continue
            out.append(_web_message_to_minimal(chat_id, n))
            if len(out) >= max_messages:
                return out

        # если самая ранняя в этой странице уже меньше start_dt — дальше только старее
        if start_dt and page_times and min(page_times) < start_dt:
            break

        if not page_info.get("hasPreviousPage"):
            break
        before = page_info.get("startCursor")
        if not before:
            break

    return out


def main() -> None:
    # Подхватываем переменные из файла `env` (без точки), чтобы не заставлять пользователя экспортить руками.
    # Можно указать явный путь: RETAILCRM_ENV_FILE="/abs/path/to/env"
    env_file = os.environ.get("RETAILCRM_ENV_FILE", "env")
    load_dotenv(env_file, override=False)

    # Поддержка Railway: создаём curl-файлы из переменных окружения, если они есть
    base_dir = Path(__file__).parent
    if os.environ.get("WEB_CURL_CONTENT"):
        web_curl_path = base_dir / "web_curl.txt"
        web_curl_path.write_text(os.environ["WEB_CURL_CONTENT"], encoding="utf-8")
        print("✅ Создан web_curl.txt из переменной окружения")
    
    if os.environ.get("WEB_MESSAGES_CURL_CONTENT"):
        web_messages_curl_path = base_dir / "web_messages_curl.txt"
        web_messages_curl_path.write_text(os.environ["WEB_MESSAGES_CURL_CONTENT"], encoding="utf-8")
        print("✅ Создан web_messages_curl.txt из переменной окружения")

    # Поддержка автоматического вычисления дат за последние N дней
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
    chat_limit = int(os.environ.get("CHAT_LIMIT", "0"))  # 0 = без лимита
    batch_size = int(os.environ.get("BATCH_SIZE", "500"))  # Размер партии для записи
    web_timeout_s = int(os.environ.get("WEB_TIMEOUT_S", "180"))
    web_retries = int(os.environ.get("WEB_RETRIES", "5"))

    ss = open_spreadsheet(
        spreadsheet_id=_env("GOOGLE_SHEETS_ID"),
        service_account_json_path=_env("GOOGLE_SERVICE_ACCOUNT_JSON"),
    )

    # Очищаем листы для полной перевыгрузки и увеличиваем размер
    print("🧹 Очищаю существующие данные для полной перевыгрузки...")
    try:
        chats_ws = ss.worksheet("chats_raw")
        chats_ws.clear()
        # Увеличиваем размер листа для больших объёмов данных
        chats_ws.resize(rows=10000, cols=50)
        print("   ✅ Очищен лист 'chats_raw', размер увеличен до 10000 строк")
    except Exception as e:
        print(f"   ⚠️ Ошибка при очистке chats_raw: {e}")
        # Пытаемся создать новый лист
        try:
            chats_ws = ss.add_worksheet(title="chats_raw", rows=10000, cols=50)
            print("   ✅ Создан новый лист 'chats_raw'")
        except Exception:
            pass
    
    try:
        messages_ws = ss.worksheet("messages_raw")
        messages_ws.clear()
        # Увеличиваем размер листа для больших объёмов данных
        messages_ws.resize(rows=100000, cols=50)  # Для сообщений нужно больше строк
        print("   ✅ Очищен лист 'messages_raw', размер увеличен до 100000 строк")
    except Exception as e:
        print(f"   ⚠️ Ошибка при очистке messages_raw: {e}")
        # Пытаемся создать новый лист
        try:
            messages_ws = ss.add_worksheet(title="messages_raw", rows=100000, cols=50)
            print("   ✅ Создан новый лист 'messages_raw'")
        except Exception:
            pass

    # 1) пользователи берём из RetailCRM API (он работает)
    client = RetailCrmClient.from_env()
    users = client.get_users()
    
    # Кеш для заказов по client_id (чтобы не делать повторные запросы)
    orders_cache: Dict[str, List[Dict[str, Any]]] = {}
    enable_order_check = os.environ.get("ENABLE_ORDER_CHECK", "1") == "1"

    chats_rows: List[Dict[str, Any]] = []
    messages_rows: List[Dict[str, Any]] = []
    metrics_rows = []

    processed = 0
    skipped = 0
    batch_num = 0

    # 2) чаты: web GraphQL (если WEB_CURL_FILE есть/заполнен)
    # Если файла нет — продолжим пытаться через public API (но у вас он 404).
    web_chats: List[Dict[str, Any]] = []
    try:
        if os.path.exists(os.path.join(os.path.dirname(__file__), web_curl_file)):
            web_curl_file = os.path.join(os.path.dirname(__file__), web_curl_file)
        if os.path.exists(web_curl_file):
            wg = WebGraphQLClient(curl_file=web_curl_file)
            # Преобразуем YYYY-MM-DD в ISO для GraphQL
            start_iso = f"{start}T00:00:00Z"
            end_iso = f"{end}T23:59:59Z"
            # CHANNELS в нашем env — whatsapp/instagram, а web types — WHATSAPP/INSTAGRAM
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

    if not web_chats:
        raise RuntimeError(
            "Не удалось получить чаты. В вашем аккаунте /api/v5/chats отсутствует (404), поэтому нужен WEB_CURL_FILE.\n"
            "Проверьте, что web_curl.txt заполнен Copy as cURL (bash) для batch запроса."
        )

    # Маппинг пользователей из web GraphQL (у него другой id-space, не совпадает с /api/v5/users)
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

    # объединяем, чтобы compute_chat_metrics умел показывать имя
    users_combined = dict(users)
    # При коллизиях (маловероятно) предпочитаем web-имя.
    users_combined.update(web_users_by_id)

    # 3) сообщения: web GraphQL (messages)
    wg_messages = None
    try:
        # искать рядом с проектом
        if os.path.exists(os.path.join(os.path.dirname(__file__), web_messages_curl_file)):
            web_messages_curl_file = os.path.join(os.path.dirname(__file__), web_messages_curl_file)
        if os.path.exists(web_messages_curl_file):
            wg_messages = WebGraphQLClient(curl_file=web_messages_curl_file, timeout_s=web_timeout_s, max_retries=web_retries)
    except Exception as e:
        print(f"WEB messages disabled / failed: {e}")
    if wg_messages is None:
        print("WEB messages: OFF (web_messages_curl.txt not found or failed to load) -> using lastMessage fallback")
    else:
        print(f"WEB messages: ON (ops={sorted(wg_messages.ops.keys())})")

    print(f"   Всего чатов для обработки: {len(web_chats)}")

    for raw_chat in web_chats:
        # Приводим web chat к нашему формату
        chat = _normalize_chat(
            {
                "id": raw_chat.get("id"),
                "channel": (raw_chat.get("channel") or {}).get("type") or (raw_chat.get("channel") or {}).get("name"),
                "clientId": (raw_chat.get("customer") or {}).get("id"),
                "managerId": ((raw_chat.get("lastDialog") or {}).get("responsible") or {}).get("id"),
                "createdAt": (raw_chat.get("lastDialog") or {}).get("createdAt") or raw_chat.get("lastActivity"),
                "updatedAt": raw_chat.get("lastActivity"),
                "status": (raw_chat.get("lastDialog") or {}).get("closedAt") and "CLOSED" or "ACTIVE",
                "raw": raw_chat,
            }
        )

        chat_id = chat["id"]
        if not str(chat_id).strip():
            skipped += 1
            continue

        # Сообщения: если есть messages curl — тянем историю, иначе fallback на lastMessage/lastNotSystemMessage.
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
                # Не валим весь прогон из-за одного чата/таймаута.
                print(f"WEB messages failed for chat_id={chat_id}: {e}. Falling back to lastMessage.")
                messages = []
        else:
            pseudo_messages: List[Dict[str, Any]] = []
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
                sent_at = m.get("time")
                text = m.get("content") or ""
                pseudo_messages.append(
                    {
                        "id": m.get("id") or f"{chat_id}:{key}",
                        "chatId": chat_id,
                        "direction": direction,
                        "sentAt": sent_at,
                        "text": text,
                        "managerId": manager_id,
                    }
                )
            # дедуп, потому что lastMessage и lastNotSystemMessage часто одинаковые
            seen_ids = set()
            normed = []
            for m in pseudo_messages:
                mid = str(m.get("id") or "")
                if mid and mid in seen_ids:
                    continue
                if mid:
                    seen_ids.add(mid)
                normed.append(m)
            messages = [_normalize_message(m) for m in normed]

        # Если web-messages был, но на этом чате не получилось — fallback на last*.
        if not messages:
            pseudo_messages: List[Dict[str, Any]] = []
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
                sent_at = m.get("time")
                text = m.get("content") or ""
                pseudo_messages.append(
                    {
                        "id": m.get("id") or f"{chat_id}:{key}",
                        "chatId": chat_id,
                        "direction": direction,
                        "sentAt": sent_at,
                        "text": text,
                        "managerId": manager_id,
                    }
                )
            seen_ids = set()
            normed = []
            for m in pseudo_messages:
                mid = str(m.get("id") or "")
                if mid and mid in seen_ids:
                    continue
                if mid:
                    seen_ids.add(mid)
                normed.append(m)
            messages = [_normalize_message(m) for m in normed]

        # таблица сообщений
        for m in messages:
            if len(messages_rows) >= max_total_messages:
                break
            messages_rows.append(
                {
                    "chat_id": chat_id,
                    "message_id": m.get("id", ""),
                    "sent_at": m.get("sentAt", ""),
                    "direction": m.get("direction", ""),
                    "manager_id": m.get("managerId", ""),
                    "message_type": (m.get("raw") or {}).get("messageType") if isinstance(m.get("raw"), dict) else m.get("messageType", ""),
                    "author_type": (m.get("raw") or {}).get("authorType") if isinstance(m.get("raw"), dict) else m.get("authorType", ""),
                    "text": (m.get("text") or "").replace("\n", " ").strip(),
                }
            )

        # аналитика/советы
        metrics = compute_chat_metrics(
            chat=chat,
            messages=messages,
            users_by_id=users_combined,
            tz_name=tz_name,
            work_hours=work_hours,
        )
        metrics_rows.append(metrics)
        
        # Проверяем заказы и оплату (если включено)
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
                        related_order = find_related_order(
                            orders,
                            chat.get("createdAt"),
                            days_window=30,
                        )
                        
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
                except Exception as e:
                    # Не прерываем выгрузку из-за ошибок заказов
                    if processed % 100 == 0:
                        print(f"  ⚠️ Ошибка при проверке заказов для чата {chat_id}: {str(e)[:80]}")

        chats_rows.append(
            {
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
            }
        )

        processed += 1
        
        # Записываем партию в Google Sheets
        if processed % batch_size == 0:
            batch_num += 1
            print(f"\n{'='*60}")
            print(f"💾 Записываю партию {batch_num} ({batch_size} чатов) в Google Sheets...")
            
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
                    header=chats_header if batch_num == 1 else None,  # Заголовок только для первой партии
                )
                print(f"   ✅ Записано {len(chats_rows)} чатов")
                chats_rows = []  # Очищаем для следующей партии
            
            if messages_rows:
                messages_header = ["chat_id", "message_id", "sent_at", "direction", "manager_id", "message_type", "author_type", "text"]
                append_to_worksheet(
                    ss,
                    "messages_raw",
                    rows=dicts_to_table(messages_rows, header=messages_header)[1:],
                    header=messages_header if batch_num == 1 else None,  # Заголовок только для первой партии
                )
                print(f"   ✅ Записано {len(messages_rows)} сообщений")
                messages_rows = []  # Очищаем для следующей партии
            
            print(f"   📊 Всего обработано: {processed}, пропущено: {skipped}")
            print(f"{'='*60}\n")
        
        if processed % 50 == 0:
            print(f"Processed chats: {processed} (skipped: {skipped})")
        if len(messages_rows) >= max_total_messages:
            print(f"Reached MAX_TOTAL_MESSAGES={max_total_messages}. Stopping message collection early.")
            break
        if chat_limit and processed >= chat_limit:
            print(f"Reached CHAT_LIMIT={chat_limit}. Stopping early for debug.")
            break

    # сводка менеджеров
    manager_summary = aggregate_manager_summary(metrics_rows)
    channel_summary = aggregate_channel_summary(metrics_rows)

    # советы по чатам
    advice_rows: List[Dict[str, Any]] = []
    for m in metrics_rows:
        advice_rows.append(
            {
                "chat_id": m.chat_id,
                "channel": m.channel,
                "manager_id": m.manager_id or "",
                "manager_name": m.manager_name,
                "inbound": m.inbound_count,
                "outbound": m.outbound_count,
                "first_response_sec": m.first_response_sec if m.first_response_sec is not None else "",
                "unanswered_inbound": m.unanswered_inbound,
                "advice": " | ".join(m.advice),
            }
        )

    # Записываем оставшиеся данные (последняя партия)
    if chats_rows or messages_rows:
        print(f"\n💾 Записываю последнюю партию в Google Sheets...")
        
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
                header=None,  # Заголовок уже есть
            )
            print(f"   ✅ Записано {len(chats_rows)} чатов")
        
        if messages_rows:
            messages_header = ["chat_id", "message_id", "sent_at", "direction", "manager_id", "message_type", "author_type", "text"]
            append_to_worksheet(
                ss,
                "messages_raw",
                rows=dicts_to_table(messages_rows, header=messages_header)[1:],
                header=None,  # Заголовок уже есть
            )
            print(f"   ✅ Записано {len(messages_rows)} сообщений")
    upsert_worksheet(
        ss,
        "manager_summary",
        rows=dicts_to_table(
            manager_summary,
            header=[
                "manager_id",
                "manager_name",
                "chats",
                "inbound",
                "outbound",
                "unanswered_inbound",
                "slow_first_reply_chats",
                "no_reply_chats",
                "responded_chats",
                "median_first_reply_sec",
                "p90_first_reply_sec",
                "response_rate",
            ],
        ),
    )
    upsert_worksheet(
        ss,
        "channel_summary",
        rows=dicts_to_table(
            channel_summary,
            header=[
                "channel",
                "chats",
                "inbound",
                "outbound",
                "no_reply_chats",
                "slow_first_reply_chats",
                "responded_chats",
                "median_first_reply_sec",
                "p90_first_reply_sec",
                "response_rate",
            ],
        ),
    )
    upsert_worksheet(
        ss,
        "chat_advice",
        rows=dicts_to_table(
            advice_rows,
            header=[
                "chat_id",
                "channel",
                "manager_id",
                "manager_name",
                "inbound",
                "outbound",
                "first_response_sec",
                "unanswered_inbound",
                "advice",
            ],
        ),
    )
    print(f"Done. Processed chats: {processed}; skipped: {skipped}. Messages: {len(messages_rows)}")


if __name__ == "__main__":
    main()


