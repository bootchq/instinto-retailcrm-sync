from __future__ import annotations

"""
Probe для web-эндпоинта сообщений чата (GraphQL batch).

1) В браузере открой https://instinto.retailcrm.ru/chats
2) Открой любой чат (справа должна появиться лента сообщений)
3) DevTools → Network → Fetch/XHR
4) Найди запрос "batch" на https://mg-s1.retailcrm.pro/api/graphql/v1/batch,
   который возвращает messages/edges/node (или message/MessageFragment и т.п.)
5) Copy → Copy as cURL (bash)
6) Вставь в файл web_messages_curl.txt (он уже создан рядом)

Запуск:
  source .venv/bin/activate
  python probe_web_messages.py
"""

import json
from pathlib import Path

from curl_import import fetch_json_from_curl, load_curl_file, parse_curl_bash


def main() -> None:
    curl_path = Path(__file__).with_name("web_messages_curl.txt")
    if not curl_path.exists():
        raise SystemExit(f"Файл не найден: {curl_path}")

    curl_cmd = load_curl_file(str(curl_path)).strip()
    if not curl_cmd or curl_cmd.startswith("#"):
        raise SystemExit(f"Файл пустой/шаблонный: {curl_path}. Вставь туда Copy as cURL (bash) для запроса сообщений.")

    # 1) Печатаем, что именно за batch вы скопировали (operationName),
    # чтобы отличить "список чатов" от "ленты сообщений".
    parsed = parse_curl_bash(curl_cmd)
    op_names = []
    try:
        body = json.loads(parsed.data or "null")
        if isinstance(body, list):
            for item in body:
                if isinstance(item, dict) and item.get("operationName"):
                    op_names.append(str(item["operationName"]))
    except Exception:
        pass

    req, data = fetch_json_from_curl(curl_cmd, raise_http=False)
    print("=" * 80)
    print("REQUEST")
    print("=" * 80)
    print(
        json.dumps(
            {
                "method": req.method,
                "url": req.url,
                "headers_keys": sorted(req.headers.keys()),
                "operation_names_in_body": op_names[:30],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print("=" * 80)
    print("RESPONSE (head)")
    print("=" * 80)
    print(json.dumps(data, ensure_ascii=False, indent=2)[:8000])

    # 2) Быстрый хинт: этот curl не про сообщения.
    # Ищем явные признаки сообщений в ответе/операциях.
    joined_ops = " ".join(op_names).lower()
    looks_like_messages = ("message" in joined_ops) or ("messages" in joined_ops) or ("dialog" in joined_ops)
    if not looks_like_messages:
        print("\n" + "=" * 80)
        print("HINT")
        print("=" * 80)
        print(
            "Этот batch похож на общий (counters/chat/chatsList), а не на ленту сообщений.\n"
            "Нужно в Network поймать запрос, который появляется, когда ты прокручиваешь ленту сообщений ВВЕРХ (подгрузка истории).\n"
            "Обычно он тоже называется 'batch', но в Payload будут operationName с 'messages'/'dialog' и в Response будет messages/edges."
        )


if __name__ == "__main__":
    main()


