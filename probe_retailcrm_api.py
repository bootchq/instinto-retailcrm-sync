from __future__ import annotations

"""
Небольшая диагностика, чтобы быстро понять:
- доступны ли эндпоинты чатов
- какой формат данных возвращается (ключи, поля)

Запуск:
  python probe_retailcrm_api.py
"""

import json
import os
from typing import Any, Dict

try:
    from dotenv import load_dotenv  # type: ignore[import-not-found]
except Exception:
    def load_dotenv(*args: Any, **kwargs: Any) -> bool:  # type: ignore[no-redef]
        return False

from retailcrm_client import RetailCrmClient
from retailcrm_endpoints import DEFAULT_ENDPOINTS


def _pp(title: str, obj: Any) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)
    print(json.dumps(obj, ensure_ascii=False, indent=2)[:5000])


def main() -> None:
    env_file = os.environ.get("RETAILCRM_ENV_FILE", "env")
    load_dotenv(env_file, override=False)

    client = RetailCrmClient.from_env()

    # 1) users (проверяем базовую авторизацию)
    users = client.get_users()
    _pp("OK: users sample (up to 3)", list(users.values())[:3])

    # 2) chats — путь в API может отличаться в зависимости от подключённого модуля/интеграции.
    # Сначала пробуем набор кандидатов и смотрим, что вернёт 200/400/403 вместо 404.
    start = "2000-01-01"
    end = "2100-01-01"
    candidates = [
        DEFAULT_ENDPOINTS.chats,
        "/api/v5/communications/chats",
        "/api/v5/communication/chats",
        "/api/v5/communications/dialogs",
        "/api/v5/messenger/chats",
        "/api/v5/messengers/chats",
        "/api/v5/chats/list",
    ]

    probe_results = []
    for p in candidates:
        probe_results.append(
            client.probe_get(
                p,
                params={
                    "limit": 20,
                    "page": 1,
                    "startDate": start,
                    "endDate": end,
                },
            )
        )

    _pp("Chats endpoint probe (look for status=200/400/403; 404 means method doesn't exist)", probe_results)

    if all(r.get("status") == 404 for r in probe_results):
        _pp(
            "Chats: NOT FOUND IN PUBLIC API",
            {
                "hint": "Похоже, что 'Чаты' в вашем аккаунте не доступны через публичный /api/v5/* метод. Нужно взять реальный URL через DevTools.",
                "how_to": [
                    "Открой https://instinto.retailcrm.ru/chats",
                    "F12 → Network",
                    "Фильтр: Fetch/XHR",
                    "Обнови страницу и кликни на любой чат",
                    "Найди запрос, который возвращает список диалогов/сообщений (JSON)",
                    "Скопируй Request URL (можно без домена) и query/body параметры",
                ],
            },
        )
        return

    _pp(
        "Next step",
        {
            "note": "Пришлите сюда строки из probe_results, где status != 404 (особенно 200/400/403). Я подставлю правильный путь в retailcrm_endpoints.py и включу выгрузку сообщений.",
            "current_defaults": {"chats": DEFAULT_ENDPOINTS.chats, "chat_messages": DEFAULT_ENDPOINTS.chat_messages},
        },
    )


if __name__ == "__main__":
    main()


