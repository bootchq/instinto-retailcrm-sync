from __future__ import annotations

"""
Probe для web-endpoint'ов (когда /api/v5/chats не существует).

Шаги:
1) В браузере открой https://instinto.retailcrm.ru/chats
2) DevTools → Network → Fetch/XHR
3) Кликни на любой чат и найди запрос, который возвращает JSON со списком диалогов или сообщений
4) Right click → Copy → Copy as cURL (bash)
5) Вставь команду в файл `web_curl.txt` рядом с этим скриптом (в папке проекта)

Дальше:
  source .venv/bin/activate
  python probe_web_chats.py
"""

import json
from pathlib import Path

from curl_import import fetch_json_from_curl, load_curl_file


def main() -> None:
    curl_path = Path(__file__).with_name("web_curl.txt")
    if not curl_path.exists():
        curl_path.write_text(
            "# ВСТАВЬ СЮДА \"Copy as cURL (bash)\" ИЗ DEVTOOLS (Network → Fetch/XHR).\n"
            "# ВАЖНО: не отправляй содержимое этого файла в чат — там cookies/токены.\n",
            encoding="utf-8",
        )
        raise SystemExit(
            f"Я создал шаблон: {curl_path}\n"
            "Теперь вставь туда Copy as cURL (bash) и запусти probe_web_chats.py ещё раз."
        )

    curl_cmd = load_curl_file(str(curl_path))
    req, data = fetch_json_from_curl(curl_cmd, raise_http=False)
    print("=" * 80)
    print("REQUEST")
    print("=" * 80)
    print(json.dumps({"method": req.method, "url": req.url, "headers_keys": sorted(req.headers.keys())}, ensure_ascii=False, indent=2))
    print("=" * 80)
    print("RESPONSE (head)")
    print("=" * 80)
    print(json.dumps(data, ensure_ascii=False, indent=2)[:8000])


if __name__ == "__main__":
    main()


