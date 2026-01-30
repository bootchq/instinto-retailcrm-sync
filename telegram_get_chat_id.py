from __future__ import annotations

"""
Помогает получить TELEGRAM_CHAT_ID для лички, без браузера.

Шаги:
1) В Telegram напиши своему боту любое сообщение (например "test")
2) В env укажи TELEGRAM_BOT_TOKEN="..."
3) Запусти:
   python telegram_get_chat_id.py
Скрипт выведет найденные chat.id.
"""

import json
from pathlib import Path
from typing import Any, Dict, List

import requests


def _load_env(env_path: Path) -> Dict[str, str]:
    env: Dict[str, str] = {}
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def main() -> None:
    base = Path(__file__).resolve().parent
    env = _load_env(base / "env")
    token = str(env.get("TELEGRAM_BOT_TOKEN", "")).strip()
    if not token:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN in env")

    # Проверка токена
    me = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=30).json()
    if not me.get("ok"):
        raise SystemExit(f"Token check failed: {json.dumps(me, ensure_ascii=False)}")

    data = requests.get(f"https://api.telegram.org/bot{token}/getUpdates", timeout=30).json()
    if not data.get("ok"):
        raise SystemExit(f"getUpdates failed: {json.dumps(data, ensure_ascii=False)}")

    updates: List[Dict[str, Any]] = data.get("result") or []
    if not updates:
        raise SystemExit("No updates yet. Send a message to the bot first, then rerun.")

    seen = set()
    print("Found chats:")
    for u in updates:
        msg = (u.get("message") or u.get("edited_message") or u.get("channel_post") or {})
        chat = (msg.get("chat") or {})
        cid = chat.get("id")
        ctype = chat.get("type")
        uname = chat.get("username") or ""
        title = chat.get("title") or ""
        if cid is None:
            continue
        key = (cid, ctype, uname, title)
        if key in seen:
            continue
        seen.add(key)
        print(f"- chat.id={cid} type={ctype} username={uname} title={title}")


if __name__ == "__main__":
    main()

