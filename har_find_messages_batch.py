from __future__ import annotations

"""
Автопоиск GraphQL batch-запроса, который тянет ленту сообщений, по HAR файлу.

Зачем: чтобы не искать "правильный batch" руками среди десятков одинаковых.

Как использовать:
1) DevTools → Network → Fetch/XHR
2) Включи Preserve log
3) Открой чат и ПРОКРУТИ ЛЕНТУ ВВЕРХ, чтобы подгрузилась история
4) Export HAR (with content) / "Save all as HAR with content"
5) Сохрани файл в папку проекта как `chats.har`
6) Запусти:
     source .venv/bin/activate
     python har_find_messages_batch.py

Скрипт:
- найдёт запросы на .../api/graphql/v1/batch
- выберет те, где в body есть "messages"/"dialog"/"MessageConnection"
- создаст `web_messages_curl.txt` автоматически
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _extract_operation_names(post_text: str) -> List[str]:
    names: List[str] = []
    try:
        body = json.loads(post_text)
        if isinstance(body, list):
            for item in body:
                if isinstance(item, dict) and item.get("operationName"):
                    names.append(str(item["operationName"]))
    except Exception:
        pass
    return names


def _score(post_text: str) -> int:
    t = post_text.lower()
    score = 0
    # сильные сигналы
    for kw in ("messages", "messageconnection", "dialog", "messagefragment", "messagedata"):
        if kw in t:
            score += 5
    # связи edges/pageInfo обычно у ленты
    for kw in ("edges", "pageinfo"):
        if kw in t:
            score += 1
    return score


def _har_entries(har: Dict[str, Any]) -> List[Dict[str, Any]]:
    return (((har.get("log") or {}).get("entries")) or [])


def _headers_to_dict(headers: List[Dict[str, Any]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for h in headers:
        name = str(h.get("name") or "")
        value = str(h.get("value") or "")
        if not name:
            continue
        # drop pseudo-headers, keep common ones
        if name.startswith(":"):
            continue
        out[name] = value
    return out


def _build_curl(url: str, headers: Dict[str, str], post_text: str) -> str:
    # ВАЖНО: cookies/токены остаются локально в файле, не отправляйте его в чат.
    lines = [f"curl '{url}' \\"]
    # важные хедеры
    preferred = [
        "accept",
        "content-type",
        "origin",
        "referer",
        "user-agent",
        "x-client-token",
        "cookie",
        "authorization",
    ]
    used = set()
    for k in preferred:
        for hk, hv in headers.items():
            if hk.lower() == k and hv:
                safe_v = hv.replace("'", "'\"'\"'")
                lines.append(f"  -H '{hk}: {safe_v}' \\")
                used.add(hk)
    # остальные (минимально)
    for hk, hv in headers.items():
        if hk in used:
            continue
        if hk.lower() in ("accept-language", "sec-fetch-site", "sec-fetch-mode", "sec-fetch-dest", "connection", "sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform"):
            safe_v = hv.replace("'", "'\"'\"'")
            lines.append(f"  -H '{hk}: {safe_v}' \\")

    safe_body = post_text.replace("'", "'\"'\"'")
    lines.append(f"  --data-raw '{safe_body}'")
    return "\n".join(lines) + "\n"


def main() -> None:
    base = Path(__file__).resolve().parent
    # 1) Явный путь через env (если хочется хранить в "Сырье")
    env_path = None
    try:
        import os

        env_path = os.environ.get("HAR_FILE")
    except Exception:
        env_path = None

    # 2) Дефолтно ожидаем chats.har рядом со скриптом
    har_path = Path(env_path).expanduser() if env_path else (base / "chats.har")

    # 3) Если нет — попробуем найти любой *.har в Cursor/INSTINTO/Сырье
    if not har_path.exists():
        hint = Path(__file__).resolve()
        cursor_root = None
        for p in hint.parents:
            if p.name == "Cursor":
                cursor_root = p
                break
        if cursor_root is not None:
            raw_dir = cursor_root / "INSTINTO" / "Сырье"
            if raw_dir.exists():
                hars = sorted(raw_dir.glob("*.har"), key=lambda p: p.stat().st_mtime, reverse=True)
                if hars:
                    har_path = hars[0]

    if not har_path.exists():
        raise SystemExit(
            "Не найден HAR.\n"
            f"- положи файл рядом как {base / 'chats.har'}\n"
            "- или положи *.har в Cursor/INSTINTO/Сырье/\n"
            "- или задай HAR_FILE='/abs/path/to/file.har'"
        )

    har = json.loads(har_path.read_text(encoding="utf-8"))
    candidates: List[Tuple[int, int, Dict[str, Any]]] = []  # (score, size, entry)

    for e in _har_entries(har):
        req = e.get("request") or {}
        url = str(req.get("url") or "")
        if "/api/graphql/v1/batch" not in url:
            continue
        post = (req.get("postData") or {}).get("text") or ""
        if not post:
            continue
        s = _score(post)
        if s <= 0:
            continue
        size = int(e.get("response", {}).get("bodySize") or 0)
        candidates.append((s, size, e))

    if not candidates:
        raise SystemExit(
            "Не нашёл кандидатов messages/dialog в HAR.\n"
            "Убедись, что ты открыл чат и пролистал ленту вверх, а HAR сохранён 'with content'."
        )

    # лучший: максимальный score, затем size
    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
    best = candidates[0][2]
    req = best.get("request") or {}
    url = str(req.get("url") or "")
    headers = _headers_to_dict(req.get("headers") or [])
    post_text = (req.get("postData") or {}).get("text") or ""
    op_names = _extract_operation_names(post_text)

    curl_text = _build_curl(url, headers, post_text)
    out_path = base / "web_messages_curl.txt"
    out_path.write_text(curl_text, encoding="utf-8")

    print("OK: created web_messages_curl.txt from HAR")
    print("Used HAR:", str(har_path))
    print("Selected URL:", url)
    print("Operation names (first 20):", op_names[:20])
    print("Now run: python probe_web_messages.py")


if __name__ == "__main__":
    main()


