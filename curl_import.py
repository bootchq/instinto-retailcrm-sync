from __future__ import annotations

import json
import re
import shlex
import codecs
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import requests


class CurlParseError(ValueError):
    pass


@dataclass(frozen=True)
class CurlRequest:
    method: str
    url: str
    headers: Dict[str, str]
    data: Optional[str]


def parse_curl_bash(curl_command: str) -> CurlRequest:
    """
    Парсит "Copy as cURL (bash)" из DevTools.
    Поддержка минимальная, но достаточная для RetailCRM web API (headers + cookie + data).
    """
    s = curl_command.strip()
    if not s:
        raise CurlParseError("Empty curl command")
    # DevTools обычно даёт многострочный curl с "\<newline>" для переносов.
    # В bash это означает "склей строки", поэтому делаем то же самое.
    s = s.replace("\\\r\n", " ").replace("\\\n", " ")
    if not s.lstrip().startswith("curl"):
        raise CurlParseError("Expected command to start with 'curl'")

    # Пытаемся вытащить data "как есть" из исходной строки, потому что shlex
    # не понимает bash ANSI-C quoting ($'...') и может "съесть" кавычки так,
    # что в итоге в data остаётся лишний '$'.
    raw_data_token: Optional[str] = None
    m = re.search(
        r"(?:--data-raw|--data-binary|--data|-d)\s+(\$'(?:\\.|[^'])*'|'(?:\\.|[^'])*'|\"(?:\\.|[^\"])*\"|\S+)",
        s,
    )
    if m:
        raw_data_token = m.group(1)

    tokens = shlex.split(s)
    # tokens[0] == curl
    method = "GET"
    headers: Dict[str, str] = {}
    data: Optional[str] = None
    url: Optional[str] = None

    i = 1
    while i < len(tokens):
        t = tokens[i]
        if t in ("-X", "--request"):
            i += 1
            method = tokens[i].upper()
        elif t in ("-H", "--header"):
            i += 1
            raw = tokens[i]
            if ":" not in raw:
                raise CurlParseError(f"Bad header: {raw}")
            k, v = raw.split(":", 1)
            headers[k.strip()] = v.lstrip()
        elif t in ("--data", "--data-raw", "--data-binary", "-d"):
            i += 1
            data = tokens[i]
            # curl с -d обычно превращает запрос в POST, если явно не указан метод
            if method == "GET":
                method = "POST"
        elif t.startswith("http://") or t.startswith("https://"):
            url = t
        elif t == "--compressed":
            # игнорируем
            pass
        i += 1

    if not url:
        # иногда URL последний без схемы — пробуем найти
        for t in tokens:
            if re.match(r"^https?://", t):
                url = t
                break
    if not url:
        raise CurlParseError("URL not found in curl command")

    # Если нашли data в исходной строке — используем её, потому что она точнее.
    if raw_data_token:
        data = raw_data_token
        if method == "GET":
            method = "POST"

    # Нормализуем quotes для data:
    # - $'...' (bash ANSI-C)
    # - '...'
    # - "..."
    if data:
        # иногда после shlex остаётся ведущий '$' перед JSON
        if data.startswith("$[") and data.endswith("]"):
            data = data[1:]

        if data.startswith("$'") and data.endswith("'"):
            inner = data[2:-1]
            data = codecs.decode(inner, "unicode_escape")
        elif (data.startswith("'") and data.endswith("'")) or (data.startswith('"') and data.endswith('"')):
            data = data[1:-1]

    return CurlRequest(method=method, url=url, headers=headers, data=data)


def fetch_json_from_curl(
    curl_command: str,
    *,
    timeout_s: int = 60,
    raise_http: bool = False,
) -> Tuple[CurlRequest, Any]:
    req = parse_curl_bash(curl_command)
    resp = requests.request(req.method, req.url, headers=req.headers, data=req.data, timeout=timeout_s)
    if raise_http:
        resp.raise_for_status()
    if not resp.ok:
        return req, {"_http_status": resp.status_code, "_raw_text": (resp.text or "")[:4000]}
    # RetailCRM web API почти всегда JSON
    try:
        return req, resp.json()
    except Exception:
        return req, {"_raw_text": resp.text[:2000]}


def load_curl_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


