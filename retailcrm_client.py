from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional
from urllib.parse import urljoin

import requests

from retailcrm_endpoints import DEFAULT_ENDPOINTS, RetailCrmEndpoints


class RetailCrmError(RuntimeError):
    pass


def _clean_base_url(url: str) -> str:
    url = url.strip()
    if not url:
        raise ValueError("RETAILCRM_URL is empty")
    return url.rstrip("/") + "/"


@dataclass
class RetailCrmClient:
    base_url: str
    api_key: str
    endpoints: RetailCrmEndpoints = DEFAULT_ENDPOINTS
    timeout_s: int = 60
    max_retries: int = 4

    @classmethod
    def from_env(cls) -> "RetailCrmClient":
        return cls(
            base_url=_clean_base_url(os.environ["RETAILCRM_URL"]),
            api_key=os.environ["RETAILCRM_API_KEY"].strip(),
        )

    def _request(self, method: str, path: str, *, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = urljoin(self.base_url, path.lstrip("/"))
        params = dict(params or {})
        # RetailCRM чаще всего принимает apiKey как query param
        params.setdefault("apiKey", self.api_key)

        last_err: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                resp = requests.request(method, url, params=params, timeout=self.timeout_s)
                # 429/5xx — попробуем повторить
                if resp.status_code in (429, 500, 502, 503, 504) and attempt < self.max_retries:
                    time.sleep(1.0 * (2**attempt))
                    continue
                if not resp.ok:
                    raise RetailCrmError(f"HTTP {resp.status_code} on {url}: {resp.text[:500]}")
                data = resp.json()
                if isinstance(data, dict) and data.get("success") is False:
                    raise RetailCrmError(f"RetailCRM error on {url}: {data}")
                return data
            except Exception as e:
                last_err = e
                if attempt >= self.max_retries:
                    break
                time.sleep(0.8 * (2**attempt))
        raise RetailCrmError(f"Request failed after retries: {last_err}")

    def probe_get(self, path: str, *, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Лёгкий "пробник" для эндпоинтов: возвращает status_code и кусок текста ответа,
        не бросая исключение на 4xx/5xx.
        """
        url = urljoin(self.base_url, path.lstrip("/"))
        params = dict(params or {})
        params.setdefault("apiKey", self.api_key)
        try:
            resp = requests.get(url, params=params, timeout=self.timeout_s)
            text = resp.text or ""
            return {
                "url": url,
                "status": resp.status_code,
                "text_head": text[:800],
            }
        except Exception as e:
            return {
                "url": url,
                "status": None,
                "error": str(e),
            }

    def get_users(self) -> Dict[int, Dict[str, Any]]:
        """
        Возвращает словарь userId -> user.
        """
        # RetailCRM валидирует limit строго (обычно 20/50/100)
        limit = 100
        page = 1
        out: Dict[int, Dict[str, Any]] = {}

        while True:
            data = self._request("GET", self.endpoints.users, params={"limit": limit, "page": page})
            # формат может быть users: [...]
            users = data.get("users") or data.get("data") or []
            if not users:
                return out
            for u in users:
                uid = u.get("id") or u.get("userId")
                if uid is None:
                    continue
                out[int(uid)] = u

            pagination = data.get("pagination") or {}
            total_page_count = pagination.get("totalPageCount") or pagination.get("total_pages")
            if total_page_count and page >= int(total_page_count):
                return out
            if not total_page_count:
                # если пагинации нет — прекращаем, чтобы не зациклиться
                return out
            page += 1

    def iter_chats(self, *, start: str, end: str, limit: int = 100) -> Iterable[Dict[str, Any]]:
        """
        Итератор чатов за период.

        Параметры фильтра зависят от реализации API чатов в вашем аккаунте.
        Дефолтно пробуем универсальные поля createdAt/updatedAt.
        """
        page = 1
        while True:
            params = {
                "limit": limit,
                "page": page,
                # Часто встречается фильтр по периоду
                "startDate": start,
                "endDate": end,
            }
            data = self._request("GET", self.endpoints.chats, params=params)
            chats = data.get("chats") or data.get("data") or []
            if not chats:
                return
            for c in chats:
                yield c

            pagination = data.get("pagination") or {}
            total_page_count = pagination.get("totalPageCount") or pagination.get("total_pages")
            if total_page_count and page >= int(total_page_count):
                return
            # если пагинации нет — прекращаем, чтобы не зациклиться
            if not total_page_count:
                return
            page += 1

    def iter_chat_messages(self, chat_id: Any, *, limit: int = 200) -> Iterable[Dict[str, Any]]:
        page = 1
        while True:
            path = self.endpoints.chat_messages.format(chat_id=chat_id)
            data = self._request("GET", path, params={"limit": limit, "page": page})
            messages = data.get("messages") or data.get("data") or []
            if not messages:
                return
            for m in messages:
                yield m

            pagination = data.get("pagination") or {}
            total_page_count = pagination.get("totalPageCount") or pagination.get("total_pages")
            if total_page_count and page >= int(total_page_count):
                return
            if not total_page_count:
                return
            page += 1


