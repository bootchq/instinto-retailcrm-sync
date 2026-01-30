from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from curl_import import CurlParseError, CurlRequest, parse_curl_bash


class WebGraphQLError(RuntimeError):
    pass


@dataclass(frozen=True)
class OperationTemplate:
    operation_name: str
    query: str
    default_variables: Dict[str, Any]


def _load_ops_from_curl_file(curl_file: str) -> Tuple[CurlRequest, Dict[str, OperationTemplate]]:
    curl_text = Path(curl_file).read_text(encoding="utf-8")
    req = parse_curl_bash(curl_text)
    if not req.data:
        raise WebGraphQLError("Curl has no body (--data-raw). Pick a GraphQL request (batch) in DevTools.")
    try:
        payload = json.loads(req.data)
    except Exception as e:
        raise WebGraphQLError(f"Cannot json-decode curl body: {e}")
    if not isinstance(payload, list):
        raise WebGraphQLError("Expected GraphQL batch payload to be a JSON array")

    ops: Dict[str, OperationTemplate] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue
        name = item.get("operationName")
        query = item.get("query")
        variables = item.get("variables") or {}
        if not name or not query:
            continue
        ops[str(name)] = OperationTemplate(operation_name=str(name), query=str(query), default_variables=dict(variables))
    if not ops:
        raise WebGraphQLError("No operations found in batch payload")
    return req, ops


class WebGraphQLClient:
    """
    Клиент для RetailCRM web GraphQL (mg-s*.retailcrm.pro/api/graphql/v1/batch).
    """

    def __init__(self, *, curl_file: str, timeout_s: int = 180, max_retries: int = 5) -> None:
        self.curl_file = curl_file
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self._base_req, self.ops = _load_ops_from_curl_file(curl_file)

    def has_op(self, operation_name: str) -> bool:
        return operation_name in self.ops

    def request_batch(self, ops: List[Dict[str, Any]]) -> Any:
        headers = dict(self._base_req.headers)
        # Важно: content-type должен быть json
        headers.setdefault("content-type", "application/json")

        last_err: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                resp = requests.request(
                    self._base_req.method,
                    self._base_req.url,
                    headers=headers,
                    data=json.dumps(ops, ensure_ascii=False),
                    timeout=self.timeout_s,
                )
                # ретраи на 429/5xx
                if resp.status_code in (429, 500, 502, 503, 504) and attempt < self.max_retries:
                    time.sleep(1.0 * (2**attempt))
                    continue
                if not resp.ok:
                    raise WebGraphQLError(f"HTTP {resp.status_code}: {resp.text[:1000]}")
                return resp.json()
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                last_err = e
                if attempt >= self.max_retries:
                    break
                time.sleep(1.0 * (2**attempt))
            except Exception as e:
                last_err = e
                break

        raise WebGraphQLError(f"Request failed after retries: {last_err}")

    def build_op(self, operation_name: str, *, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if operation_name not in self.ops:
            raise WebGraphQLError(f"Operation not found in curl batch: {operation_name}. Available: {sorted(self.ops.keys())}")
        t = self.ops[operation_name]
        v = dict(t.default_variables)
        if variables:
            v.update(variables)
        return {"operationName": t.operation_name, "variables": v, "query": t.query}


