from __future__ import annotations

"""
Строит понятный отчёт по менеджерам на основе вкладок Google Sheets:
- manager_summary
- channel_summary

Пишет результат в вкладку `manager_report`.

Запуск:
  source .venv/bin/activate
  python manager_report.py
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from sheets import dicts_to_table, open_spreadsheet, upsert_worksheet


def _load_env(env_path: Path) -> Dict[str, str]:
    env: Dict[str, str] = {}
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def _to_int(v: Any) -> Optional[int]:
    try:
        if v is None or v == "":
            return None
        return int(float(str(v)))
    except Exception:
        return None


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(str(v))
    except Exception:
        return None


def _safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b == 0:
        return None
    return a / b


def _read_table(ws) -> List[Dict[str, Any]]:
    values = ws.get_all_values()
    if not values:
        return []
    header = values[0]
    out: List[Dict[str, Any]] = []
    for row in values[1:]:
        d: Dict[str, Any] = {}
        for i, k in enumerate(header):
            if not k:
                continue
            d[k] = row[i] if i < len(row) else ""
        out.append(d)
    return out


def _sec_to_min(sec: Optional[int]) -> str:
    if sec is None:
        return ""
    return f"{sec/60:.1f}"


def main() -> None:
    base = Path(__file__).resolve().parent
    env = _load_env(base / "env")

    ss = open_spreadsheet(
        spreadsheet_id=env["GOOGLE_SHEETS_ID"],
        service_account_json_path=env["GOOGLE_SERVICE_ACCOUNT_JSON"],
    )

    mgr_rows = _read_table(ss.worksheet("manager_summary"))
    ch_rows = _read_table(ss.worksheet("channel_summary"))

    # channel overview (top-level)
    channel_overview: List[Dict[str, Any]] = []
    for r in ch_rows:
        chats = _to_int(r.get("chats"))
        no_reply = _to_int(r.get("no_reply_chats"))
        resp_rate = _to_float(r.get("response_rate"))
        p90 = _to_int(r.get("p90_first_reply_sec"))
        channel_overview.append(
            {
                "type": "channel",
                "name": r.get("channel", ""),
                "chats": chats or "",
                "no_reply_chats": no_reply or "",
                "no_reply_rate": f"{(no_reply / chats * 100):.1f}%" if (no_reply is not None and chats) else "",
                "response_rate": f"{(resp_rate*100):.1f}%" if resp_rate is not None else "",
                "p90_first_reply_min": _sec_to_min(p90),
                "focus": "Дисциплина ответов / follow-up" if (no_reply is not None and chats and no_reply / chats > 0.15) else "",
            }
        )

    # manager overview with coaching focus
    manager_overview: List[Dict[str, Any]] = []
    for r in mgr_rows:
        mid = str(r.get("manager_id", "") or "")
        mname = str(r.get("manager_name", "") or "")
        chats = _to_int(r.get("chats"))
        inbound = _to_int(r.get("inbound"))
        outbound = _to_int(r.get("outbound"))
        no_reply = _to_int(r.get("no_reply_chats"))
        unanswered = _to_int(r.get("unanswered_inbound"))
        p90 = _to_int(r.get("p90_first_reply_sec"))
        median = _to_int(r.get("median_first_reply_sec"))

        no_reply_rate = (no_reply / chats) if (no_reply is not None and chats) else None
        unanswered_rate = (unanswered / inbound) if (unanswered is not None and inbound) else None
        out_in_ratio = (outbound / inbound) if (outbound is not None and inbound and inbound > 0) else None

        focus_parts: List[str] = []
        if no_reply_rate is not None and no_reply_rate > 0.12:
            focus_parts.append("не отвеченные/потери")
        if p90 is not None and p90 > 20 * 60:
            focus_parts.append("скорость ответа (p90)")
        if unanswered_rate is not None and unanswered_rate > 0.25:
            focus_parts.append("follow-up")
        if out_in_ratio is not None and out_in_ratio < 0.8:
            focus_parts.append("закрытие/next step")

        manager_overview.append(
            {
                "type": "manager",
                "manager_id": mid,
                "manager_name": mname,
                "chats": chats or "",
                "inbound": inbound or "",
                "outbound": outbound or "",
                "no_reply_chats": no_reply or "",
                "no_reply_rate": f"{(no_reply_rate*100):.1f}%" if no_reply_rate is not None else "",
                "unanswered_inbound": unanswered or "",
                "unanswered_rate": f"{(unanswered_rate*100):.1f}%" if unanswered_rate is not None else "",
                "median_first_reply_min": _sec_to_min(median),
                "p90_first_reply_min": _sec_to_min(p90),
                "out_in_ratio": f"{out_in_ratio:.2f}" if out_in_ratio is not None else "",
                "coaching_focus": ", ".join(focus_parts),
            }
        )

    # Sort managers: highest no_reply_rate then highest p90
    def _sort_key(x: Dict[str, Any]) -> tuple:
        nr = x.get("no_reply_rate", "")
        try:
            nr_f = float(str(nr).replace("%", "")) if nr else -1.0
        except Exception:
            nr_f = -1.0
        try:
            p90_f = float(x.get("p90_first_reply_min") or -1.0)
        except Exception:
            p90_f = -1.0
        return (-nr_f, -p90_f)

    manager_overview_sorted = sorted(manager_overview, key=_sort_key)

    # Write to sheet
    rows: List[Dict[str, Any]] = []
    rows.append({"type": "meta", "manager_id": "", "manager_name": "CHANNELS (overview) - сверху; MANAGERS - ниже"})
    rows.extend(channel_overview)
    rows.append({"type": "meta", "manager_id": "", "manager_name": "MANAGERS (sorted by no-reply rate, then p90)"})
    rows.extend(manager_overview_sorted)

    upsert_worksheet(
        ss,
        "manager_report",
        rows=dicts_to_table(
            rows,
            header=[
                "type",
                "name",
                "manager_id",
                "manager_name",
                "chats",
                "inbound",
                "outbound",
                "no_reply_chats",
                "no_reply_rate",
                "unanswered_inbound",
                "unanswered_rate",
                "median_first_reply_min",
                "p90_first_reply_min",
                "out_in_ratio",
                "response_rate",
                "focus",
                "coaching_focus",
            ],
        ),
    )

    print("OK: wrote manager_report")


if __name__ == "__main__":
    main()

