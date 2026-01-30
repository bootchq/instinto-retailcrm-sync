from __future__ import annotations

"""
Weekly digest:
- читает текущие вкладки manager_summary / channel_summary (после export_to_sheets.py)
- добавляет снепшот в history_* (append)
- строит недельную дельту (текущий снепшот vs последний снепшот >= 6 дней назад)
- пишет результат в weekly_digest

Без отправки сообщений: итог — в Google Sheets.
"""

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dateutil import parser as dtparser

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


def _now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


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


def _parse_dt(v: Any) -> Optional[datetime]:
    if not v:
        return None
    try:
        return dtparser.isoparse(str(v))
    except Exception:
        return None


def _read_table(ws) -> List[Dict[str, Any]]:
    values = ws.get_all_values()
    if not values:
        return []
    header = values[0]
    out: List[Dict[str, Any]] = []
    for row in values[1:]:
        d = {}
        for i, k in enumerate(header):
            if not k:
                continue
            d[k] = row[i] if i < len(row) else ""
        out.append(d)
    return out


def _ensure_ws(ss, title: str):
    import gspread

    try:
        return ss.worksheet(title)
    except gspread.WorksheetNotFound:
        return ss.add_worksheet(title=title, rows=200, cols=40)


def _append_rows(ws, header: List[str], rows: List[List[Any]]) -> None:
    # ensure header exists
    existing = ws.row_values(1)
    if existing != header:
        ws.clear()
        ws.update(values=[header], range_name="A1")
    if not rows:
        return
    ws.append_rows(rows, value_input_option="RAW")


def main() -> None:
    base = Path(__file__).resolve().parent
    env = _load_env(base / "env")

    ss = open_spreadsheet(
        spreadsheet_id=env["GOOGLE_SHEETS_ID"],
        service_account_json_path=env["GOOGLE_SERVICE_ACCOUNT_JSON"],
    )

    run_ts = _now_iso()

    # Read current summaries
    mgr_ws = ss.worksheet("manager_summary")
    ch_ws = ss.worksheet("channel_summary")

    mgr_rows = _read_table(mgr_ws)
    ch_rows = _read_table(ch_ws)

    # Write snapshots
    hist_mgr = _ensure_ws(ss, "history_manager_summary")
    hist_ch = _ensure_ws(ss, "history_channel_summary")

    mgr_header = ["run_ts"] + (list(mgr_rows[0].keys()) if mgr_rows else [])
    ch_header = ["run_ts"] + (list(ch_rows[0].keys()) if ch_rows else [])

    _append_rows(
        hist_mgr,
        mgr_header,
        [[run_ts] + [r.get(k, "") for k in mgr_header[1:]] for r in mgr_rows],
    )
    _append_rows(
        hist_ch,
        ch_header,
        [[run_ts] + [r.get(k, "") for k in ch_header[1:]] for r in ch_rows],
    )

    # Build deltas vs last snapshot ~1 week ago
    all_hist_mgr = _read_table(hist_mgr)
    all_hist_ch = _read_table(hist_ch)

    cutoff = _parse_dt(run_ts) - timedelta(days=6)

    def pick_baseline(rows: List[Dict[str, Any]], key_fields: List[str]) -> Dict[Tuple[str, ...], Dict[str, Any]]:
        # pick last row before cutoff per key
        best: Dict[Tuple[str, ...], Tuple[datetime, Dict[str, Any]]] = {}
        for r in rows:
            ts = _parse_dt(r.get("run_ts"))
            if not ts or ts > cutoff:
                continue
            key = tuple(str(r.get(k, "") or "") for k in key_fields)
            cur = best.get(key)
            if cur is None or ts > cur[0]:
                best[key] = (ts, r)
        return {k: v[1] for k, v in best.items()}

    mgr_baseline = pick_baseline(all_hist_mgr, ["manager_id", "manager_name"])
    ch_baseline = pick_baseline(all_hist_ch, ["channel"])

    digest_rows: List[Dict[str, Any]] = []
    for r in mgr_rows:
        key = (str(r.get("manager_id", "") or ""), str(r.get("manager_name", "") or ""))
        b = mgr_baseline.get(key)

        cur_no_reply = _to_int(r.get("no_reply_chats"))
        base_no_reply = _to_int(b.get("no_reply_chats")) if b else None
        d_no_reply = (cur_no_reply - base_no_reply) if (cur_no_reply is not None and base_no_reply is not None) else ""

        cur_resp_rate = _to_float(r.get("response_rate"))
        base_resp_rate = _to_float(b.get("response_rate")) if b else None
        d_resp_rate = (cur_resp_rate - base_resp_rate) if (cur_resp_rate is not None and base_resp_rate is not None) else ""

        cur_p90 = _to_int(r.get("p90_first_reply_sec"))
        base_p90 = _to_int(b.get("p90_first_reply_sec")) if b else None
        d_p90 = (cur_p90 - base_p90) if (cur_p90 is not None and base_p90 is not None) else ""

        digest_rows.append(
            {
                "run_ts": run_ts,
                "manager_id": r.get("manager_id", ""),
                "manager_name": r.get("manager_name", ""),
                "chats": r.get("chats", ""),
                "no_reply_chats": r.get("no_reply_chats", ""),
                "delta_no_reply_chats": d_no_reply,
                "response_rate": r.get("response_rate", ""),
                "delta_response_rate": d_resp_rate,
                "p90_first_reply_sec": r.get("p90_first_reply_sec", ""),
                "delta_p90_first_reply_sec": d_p90,
            }
        )

    channel_digest_rows: List[Dict[str, Any]] = []
    for r in ch_rows:
        key = (str(r.get("channel", "") or ""),)
        b = ch_baseline.get(key)

        cur_no_reply = _to_int(r.get("no_reply_chats"))
        base_no_reply = _to_int(b.get("no_reply_chats")) if b else None
        d_no_reply = (cur_no_reply - base_no_reply) if (cur_no_reply is not None and base_no_reply is not None) else ""

        cur_resp_rate = _to_float(r.get("response_rate"))
        base_resp_rate = _to_float(b.get("response_rate")) if b else None
        d_resp_rate = (cur_resp_rate - base_resp_rate) if (cur_resp_rate is not None and base_resp_rate is not None) else ""

        channel_digest_rows.append(
            {
                "run_ts": run_ts,
                "channel": r.get("channel", ""),
                "chats": r.get("chats", ""),
                "no_reply_chats": r.get("no_reply_chats", ""),
                "delta_no_reply_chats": d_no_reply,
                "response_rate": r.get("response_rate", ""),
                "delta_response_rate": d_resp_rate,
            }
        )

    # write digest sheets
    upsert_worksheet(
        ss,
        "weekly_digest_managers",
        rows=dicts_to_table(
            digest_rows,
            header=[
                "run_ts",
                "manager_id",
                "manager_name",
                "chats",
                "no_reply_chats",
                "delta_no_reply_chats",
                "response_rate",
                "delta_response_rate",
                "p90_first_reply_sec",
                "delta_p90_first_reply_sec",
            ],
        ),
    )
    upsert_worksheet(
        ss,
        "weekly_digest_channels",
        rows=dicts_to_table(
            channel_digest_rows,
            header=[
                "run_ts",
                "channel",
                "chats",
                "no_reply_chats",
                "delta_no_reply_chats",
                "response_rate",
                "delta_response_rate",
            ],
        ),
    )

    # small local note for debugging (optional)
    (base / "weekly_digest_last_run.json").write_text(
        json.dumps({"run_ts": run_ts, "managers": len(digest_rows), "channels": len(channel_digest_rows)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("OK: weekly digest written to Google Sheets (weekly_digest_managers / weekly_digest_channels)")


if __name__ == "__main__":
    main()

