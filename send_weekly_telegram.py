from __future__ import annotations

"""
Отправка сводки в Telegram после weekly-run.

Требует env:
  TELEGRAM_BOT_TOKEN="..."
  TELEGRAM_CHAT_ID="..."   # числовой id или @channelusername
  TELEGRAM_ENABLED="1"     # опционально (по умолчанию 0)

Сводку берём из Google Sheets:
- weekly_behavior_delta_managers
- weekly_digest_managers (если есть)
"""

import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from sheets import open_spreadsheet


def _load_env(env_path: Path) -> Dict[str, str]:
    env: Dict[str, str] = {}
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def _read_table(ws, limit: int = 50) -> List[Dict[str, Any]]:
    values = ws.get_all_values()
    if not values:
        return []
    header = values[0]
    out: List[Dict[str, Any]] = []
    for row in values[1 : 1 + limit]:
        d: Dict[str, Any] = {}
        for i, k in enumerate(header):
            if not k:
                continue
            d[k] = row[i] if i < len(row) else ""
        out.append(d)
    return out


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(str(v))
    except Exception:
        return None


def _fmt_delta(x: Any, *, scale_100: bool = False, suffix: str = "") -> str:
    v = _to_float(x)
    if v is None:
        return ""
    if scale_100:
        v = v * 100.0
        return f"{v:+.1f}%"
    if suffix:
        return f"{v:+.0f}{suffix}"
    return f"{v:+.3f}"


def _send_telegram(token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    last_err: Exception | None = None
    for attempt in range(5):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.5 * (2**attempt))
                continue
            resp.raise_for_status()
            return
        except Exception as e:
            last_err = e
            time.sleep(1.5 * (2**attempt))
    raise RuntimeError(f"Telegram send failed after retries: {last_err}")


def main() -> None:
    base = Path(__file__).resolve().parent
    env = _load_env(base / "env")

    enabled = str(env.get("TELEGRAM_ENABLED", "0")).strip() in ("1", "true", "yes", "on")
    token = str(env.get("TELEGRAM_BOT_TOKEN", "")).strip()
    chat_id = str(env.get("TELEGRAM_CHAT_ID", "")).strip()
    if not enabled:
        print("Telegram: disabled (TELEGRAM_ENABLED != 1). Skipping.")
        return
    if not token or not chat_id:
        print("Telegram: missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID. Skipping.")
        return

    ss = open_spreadsheet(
        spreadsheet_id=env["GOOGLE_SHEETS_ID"],
        service_account_json_path=env["GOOGLE_SERVICE_ACCOUNT_JSON"],
    )

    # weekly deltas
    try:
        ws_delta = ss.worksheet("weekly_behavior_delta_managers")
        delta_rows = _read_table(ws_delta, limit=50)
    except Exception:
        delta_rows = []

    # optional: weekly digest from KPI layer
    try:
        ws_kpi = ss.worksheet("weekly_digest_managers")
        kpi_rows = _read_table(ws_kpi, limit=50)
    except Exception:
        kpi_rows = []

    # Build message
    lines: List[str] = []
    lines.append("RetailCRM — недельная сводка")
    lines.append("")

    if delta_rows:
        # sort: biggest improvement by no_reply_rate (negative is good) then follow_up_gap_rate
        def score(r: Dict[str, Any]) -> float:
            d_no_reply = _to_float(r.get("delta_no_reply_rate")) or 0.0
            d_follow = _to_float(r.get("delta_follow_up_gap_rate")) or 0.0
            d_p90 = _to_float(r.get("delta_p90_first_reply_sec")) or 0.0
            # improvement => negative; weight no_reply/follow > speed
            return (d_no_reply * 100.0) + (d_follow * 100.0) + (d_p90 / 60.0) * 0.2

        rows_sorted = sorted(delta_rows, key=score)
        best = rows_sorted[:3]
        worst = list(reversed(rows_sorted))[:3]

        lines.append("Топ улучшений (по поведению):")
        for r in best:
            name = (r.get("manager_name") or r.get("manager_id") or "").strip()
            lines.append(
                f"- {name}: "
                f"no-reply { _fmt_delta(r.get('delta_no_reply_rate'), scale_100=True) } | "
                f"follow-up gap { _fmt_delta(r.get('delta_follow_up_gap_rate'), scale_100=True) } | "
                f"next step { _fmt_delta(r.get('delta_next_step_rate'), scale_100=True) } | "
                f"p90 { _fmt_delta(r.get('delta_p90_first_reply_sec'), suffix='s') }"
            )
        lines.append("")
        lines.append("Топ ухудшений (обратить внимание):")
        for r in worst:
            name = (r.get("manager_name") or r.get("manager_id") or "").strip()
            lines.append(
                f"- {name}: "
                f"no-reply { _fmt_delta(r.get('delta_no_reply_rate'), scale_100=True) } | "
                f"follow-up gap { _fmt_delta(r.get('delta_follow_up_gap_rate'), scale_100=True) } | "
                f"next step { _fmt_delta(r.get('delta_next_step_rate'), scale_100=True) } | "
                f"p90 { _fmt_delta(r.get('delta_p90_first_reply_sec'), suffix='s') }"
            )
        lines.append("")
    else:
        lines.append("Дельта поведения пока недоступна (нет weekly_behavior_delta_managers).")
        lines.append("")

    if kpi_rows:
        lines.append("KPI (скорость/неответы):")
        for r in kpi_rows[:6]:
            name = (r.get("manager_name") or r.get("manager_id") or "").strip()
            lines.append(
                f"- {name}: no_reply={r.get('no_reply_chats','')} "
                f"(Δ {r.get('delta_no_reply_chats','')}) | "
                f"resp_rate={r.get('response_rate','')} "
                f"(Δ {r.get('delta_response_rate','')})"
            )
        lines.append("")

    # hard limit safety
    text = "\n".join(lines)
    if len(text) > 3800:
        text = text[:3800] + "\n...\n(обрезано)"

    _send_telegram(token, chat_id, text)
    print("OK: Telegram sent")


if __name__ == "__main__":
    main()

