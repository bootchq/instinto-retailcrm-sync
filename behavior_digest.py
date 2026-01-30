from __future__ import annotations

"""
Behavior digest (weekly-ready):

Читает `messages_raw` + `chats_raw` из Google Sheets и считает поведенческие метрики по менеджерам:
- next_step_rate: доля чатов, где последний исходящий содержит вопрос/следующий шаг
- questions_per_chat: среднее число вопросов от менеджера
- spin_rate (упрощённо): доля чатов с >=2 вопросами и ключевыми "ситуационными" словами
- upsell_rate: доля чатов, где менеджер предлагает доп. товар/комплект/акцию
- follow_up_gap_rate: доля чатов, где клиент писал, но менеджер не вернулся в течение 24ч (признак слабого follow-up)

Также пишет примеры (3–5 на менеджера) в `weekly_examples`:
категории: no_reply, slow_reply, no_next_step_high_intent, good

Выход в Google Sheets:
- `behavior_snapshot_managers` (текущие метрики)
- `history_behavior_managers` (append снепшоты с run_ts)
- `weekly_behavior_delta_managers` (дельта vs последний снепшот >= 6 дней назад)
- `weekly_examples` (примеры с короткими редактированными фрагментами)
"""

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

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


def _parse_dt(v: Any) -> Optional[datetime]:
    if not v:
        return None
    try:
        return dtparser.isoparse(str(v))
    except Exception:
        return None


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


_RE_PHONE = re.compile(r"\b\+?\d[\d\s\-()]{7,}\b")
_RE_LONG_DIGITS = re.compile(r"\b\d{5,}\b")
_RE_EMAIL = re.compile(r"\b[\w.\-+]+@[\w.\-]+\.\w+\b", re.IGNORECASE)
_RE_URL = re.compile(r"https?://\S+", re.IGNORECASE)


def _redact_text(s: Any, max_len: int = 220) -> str:
    s = "" if s is None else str(s)
    s = _RE_URL.sub("[link]", s)
    s = _RE_EMAIL.sub("[email]", s)
    s = _RE_LONG_DIGITS.sub("***", s)
    s = _RE_PHONE.sub("***", s)
    s = s.replace("\n", " ").strip()
    return s[:max_len]


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


@dataclass(frozen=True)
class Msg:
    chat_id: str
    sent_at: Optional[datetime]
    direction: str  # in/out
    manager_id: str
    message_type: str
    author_type: str
    text: str


def _is_manager_msg(m: Msg) -> bool:
    return m.direction == "out"


def _is_customer_msg(m: Msg) -> bool:
    return m.direction == "in" and (m.author_type or "").lower() == "customer"


def _is_textish(m: Msg) -> bool:
    t = (m.message_type or "").upper()
    return t in ("TEXT", "COMMAND", "ORDER", "PRODUCT", "FILE", "AUDIO", "IMAGE", "")


_KW_INTENT = re.compile(r"\b(цена|стоимост|сколько|налич|размер|доставк|оплат|адрес|заказ|оформ|хочу|купить)\b", re.IGNORECASE)
_KW_UPSELL = re.compile(r"\b(в комплект|набор|дополнит|к этому|ещ[её] можно|рекомендую|возьм(ите|и)|акци|скидк|подарок)\b", re.IGNORECASE)
_KW_NEXTSTEP = re.compile(
    r"\b(оформим|оформляю|заказ|доставка|адрес|самовывоз|оплат|ссылк|подтвердите|куда отправить|какой размер|какая модель)\b|\?",
    re.IGNORECASE,
)
_KW_SPIN_SITUATION = re.compile(r"\b(какой|какая|какие|сколько|когда|куда|размер|рост|вес|предпочт|для кого)\b", re.IGNORECASE)


def _count_questions(text: str) -> int:
    if not text:
        return 0
    # '?' + "вопросительные слова" в начале фразы
    q = text.count("?")
    if re.search(r"(^|\s)(что|как|какой|какая|какие|сколько|когда|куда|зачем|почему)\b", text, re.IGNORECASE):
        q += 1
    return q


def _chat_snippet(msgs: List[Msg]) -> Tuple[str, str]:
    """
    Возвращает (snippet_in, snippet_out): по 1–2 первых осмысленных сообщения клиента/менеджера.
    """
    inbound = ""
    outbound = ""
    for m in msgs:
        if not inbound and _is_customer_msg(m) and _is_textish(m) and m.text:
            inbound = _redact_text(m.text)
        if not outbound and _is_manager_msg(m) and _is_textish(m) and m.text:
            outbound = _redact_text(m.text)
        if inbound and outbound:
            break
    return inbound, outbound


def main() -> None:
    base = Path(__file__).resolve().parent
    env = _load_env(base / "env")
    ss = open_spreadsheet(
        spreadsheet_id=env["GOOGLE_SHEETS_ID"],
        service_account_json_path=env["GOOGLE_SERVICE_ACCOUNT_JSON"],
    )

    run_ts = _now_iso()

    chats = _read_table(ss.worksheet("chats_raw"))
    msgs_rows = _read_table(ss.worksheet("messages_raw"))

    # Build manager map from chats_raw
    mgr_name_by_id: Dict[str, str] = {}
    chat_mgr_id: Dict[str, str] = {}
    chat_mgr_name: Dict[str, str] = {}
    chat_first_reply_sec: Dict[str, Optional[int]] = {}
    chat_no_reply: Dict[str, Optional[int]] = {}
    chat_unanswered: Dict[str, Optional[int]] = {}
    for r in chats:
        cid = str(r.get("chat_id", "") or "")
        mid = str(r.get("manager_id", "") or "")
        mname = str(r.get("manager_name", "") or "")
        if cid:
            chat_mgr_id[cid] = mid
            chat_mgr_name[cid] = mname
            chat_first_reply_sec[cid] = _to_int(r.get("first_response_sec"))
            chat_unanswered[cid] = _to_int(r.get("unanswered_inbound"))
            # no_reply как индикатор: нет исходящих
            inb = _to_int(r.get("inbound_count")) or 0
            outb = _to_int(r.get("outbound_count")) or 0
            chat_no_reply[cid] = 1 if (inb > 0 and outb == 0) else 0
        if mid and mname:
            mgr_name_by_id[mid] = mname

    # Group messages by chat_id
    by_chat: Dict[str, List[Msg]] = {}
    for r in msgs_rows:
        cid = str(r.get("chat_id", "") or "")
        if not cid:
            continue
        m = Msg(
            chat_id=cid,
            sent_at=_parse_dt(r.get("sent_at")),
            direction=str(r.get("direction", "") or ""),
            manager_id=str(r.get("manager_id", "") or ""),
            message_type=str(r.get("message_type", "") or ""),
            author_type=str(r.get("author_type", "") or ""),
            text=str(r.get("text", "") or ""),
        )
        by_chat.setdefault(cid, []).append(m)

    # Sort per chat
    for cid, ms in by_chat.items():
        ms.sort(key=lambda x: x.sent_at or datetime.min)

    # Per manager aggregates
    agg: Dict[str, Dict[str, Any]] = {}

    # Examples accumulator
    examples: List[Dict[str, Any]] = []

    def ensure(mid: str, name: str) -> Dict[str, Any]:
        if mid not in agg:
            agg[mid] = {
                "manager_id": mid,
                "manager_name": name,
                "chats": 0,
                "responded_chats": 0,
                "no_reply_chats": 0,
                "median_first_reply_sec": "",
                "p90_first_reply_sec": "",
                "response_rate": "",
                "no_reply_rate": "",
                "avg_questions_per_chat": 0.0,
                "next_step_rate": "",
                "spin_rate": "",
                "upsell_rate": "",
                "follow_up_gap_rate": "",
                "high_intent_chats": 0,
            }
        return agg[mid]

    first_reply_values_by_mgr: Dict[str, List[int]] = {}

    # For picking examples: collect candidate scores
    cand_no_reply: Dict[str, List[Tuple[str, int, str, str]]] = {}
    cand_slow: Dict[str, List[Tuple[str, int, str, str]]] = {}
    cand_no_next: Dict[str, List[Tuple[str, int, str, str]]] = {}
    cand_good: Dict[str, List[Tuple[str, int, str, str]]] = {}

    for cid, ms in by_chat.items():
        mid = chat_mgr_id.get(cid) or ""
        mname = chat_mgr_name.get(cid) or mgr_name_by_id.get(mid, "") or ""
        if not mid:
            # fallback: first outbound manager_id
            for m in ms:
                if _is_manager_msg(m) and m.manager_id:
                    mid = m.manager_id
                    break
        if not mname and mid:
            mname = mgr_name_by_id.get(mid, "")

        if not mid and not mname:
            mid = ""
            mname = "(unassigned)"

        a = ensure(mid, mname)
        a["chats"] += 1

        # responded / no-reply from chats_raw quick signal
        no_reply_flag = chat_no_reply.get(cid)
        if no_reply_flag:
            a["no_reply_chats"] += 1
        responded = any(_is_manager_msg(m) and _is_textish(m) for m in ms)
        if responded:
            a["responded_chats"] += 1

        fr = chat_first_reply_sec.get(cid)
        if fr is not None and fr > 0:
            first_reply_values_by_mgr.setdefault(mid, []).append(fr)

        # behavior features
        manager_texts = [m.text for m in ms if _is_manager_msg(m) and _is_textish(m) and m.text]
        customer_texts = [m.text for m in ms if _is_customer_msg(m) and _is_textish(m) and m.text]

        questions = sum(_count_questions(t) for t in manager_texts)
        # track later as avg
        a.setdefault("_questions_sum", 0)
        a.setdefault("_next_step_hits", 0)
        a.setdefault("_spin_hits", 0)
        a.setdefault("_upsell_hits", 0)
        a.setdefault("_follow_gap_hits", 0)
        a.setdefault("_high_intent", 0)

        a["_questions_sum"] += questions

        last_out = ""
        for m in reversed(ms):
            if _is_manager_msg(m) and _is_textish(m) and m.text:
                last_out = m.text
                break
        next_step = bool(last_out and _KW_NEXTSTEP.search(last_out))
        if next_step:
            a["_next_step_hits"] += 1

        spin = bool(questions >= 2 and any(_KW_SPIN_SITUATION.search(t) for t in manager_texts))
        if spin:
            a["_spin_hits"] += 1

        upsell = any(_KW_UPSELL.search(t) for t in manager_texts)
        if upsell:
            a["_upsell_hits"] += 1

        # high intent: customer asks price/availability/delivery
        high_intent = any(_KW_INTENT.search(t) for t in customer_texts)
        if high_intent:
            a["_high_intent"] += 1

        # follow-up gap: last customer msg exists and last manager msg is far before it (no return within 24h)
        last_cust_dt = None
        last_mgr_dt = None
        for m in reversed(ms):
            if last_cust_dt is None and _is_customer_msg(m) and m.sent_at:
                last_cust_dt = m.sent_at
            if last_mgr_dt is None and _is_manager_msg(m) and m.sent_at and _is_textish(m):
                last_mgr_dt = m.sent_at
            if last_cust_dt and last_mgr_dt:
                break
        follow_gap = False
        if last_cust_dt:
            # find manager msg after last_cust_dt
            mgr_after = any(_is_manager_msg(m) and m.sent_at and m.sent_at > last_cust_dt for m in ms)
            if not mgr_after:
                follow_gap = True
        if follow_gap:
            a["_follow_gap_hits"] += 1

        # Examples candidates
        sn_in, sn_out = _chat_snippet(ms)
        # For sorting candidates: use "severity" score
        if no_reply_flag:
            cand_no_reply.setdefault(mid, []).append((cid, 100, sn_in, sn_out))
        if fr is not None and fr > 0:
            # slow: > 30 min
            if fr >= 30 * 60:
                cand_slow.setdefault(mid, []).append((cid, fr, sn_in, sn_out))
        if high_intent and responded and not next_step:
            cand_no_next.setdefault(mid, []).append((cid, 50, sn_in, sn_out))
        if responded and next_step and questions >= 1 and fr is not None and fr > 0 and fr <= 10 * 60:
            cand_good.setdefault(mid, []).append((cid, 10 * 60 - fr, sn_in, sn_out))

    # finalize aggregates
    out_rows: List[Dict[str, Any]] = []
    for mid, a in agg.items():
        chats_n = int(a["chats"])
        responded_n = int(a["responded_chats"])
        no_reply_n = int(a["no_reply_chats"])

        frs = sorted(first_reply_values_by_mgr.get(mid, []))
        median = frs[len(frs) // 2] if frs else None
        p90 = frs[int(len(frs) * 0.9)] if frs else None

        q_avg = (a.get("_questions_sum", 0) / chats_n) if chats_n else 0.0

        next_rate = (a.get("_next_step_hits", 0) / chats_n) if chats_n else None
        spin_rate = (a.get("_spin_hits", 0) / chats_n) if chats_n else None
        upsell_rate = (a.get("_upsell_hits", 0) / chats_n) if chats_n else None
        follow_rate = (a.get("_follow_gap_hits", 0) / chats_n) if chats_n else None

        resp_rate = (responded_n / chats_n) if chats_n else None
        no_reply_rate = (no_reply_n / chats_n) if chats_n else None

        out_rows.append(
            {
                "run_ts": run_ts,
                "manager_id": mid,
                "manager_name": a.get("manager_name", ""),
                "chats": chats_n,
                "responded_chats": responded_n,
                "response_rate": round(resp_rate, 4) if resp_rate is not None else "",
                "no_reply_chats": no_reply_n,
                "no_reply_rate": round(no_reply_rate, 4) if no_reply_rate is not None else "",
                "median_first_reply_sec": median if median is not None else "",
                "p90_first_reply_sec": p90 if p90 is not None else "",
                "avg_questions_per_chat": round(q_avg, 3),
                "next_step_rate": round(next_rate, 4) if next_rate is not None else "",
                "spin_rate": round(spin_rate, 4) if spin_rate is not None else "",
                "upsell_rate": round(upsell_rate, 4) if upsell_rate is not None else "",
                "follow_up_gap_rate": round(follow_rate, 4) if follow_rate is not None else "",
                "high_intent_chats": int(a.get("_high_intent", 0)),
            }
        )

    # Write snapshot
    header = [
        "run_ts",
        "manager_id",
        "manager_name",
        "chats",
        "responded_chats",
        "response_rate",
        "no_reply_chats",
        "no_reply_rate",
        "median_first_reply_sec",
        "p90_first_reply_sec",
        "avg_questions_per_chat",
        "next_step_rate",
        "spin_rate",
        "upsell_rate",
        "follow_up_gap_rate",
        "high_intent_chats",
    ]
    upsert_worksheet(ss, "behavior_snapshot_managers", rows=dicts_to_table(out_rows, header=header))

    # Append history
    import gspread

    try:
        hist_ws = ss.worksheet("history_behavior_managers")
    except gspread.WorksheetNotFound:
        hist_ws = ss.add_worksheet(title="history_behavior_managers", rows=200, cols=60)
        hist_ws.update(values=[header], range_name="A1")

    # Ensure header
    existing = hist_ws.row_values(1)
    if existing != header:
        hist_ws.clear()
        hist_ws.update(values=[header], range_name="A1")
    if out_rows:
        hist_ws.append_rows([[r.get(k, "") for k in header] for r in out_rows], value_input_option="RAW")

    # Weekly delta vs last snapshot >= 6 days ago
    hist_rows = _read_table(hist_ws)
    cutoff = _parse_dt(run_ts) - timedelta(days=6)

    baseline: Dict[str, Dict[str, Any]] = {}
    best_ts: Dict[str, datetime] = {}
    for r in hist_rows:
        ts = _parse_dt(r.get("run_ts"))
        if not ts or ts > cutoff:
            continue
        mid = str(r.get("manager_id", "") or "")
        if mid not in best_ts or ts > best_ts[mid]:
            best_ts[mid] = ts
            baseline[mid] = r

    delta_rows: List[Dict[str, Any]] = []
    for r in out_rows:
        mid = str(r.get("manager_id", "") or "")
        b = baseline.get(mid)
        row = dict(r)
        for k in (
            "response_rate",
            "no_reply_rate",
            "avg_questions_per_chat",
            "next_step_rate",
            "spin_rate",
            "upsell_rate",
            "follow_up_gap_rate",
            "median_first_reply_sec",
            "p90_first_reply_sec",
        ):
            cur = _to_float(row.get(k)) if "sec" not in k else _to_float(row.get(k))
            base_val = _to_float(b.get(k)) if b else None
            if cur is None or base_val is None:
                row[f"delta_{k}"] = ""
            else:
                row[f"delta_{k}"] = round(cur - base_val, 4)
        delta_rows.append(row)

    delta_header = header + [
        "delta_response_rate",
        "delta_no_reply_rate",
        "delta_avg_questions_per_chat",
        "delta_next_step_rate",
        "delta_spin_rate",
        "delta_upsell_rate",
        "delta_follow_up_gap_rate",
        "delta_median_first_reply_sec",
        "delta_p90_first_reply_sec",
    ]
    upsert_worksheet(ss, "weekly_behavior_delta_managers", rows=dicts_to_table(delta_rows, header=delta_header))

    # Build weekly examples (top 3 each category per manager)
    def pick_top(cands: Dict[str, List[Tuple[str, int, str, str]]], reverse: bool = True) -> Dict[str, List[Tuple[str, int, str, str]]]:
        out: Dict[str, List[Tuple[str, int, str, str]]] = {}
        for mid, items in cands.items():
            items_sorted = sorted(items, key=lambda x: x[1], reverse=reverse)[:3]
            out[mid] = items_sorted
        return out

    top_no_reply = pick_top(cand_no_reply, reverse=True)
    top_slow = pick_top(cand_slow, reverse=True)
    top_no_next = pick_top(cand_no_next, reverse=True)
    top_good = pick_top(cand_good, reverse=True)

    for mid, items in top_no_reply.items():
        mname = mgr_name_by_id.get(mid) or agg.get(mid, {}).get("manager_name", "")
        for cid, score, sn_in, sn_out in items:
            examples.append(
                {
                    "run_ts": run_ts,
                    "manager_id": mid,
                    "manager_name": mname,
                    "category": "no_reply",
                    "chat_id": cid,
                    "snippet_in": sn_in,
                    "snippet_out": sn_out,
                    "note": "Клиент написал — нет ответа менеджера (потеря).",
                }
            )
    for mid, items in top_slow.items():
        mname = mgr_name_by_id.get(mid) or agg.get(mid, {}).get("manager_name", "")
        for cid, score, sn_in, sn_out in items:
            examples.append(
                {
                    "run_ts": run_ts,
                    "manager_id": mid,
                    "manager_name": mname,
                    "category": "slow_reply",
                    "chat_id": cid,
                    "snippet_in": sn_in,
                    "snippet_out": sn_out,
                    "note": "Очень долгий первый ответ (хвост p90).",
                }
            )
    for mid, items in top_no_next.items():
        mname = mgr_name_by_id.get(mid) or agg.get(mid, {}).get("manager_name", "")
        for cid, score, sn_in, sn_out in items:
            examples.append(
                {
                    "run_ts": run_ts,
                    "manager_id": mid,
                    "manager_name": mname,
                    "category": "no_next_step_high_intent",
                    "chat_id": cid,
                    "snippet_in": sn_in,
                    "snippet_out": sn_out,
                    "note": "Есть горячий запрос, но менеджер не закрывает в следующий шаг.",
                }
            )
    for mid, items in top_good.items():
        mname = mgr_name_by_id.get(mid) or agg.get(mid, {}).get("manager_name", "")
        for cid, score, sn_in, sn_out in items:
            examples.append(
                {
                    "run_ts": run_ts,
                    "manager_id": mid,
                    "manager_name": mname,
                    "category": "good",
                    "chat_id": cid,
                    "snippet_in": sn_in,
                    "snippet_out": sn_out,
                    "note": "Быстро + вопрос/следующий шаг (хороший паттерн).",
                }
            )

    ex_header = ["run_ts", "manager_id", "manager_name", "category", "chat_id", "snippet_in", "snippet_out", "note"]
    upsert_worksheet(ss, "weekly_examples", rows=dicts_to_table(examples, header=ex_header))

    (base / "behavior_digest_last_run.json").write_text(
        json.dumps({"run_ts": run_ts, "managers": len(out_rows), "examples": len(examples)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("OK: wrote behavior_snapshot_managers / weekly_behavior_delta_managers / weekly_examples")


if __name__ == "__main__":
    main()

