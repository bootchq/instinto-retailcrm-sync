from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

from dateutil import parser as dtparser
from zoneinfo import ZoneInfo


def _dt(v: Any) -> Optional[datetime]:
    if not v:
        return None
    if isinstance(v, datetime):
        return v
    try:
        return dtparser.isoparse(str(v))
    except Exception:
        return None


def _to_tz(dt: datetime, tz: ZoneInfo) -> datetime:
    # Если нет tzinfo — считаем, что это время уже в локальном TZ
    if dt.tzinfo is None:
        return dt.replace(tzinfo=tz)
    return dt.astimezone(tz)


def _parse_work_hours(work_hours: str) -> Tuple[time, time]:
    """
    Формат: "HH:MM-HH:MM", например "10:00-23:00".
    """
    s = work_hours.strip()
    a, b = s.split("-", 1)
    h1, m1 = a.strip().split(":", 1)
    h2, m2 = b.strip().split(":", 1)
    start = time(int(h1), int(m1))
    end = time(int(h2), int(m2))
    return start, end


def _business_seconds_between(
    start_dt: datetime,
    end_dt: datetime,
    *,
    tz: ZoneInfo,
    work_start: time,
    work_end: time,
) -> Optional[int]:
    """
    Считает длительность между start_dt и end_dt только в пределах рабочего окна.

    Если end_dt < start_dt -> None.
    """
    if end_dt < start_dt:
        return None

    s = _to_tz(start_dt, tz)
    e = _to_tz(end_dt, tz)

    # Поддерживаем только окно в пределах суток (без ночных смен).
    if work_end <= work_start:
        # на будущее: если нужно, расширим на "22:00-06:00"
        raise ValueError("work hours that cross midnight are not supported")

    total = 0
    cur_day = s.date()
    last_day = e.date()

    while cur_day <= last_day:
        window_start = datetime.combine(cur_day, work_start, tzinfo=tz)
        window_end = datetime.combine(cur_day, work_end, tzinfo=tz)

        seg_start = max(s, window_start)
        seg_end = min(e, window_end)
        if seg_end > seg_start:
            total += int((seg_end - seg_start).total_seconds())

        cur_day = cur_day + timedelta(days=1)

    return total


_RE_HAS_QUESTION = re.compile(r"\?\s*$|(\bка(кой|кая|кие|кие)\b|\bчто\b|\bсколько\b|\bкогда\b|\bгде\b)", re.IGNORECASE)
_RE_PRICE_INTENT = re.compile(r"\bцена\b|\bсколько\s+стоит\b|\bпрайс\b|\bстоимость\b", re.IGNORECASE)
_RE_BUY_INTENT = re.compile(r"\bхочу\b|\bкуп(ить|лю)\b|\bзакаж(у|ать)\b|\bоформ(ить|ляем)\b", re.IGNORECASE)
_RE_AVAIL_INTENT = re.compile(r"\bналич(ие|ии)\b|\bесть\b|\bв\s+наличии\b", re.IGNORECASE)
_RE_NEXT_STEP = re.compile(r"\bоформим\b|\bссылка\b|\bкорзин(а|у)\b|\bоплат(а|ить)\b|\bдоставка\b", re.IGNORECASE)


@dataclass(frozen=True)
class ChatMetrics:
    chat_id: str
    channel: str
    manager_id: Optional[str]
    manager_name: str
    inbound_count: int
    outbound_count: int
    first_inbound_at: Optional[datetime]
    first_outbound_at: Optional[datetime]
    first_response_sec: Optional[int]
    last_inbound_at: Optional[datetime]
    last_outbound_at: Optional[datetime]
    unanswered_inbound: int
    advice: List[str]


def compute_chat_metrics(
    chat: Dict[str, Any],
    messages: List[Dict[str, Any]],
    *,
    users_by_id: Dict[int, Dict[str, Any]],
    tz_name: str,
    work_hours: str = "10:00-23:00",
    slow_first_reply_sec: int = 10 * 60,
) -> ChatMetrics:
    """
    Rule-based аналитика по одному чату.

    Ожидаемый формат сообщений (адаптируется в export_to_sheets.py):
    - direction: "in" | "out"
    - sentAt: iso datetime
    - text: str
    - managerId: optional
    """
    chat_id = str(chat.get("id") or chat.get("chatId") or "")
    channel = str(chat.get("channel") or chat.get("source") or "")

    # менеджер: берём из чата, иначе из первых исходящих сообщений
    manager_id_val = chat.get("managerId") or chat.get("userId") or None
    if manager_id_val is None:
        for m in messages:
            if m.get("direction") == "out" and m.get("managerId"):
                manager_id_val = m.get("managerId")
                break
    manager_id = str(manager_id_val) if manager_id_val is not None else None

    manager_name = ""
    if manager_id is not None:
        u = users_by_id.get(int(manager_id)) if manager_id.isdigit() else None
        if u:
            manager_name = str(u.get("firstName") or u.get("name") or u.get("email") or manager_id)
        else:
            manager_name = manager_id

    inbound = [m for m in messages if m.get("direction") == "in"]
    outbound = [m for m in messages if m.get("direction") == "out"]

    inbound_count = len(inbound)
    outbound_count = len(outbound)

    tz = ZoneInfo(tz_name)
    work_start, work_end = _parse_work_hours(work_hours)

    inbound_times = [_dt(m.get("sentAt")) for m in inbound]
    outbound_times = [_dt(m.get("sentAt")) for m in outbound]
    inbound_times = [t for t in inbound_times if t]
    outbound_times = [t for t in outbound_times if t]

    first_inbound_at = min(inbound_times) if inbound_times else None
    # Первый исходящий ПОСЛЕ первого входящего
    first_outbound_at = None
    if first_inbound_at:
        for t in sorted(outbound_times):
            if t >= first_inbound_at:
                first_outbound_at = t
                break
    last_inbound_at = max(inbound_times) if inbound_times else None
    last_outbound_at = max(outbound_times) if outbound_times else None

    first_response_sec: Optional[int] = None
    if first_inbound_at and first_outbound_at:
        # Считаем “рабочее” время реакции: вне 10:00–23:00 таймер не тикает.
        first_response_sec = _business_seconds_between(
            first_inbound_at,
            first_outbound_at,
            tz=tz,
            work_start=work_start,
            work_end=work_end,
        )

    # unanswered inbound: считаем входящие после последнего исходящего
    unanswered_inbound = 0
    if last_inbound_at and (not last_outbound_at or last_inbound_at > last_outbound_at):
        # грубо: все входящие после последнего out
        cutoff = last_outbound_at
        for t in inbound_times:
            if cutoff is None or t > cutoff:
                unanswered_inbound += 1

    # советы
    advice: List[str] = []
    if inbound_count > 0 and outbound_count == 0:
        advice.append("Нет ответа менеджера на входящие сообщения — проверьте распределение/уведомления и дайте быстрый первый ответ.")
    if first_response_sec is not None and first_response_sec > slow_first_reply_sec:
        advice.append("Долгий первый ответ — сократите время реакции (цель: ≤10 минут) и используйте быстрый шаблон приветствия+уточнение.")
    if unanswered_inbound > 0:
        advice.append("Есть непрочитанные/неотвеченные входящие — сделайте follow-up и зафиксируйте следующий шаг (ссылка/оформление/варианты).")

    # контентные эвристики
    text_out = " \n".join([str(m.get("text") or "") for m in outbound][:6])
    text_in = " \n".join([str(m.get("text") or "") for m in inbound][:6])
    if inbound_count > 0 and outbound_count > 0:
        if not _RE_HAS_QUESTION.search(text_out):
            advice.append("Мало уточняющих вопросов — добавьте 1–2 вопроса по потребности/параметрам, прежде чем давать финальный оффер.")

        intent = bool(_RE_PRICE_INTENT.search(text_in) or _RE_BUY_INTENT.search(text_in) or _RE_AVAIL_INTENT.search(text_in))
        if intent and not _RE_NEXT_STEP.search(text_out):
            advice.append("Клиент проявляет интерес (цена/наличие/хочу), но нет явного next step — предложите вариант и завершите действием: ссылка/оформление/доставка/оплата.")

    # дедуп советов
    seen = set()
    advice = [a for a in advice if not (a in seen or seen.add(a))]

    return ChatMetrics(
        chat_id=chat_id,
        channel=channel,
        manager_id=manager_id,
        manager_name=manager_name,
        inbound_count=inbound_count,
        outbound_count=outbound_count,
        first_inbound_at=first_inbound_at,
        first_outbound_at=first_outbound_at,
        first_response_sec=first_response_sec,
        last_inbound_at=last_inbound_at,
        last_outbound_at=last_outbound_at,
        unanswered_inbound=unanswered_inbound,
        advice=advice,
    )


def aggregate_manager_summary(rows: Iterable[ChatMetrics]) -> List[Dict[str, Any]]:
    by_mgr: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in rows:
        key = (r.manager_id or "", r.manager_name or "")
        s = by_mgr.setdefault(
            key,
            {
                "manager_id": r.manager_id or "",
                "manager_name": r.manager_name or "",
                "chats": 0,
                "inbound": 0,
                "outbound": 0,
                "unanswered_inbound": 0,
                "slow_first_reply_chats": 0,
                "no_reply_chats": 0,
                "responded_chats": 0,
                "first_reply_secs": [],
            },
        )
        s["chats"] += 1
        s["inbound"] += r.inbound_count
        s["outbound"] += r.outbound_count
        s["unanswered_inbound"] += r.unanswered_inbound
        if r.outbound_count == 0 and r.inbound_count > 0:
            s["no_reply_chats"] += 1
        if r.first_response_sec is not None and r.first_response_sec > 10 * 60:
            s["slow_first_reply_chats"] += 1
        if r.first_response_sec is not None:
            s["responded_chats"] += 1
            s["first_reply_secs"].append(int(r.first_response_sec))

    def _pct(values: List[int], p: float) -> Optional[int]:
        if not values:
            return None
        vals = sorted(values)
        # ближайший ранг
        k = max(0, min(len(vals) - 1, int(round((len(vals) - 1) * p))))
        return int(vals[k])

    out: List[Dict[str, Any]] = []
    for s in by_mgr.values():
        vals: List[int] = s.pop("first_reply_secs", [])
        median = _pct(vals, 0.5)
        p90 = _pct(vals, 0.9)
        inbound_chats = int(s["chats"])  # приближение: чаты в целом; точнее можно по inbound_count>0
        out.append(
            {
                **s,
                "median_first_reply_sec": median if median is not None else "",
                "p90_first_reply_sec": p90 if p90 is not None else "",
                "response_rate": (float(s["responded_chats"]) / float(inbound_chats)) if inbound_chats else "",
            }
        )

    # сортировка: больше чатов сверху
    return sorted(out, key=lambda x: (-int(x["chats"]), x["manager_name"]))


def aggregate_channel_summary(rows: Iterable[ChatMetrics]) -> List[Dict[str, Any]]:
    by_ch: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        ch = (r.channel or "").lower() or "unknown"
        s = by_ch.setdefault(
            ch,
            {
                "channel": ch,
                "chats": 0,
                "inbound": 0,
                "outbound": 0,
                "no_reply_chats": 0,
                "slow_first_reply_chats": 0,
                "responded_chats": 0,
                "first_reply_secs": [],
            },
        )
        s["chats"] += 1
        s["inbound"] += r.inbound_count
        s["outbound"] += r.outbound_count
        if r.outbound_count == 0 and r.inbound_count > 0:
            s["no_reply_chats"] += 1
        if r.first_response_sec is not None and r.first_response_sec > 10 * 60:
            s["slow_first_reply_chats"] += 1
        if r.first_response_sec is not None:
            s["responded_chats"] += 1
            s["first_reply_secs"].append(int(r.first_response_sec))

    def _pct(values: List[int], p: float) -> Optional[int]:
        if not values:
            return None
        vals = sorted(values)
        k = max(0, min(len(vals) - 1, int(round((len(vals) - 1) * p))))
        return int(vals[k])

    out: List[Dict[str, Any]] = []
    for s in by_ch.values():
        vals: List[int] = s.pop("first_reply_secs", [])
        out.append(
            {
                **s,
                "median_first_reply_sec": _pct(vals, 0.5) or "",
                "p90_first_reply_sec": _pct(vals, 0.9) or "",
                "response_rate": (float(s["responded_chats"]) / float(s["chats"])) if s["chats"] else "",
            }
        )

    return sorted(out, key=lambda x: (-int(x["chats"]), x["channel"]))


