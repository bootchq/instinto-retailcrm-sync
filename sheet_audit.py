from __future__ import annotations

"""
Аудит Google Sheets без выгрузки PII:
- список вкладок
- заголовки (header)
- количество непустых строк

Результат сохраняется в файл sheet_audit_report.json рядом.
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, List


def _load_env(env_path: Path) -> Dict[str, str]:
    env: Dict[str, str] = {}
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


_RE_PHONE = re.compile(r"\b\+?\d[\d\s\-()]{7,}\b")
_RE_LONG_DIGITS = re.compile(r"\b\d{5,}\b")


def _redact(s: Any, max_len: int = 120) -> str:
    s = "" if s is None else str(s)
    s = _RE_LONG_DIGITS.sub("***", s)
    s = _RE_PHONE.sub("***", s)
    s = s.replace("\n", " ")
    return s[:max_len]


def main() -> None:
    base = Path(__file__).resolve().parent
    env_path = base / "env"
    if not env_path.exists():
        raise SystemExit(f"Не найден env: {env_path}")

    env = _load_env(env_path)
    sheet_id = env["GOOGLE_SHEETS_ID"]
    sa_path = Path(env["GOOGLE_SERVICE_ACCOUNT_JSON"]).expanduser()
    if not sa_path.exists():
        raise SystemExit(f"Не найден service account json: {sa_path}")

    import gspread
    from google.oauth2.service_account import Credentials

    creds = Credentials.from_service_account_file(
        str(sa_path),
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    gc = gspread.authorize(creds)
    ss = gc.open_by_key(sheet_id)

    report: Dict[str, Any] = {
        "spreadsheet_title": ss.title,
        "spreadsheet_id": sheet_id,
        "worksheets": [],
    }

    for ws in ss.worksheets():
        header = ws.row_values(1)
        # ВНИМАНИЕ: get_all_values может быть тяжёлым, но это audit.
        values = ws.get_all_values()
        non_empty_rows = len(values)
        sample_rows: List[List[str]] = []
        # Для некоторых “технических” вкладок полезно видеть больше колонок/строк (всё равно с редактированием).
        wide_titles = {
            "manager_report",
            "behavior_snapshot_managers",
            "weekly_behavior_delta_managers",
            "weekly_examples",
            "weekly_digest_managers",
            "weekly_digest_channels",
        }
        row_limit = 10 if ws.title in wide_titles else 3
        col_limit = 18 if ws.title in wide_titles else 8
        for r in values[1 : 1 + row_limit]:
            sample_rows.append([_redact(x) for x in r[: min(col_limit, len(r))]])

        report["worksheets"].append(
            {
                "title": ws.title,
                "non_empty_rows": non_empty_rows,
                "header": header,
                "sample_rows_first_3": sample_rows,
            }
        )

    out = base / "sheet_audit_report.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"OK: wrote {out}")


if __name__ == "__main__":
    main()


