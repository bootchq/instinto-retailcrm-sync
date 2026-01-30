from __future__ import annotations

"""
Одна команда для полного прогона:
  python run_all.py

Что делает:
- export_to_sheets.py
- manager_report.py
- behavior_digest.py
- weekly_digest.py
- (опционально) send_weekly_telegram.py
- sheet_audit.py

Идея: тебе не помнить порядок и команды.
"""

import os
import subprocess
import sys
from pathlib import Path
from typing import List


def _run(module_or_file: str, args: List[str] | None = None) -> None:
    args = args or []
    # запускаем тем же python, что и в .venv
    cmd = [sys.executable, module_or_file] + args
    print(f"\n=== RUN: {' '.join(cmd)} ===\n", flush=True)
    subprocess.run(cmd, check=True)


def main() -> None:
    base = Path(__file__).resolve().parent
    os.chdir(str(base))

    _run("export_to_sheets.py")
    _run("manager_report.py")
    _run("behavior_digest.py")
    _run("weekly_digest.py")

    # Telegram — опционально
    _run("send_weekly_telegram.py")

    _run("sheet_audit.py")
    print("\nOK: full pipeline done\n")


if __name__ == "__main__":
    main()

