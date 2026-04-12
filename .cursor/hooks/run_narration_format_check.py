#!/usr/bin/env python3
"""
Cursor hook: stdin JSON with file_path (afterFileEdit / afterTabFileEdit).
Runs scripts/check_narration_format.py only for narrations/**/*.md that already
use the two-part layout (contain '## VOICEOVER'), so legacy files stay quiet.
Always exits 0 (does not block edits).
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
CHECK_SCRIPT = REPO_ROOT / "scripts" / "check_narration_format.py"


def _norm(p: str) -> str:
    return p.replace("\\", "/")


def should_run_check(abs_path: Path) -> bool:
    s = _norm(str(abs_path))
    if "/narrations/" not in s and not s.endswith("/narrations"):
        return False
    if not abs_path.suffix.lower() == ".md":
        return False
    if abs_path.name == "README.md":
        return False
    try:
        text = abs_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    # Only auto-check files that opted into the two-part layout
    if "## VOICEOVER" not in text:
        return False
    return True


def main() -> None:
    raw = sys.stdin.read()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        sys.exit(0)

    fp = data.get("file_path") or ""
    if not fp:
        sys.exit(0)

    path = Path(fp).resolve()
    if not should_run_check(path):
        sys.exit(0)

    if not CHECK_SCRIPT.is_file():
        print(
            f"narration-format hook: missing {CHECK_SCRIPT}",
            file=sys.stderr,
        )
        sys.exit(0)

    subprocess.run(
        [sys.executable, str(CHECK_SCRIPT), str(path)],
        cwd=str(REPO_ROOT),
    )
    sys.exit(0)


if __name__ == "__main__":
    main()
