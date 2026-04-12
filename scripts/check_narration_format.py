#!/usr/bin/env python3
"""
Validate two-part narration layout (VOICEOVER vs SOURCES) for explicitly listed files.
Does not edit files. Legacy scripts without these headings are left unchanged by design.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

VOICEOVER_HEADINGS = (
    "## VOICEOVER",
    "## VOICEOVER SCRIPT",
)
SOURCES_HEADINGS = (
    "## SOURCES AND PRODUCER NOTES",
    "## SOURCES",
    "## SOURCES / PRODUCER CHECKLIST",
)


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def has_heading(text: str, headings: tuple[str, ...]) -> bool:
    lines = text.splitlines()
    for line in lines:
        stripped = line.strip()
        for h in headings:
            if stripped == h or stripped.startswith(h + " "):
                return True
    return False


def vo_contains_url(text: str) -> bool:
    """VO block should not contain raw http(s) links; inline citation sentences are OK."""
    if not has_heading(text, VOICEOVER_HEADINGS):
        return False
    lower = text.lower()
    start = 0
    for token in VOICEOVER_HEADINGS:
        idx = lower.find(token.lower())
        if idx != -1:
            start = idx
            break
    end = len(text)
    for token in SOURCES_HEADINGS:
        idx = lower.find(token.lower(), start + 1)
        if idx != -1:
            end = min(end, idx)
    vo = text[start:end]
    return bool(re.search(r"https?://", vo))


def check_file(path: Path) -> list[str]:
    issues: list[str] = []
    if not path.is_file():
        return [f"Not a file: {path}"]
    text = read_text(path)
    if not has_heading(text, VOICEOVER_HEADINGS):
        issues.append("Missing '## VOICEOVER' heading.")
    if not has_heading(text, SOURCES_HEADINGS):
        issues.append("Missing '## SOURCES' or '## SOURCES AND PRODUCER NOTES' heading.")
    if vo_contains_url(text):
        issues.append("VOICEOVER section appears to contain http(s) URLs; move to SOURCES.")
    return issues


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check narration markdown for VOICEOVER + SOURCES split."
    )
    parser.add_argument(
        "paths",
        nargs="*",
        help="Markdown files to check (paths relative to CWD or absolute).",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with code 1 if any checked file has issues.",
    )
    args = parser.parse_args()
    if not args.paths:
        print(
            "Usage: python scripts/check_narration_format.py [--strict] <file.md> [file2.md ...]",
            file=sys.stderr,
        )
        print("Example: python scripts/check_narration_format.py narrations/new/draft.md", file=sys.stderr)
        return 2

    any_issues = False
    for raw in args.paths:
        path = Path(raw)
        if not path.is_absolute():
            path = Path.cwd() / path
        path = path.resolve()
        try:
            path.relative_to(REPO_ROOT)
        except ValueError:
            print(f"Skip (outside repo): {path}", file=sys.stderr)
            continue
        issues = check_file(path)
        if issues:
            any_issues = True
            print(f"{path}:")
            for i in issues:
                print(f"  - {i}")
        else:
            print(f"{path}: OK")

    return 1 if (args.strict and any_issues) else 0


if __name__ == "__main__":
    sys.exit(main())
