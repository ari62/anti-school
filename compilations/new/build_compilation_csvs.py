#!/usr/bin/env python3
"""Parse compilations.md into one subdirectory + videos.csv per ## section."""

from __future__ import annotations

import csv
import re
from pathlib import Path

LIST_ITEM_RE = re.compile(
    r"^-\s*\[(?P<title>[^\]]*)\]\((?P<url>https://www\.youtube\.com[^)]+)\)(?P<rest>.*)$"
)


def slugify(heading: str, max_len: int = 80) -> str:
    s = heading.strip()
    if s.startswith("##"):
        s = s[2:].strip()
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return (s[:max_len] or "section").rstrip("_")


def parse_sections(md_text: str) -> list[tuple[str, list[dict[str, str]]]]:
    """Return [(section_slug, rows), ...] where rows have title, url, note."""
    lines = md_text.splitlines()
    sections: list[tuple[str, list[dict[str, str]]]] = []
    current_slug: str | None = None
    current_rows: list[dict[str, str]] = []

    heading_re = re.compile(r"^##\s+(.+)$")

    for line in lines:
        hm = heading_re.match(line)
        if hm:
            if current_slug is not None:
                sections.append((current_slug, current_rows))
            current_slug = slugify(hm.group(1))
            current_rows = []
            continue

        if current_slug is None:
            continue

        m = LIST_ITEM_RE.match(line)
        if not m:
            continue
        title = m.group("title").strip()
        url = m.group("url").strip()
        rest = m.group("rest").strip()
        if rest.startswith("(") and rest.endswith(")"):
            note = rest[1:-1].strip()
        else:
            note = rest
        current_rows.append({"title": title, "url": url, "note": note})

    if current_slug is not None:
        sections.append((current_slug, current_rows))

    return sections


def write_csvs(base_dir: Path, sections: list[tuple[str, list[dict[str, str]]]]) -> None:
    base_dir.mkdir(parents=True, exist_ok=True)
    for slug, rows in sections:
        if not rows:
            continue
        sub = base_dir / slug
        sub.mkdir(parents=True, exist_ok=True)
        csv_path = sub / "videos.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["title", "url", "note"], extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        print(f"Wrote {len(rows)} rows -> {csv_path}")


def main() -> None:
    here = Path(__file__).resolve().parent
    md_path = here / "compilations.md"
    text = md_path.read_text(encoding="utf-8")
    sections = parse_sections(text)
    write_csvs(here, sections)


if __name__ == "__main__":
    main()
