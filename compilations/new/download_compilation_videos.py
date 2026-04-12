#!/usr/bin/env python3
"""Download YouTube URLs from a compilation CSV using yt-dlp into the CSV's directory.

Skips videos already recorded in ``.yt-dlp-archive.txt`` or with an existing output file
(``--download-archive`` + ``--no-overwrites``), by default.

By default passes ``--cookies-from-browser chrome`` and
``--remote-components ejs:github`` so YouTube's JS challenges can run (without
that, requests often fail with "Sign in to confirm you're not a bot" even when
cookies are present). Disable the latter with ``--no-remote-components``.
"""

from __future__ import annotations

import argparse
import csv
import shutil
import subprocess
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read a compilation videos.csv and download each URL with yt-dlp "
        "into the same folder as the CSV."
    )
    parser.add_argument(
        "csv_path",
        type=Path,
        help="Path to videos.csv (must include a 'url' column).",
    )
    parser.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="Print URLs and yt-dlp commands without downloading.",
    )
    parser.add_argument(
        "--yt-dlp",
        dest="ytdlp",
        default="yt-dlp",
        help="yt-dlp executable name or path (default: yt-dlp).",
    )
    parser.add_argument(
        "--cookies",
        type=Path,
        default=None,
        metavar="FILE",
        help="Netscape-format cookies file for yt-dlp (--cookies).",
    )
    parser.add_argument(
        "--cookies-from-browser",
        dest="cookies_from_browser",
        default="chrome",
        metavar="SPEC",
        help="Browser cookies for yt-dlp (default: chrome). E.g. firefox, safari, 'chrome:Profile 1'. "
        "See yt-dlp --help.",
    )
    parser.add_argument(
        "--no-cookies-from-browser",
        action="store_true",
        help="Do not pass --cookies-from-browser to yt-dlp (use with --cookies or bare yt-dlp).",
    )
    parser.add_argument(
        "--no-remote-components",
        action="store_true",
        help="Do not pass --remote-components ejs:github (YouTube downloads usually need this).",
    )
    args = parser.parse_args()

    if args.no_cookies_from_browser:
        args.cookies_from_browser = None

    csv_path = args.csv_path.expanduser().resolve()
    if not csv_path.is_file():
        sys.exit(f"Not a file: {csv_path}")

    ytdlp = shutil.which(args.ytdlp) or args.ytdlp

    cookie_args: list[str] = []
    if args.cookies:
        cookie_path = args.cookies.expanduser().resolve()
        if not cookie_path.is_file():
            sys.exit(f"Cookies file not found: {cookie_path}")
        cookie_args.extend(["--cookies", str(cookie_path)])
    if args.cookies_from_browser:
        cookie_args.extend(["--cookies-from-browser", args.cookies_from_browser])

    remote_args: list[str] = []
    if not args.no_remote_components:
        remote_args.extend(["--remote-components", "ejs:github"])

    out_dir = csv_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)
    archive_path = out_dir / ".yt-dlp-archive.txt"
    # Safe filenames; id disambiguates duplicates
    out_template = str(out_dir / "%(title)s [%(id)s].%(ext)s")

    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "url" not in reader.fieldnames:
            sys.exit("CSV must have a header row including a 'url' column.")
        rows = [r for r in reader if r.get("url", "").strip()]

    if not rows:
        sys.exit("No rows with a non-empty url.")

    for i, row in enumerate(rows, start=1):
        url = row["url"].strip()
        title = (row.get("title") or "").strip()
        label = f"{title} — {url}" if title else url
        print(f"[{i}/{len(rows)}] {label}")

        cmd = [
            ytdlp,
            *cookie_args,
            *remote_args,
            "--restrict-filenames",
            "--no-overwrites",
            "--download-archive",
            str(archive_path),
            "-o",
            out_template,
            url,
        ]
        if args.dry_run:
            print(" ", " ".join(cmd))
            continue

        try:
            subprocess.run(cmd, check=True)
        except KeyboardInterrupt:
            print("\nInterrupted.", file=sys.stderr)
            sys.exit(130)
        except FileNotFoundError:
            sys.exit(
                f"Could not run {ytdlp!r}. Install yt-dlp and ensure it is on PATH, "
                f"or pass --yt-dlp /path/to/yt-dlp."
            )
        except subprocess.CalledProcessError as e:
            print(f"yt-dlp exited {e.returncode} for {url!r}", file=sys.stderr)


if __name__ == "__main__":
    main()
