#!/usr/bin/env python3
"""
Extract anti-school quotes from PDFs → JSONL, YouTube segment groupings, and Markdown.

Requires: OPENAI_API_KEY (except --parse-only, --migrate-schema). Loaded from, in merge order:
  - <project>/.env then books/.env (later file wins for duplicate keys)
  - variables already set in the shell are never overwritten

Modes:
  - Default: strict extraction pass; use --with-recall to add broad recall pass in one run.
  - --recall-only: broad recall only (separate progress: recall_extracted_books / recall_book_chunk).
  - --migrate-schema: add extraction_pass + confidence to existing JSONL (no API).
  - --score-existing: LLM intent_fit + confidence for rows missing intent_fit.

Optional: pip install pdfplumber (auto-installed if missing)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from typing import Any

# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
PDF_DIR = SCRIPT_DIR / "pdfs"
PARSED_DIR = SCRIPT_DIR / "parsed"
QUOTES_DIR = SCRIPT_DIR / "quotes"
JSONL_PATH = QUOTES_DIR / "anti_school_quotes.jsonl"
SEGMENTS_PATH = QUOTES_DIR / "youtube_segments.json"
MD_PATH = QUOTES_DIR / "anti_school_quotes.md"
PROGRESS_PATH = QUOTES_DIR / "progress.json"


def _parse_env_file(path: Path) -> dict[str, str]:
    """Parse KEY=VALUE lines (optional quotes, # comments, export prefix)."""
    if not path.is_file():
        return {}
    out: dict[str, str] = {}
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return {}
    for line in raw.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if s.startswith("export "):
            s = s[7:].strip()
        if "=" not in s:
            continue
        key, _, val = s.partition("=")
        key = key.strip()
        val = val.strip()
        if not key:
            continue
        if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
            val = val[1:-1]
        out[key] = val
    return out


def load_env_from_dotenv() -> None:
    """Apply .env files without overriding existing OS environment variables."""
    root_vars = _parse_env_file(PROJECT_ROOT / ".env")
    books_vars = _parse_env_file(SCRIPT_DIR / ".env")
    merged = {**root_vars, **books_vars}
    for key, value in merged.items():
        if key not in os.environ:
            os.environ[key] = value


# DESCHOOLING_ivan_illich.pdf is a duplicate of Illich_Ivan_Deschooling_Society.pdf — use Illich file only.
BOOKS: dict[str, dict[str, str]] = {
    "stop-stealing-dreams6print.pdf": {
        "author": "Seth Godin",
        "source": "Stop Stealing Dreams",
    },
    "how-children-fail-john-holt.pdf": {
        "author": "John Holt",
        "source": "How Children Fail",
    },
    "school-is-bad-for-children-by-j-holt_engl1301.pdf": {
        "author": "John Holt",
        "source": "School Is Bad for Children",
    },
    "TeachYourOwnHolt.pdf": {
        "author": "John Holt",
        "source": "Teach Your Own",
    },
    "insteadofeducation.pdf": {
        "author": "John Holt",
        "source": "Instead of Education",
    },
    "A LIFE WORTH LIVING - JOHN HOLT.pdf": {
        "author": "John Holt",
        "source": "A Life Worth Living",
    },
    "Illich_Ivan_Deschooling_Society.pdf": {
        "author": "Ivan Illich",
        "source": "Deschooling Society",
    },
    "2015.138838.Summerhill-A-Radical-Approach-To-Education_text.pdf": {
        "author": "A.S. Neill",
        "source": "Summerhill",
    },
    "a_s_neill--freedom--not-license_1.pdf": {
        "author": "A.S. Neill",
        "source": "Freedom Not License",
    },
    "peter-gray-free-to-learn.pdf": {
        "author": "Peter Gray",
        "source": "Free to Learn",
    },
    "Paulo Freire, Myra Bergman Ramos, Donaldo Macedo - Pedagogy of the Oppressed, 30th Anniversary Edition (2000, Bloomsbury Academic).pdf": {
        "author": "Paulo Freire",
        "source": "Pedagogy of the Oppressed",
    },
    "John Taylor Gatto - The Underground History of American Education Book.pdf": {
        "author": "John Taylor Gatto",
        "source": "The Underground History of American Education",
    },
    "John Taylor Gatto-Dumbing Us Down_ The Hidden Curriculum of Compulsory Schooling (2002).pdf": {
        "author": "John Taylor Gatto",
        "source": "Dumbing Us Down",
    },
    "EDUCATION-Weapons-of-Mass-Instruction.pdf": {
        "author": "John Taylor Gatto",
        "source": "Weapons of Mass Instruction",
    },
    "against-schools-the-tyranny-of-compulsory-schooling-reading-version.pdf": {
        "author": "John Taylor Gatto",
        "source": "Against School",
    },
    "savage-inequalities-jonathan-kozol.pdf": {
        "author": "Jonathan Kozol",
        "source": "Savage Inequalities",
    },
    "Everet_Reimer_school_is_dead.pdf": {
        "author": "Everett Reimer",
        "source": "School Is Dead",
    },
    "George Dennison  The Lives of Children.pdf": {
        "author": "George Dennison",
        "source": "The Lives of Children",
    },
    "Herbert Kohl  36 Children.pdf": {
        "author": "Herbert Kohl",
        "source": "36 Children",
    },
    "Paul Goodman  Compulsory Mis-Education.pdf": {
        "author": "Paul Goodman",
        "source": "Compulsory Mis-Education",
    },
}

CHUNK_WORDS = 4000
MODEL = "gpt-4o-mini"
RECALL_MAX_PER_CHUNK_DEFAULT = 10


def ensure_pdfplumber() -> None:
    try:
        import pdfplumber  # noqa: F401
    except ImportError:
        print("Installing pdfplumber…", file=sys.stderr)
        # No --user: Conda/v-env Python often omits user site-packages from sys.path,
        # which makes "Successfully installed" but import still fail.
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "pdfplumber"],
            stdout=sys.stderr,
        )


def ensure_openai() -> None:
    try:
        import openai  # noqa: F401
    except ImportError:
        print("Installing openai…", file=sys.stderr)
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "openai"],
            stdout=sys.stderr,
        )


def extract_pdf_text(pdf_path: Path, out_txt: Path) -> str:
    import pdfplumber

    parts: list[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            parts.append(t)
    text = "\n\n".join(parts)
    text = re.sub(r"\n{3,}", "\n\n", text)
    out_txt.parent.mkdir(parents=True, exist_ok=True)
    out_txt.write_text(text, encoding="utf-8")
    return text


def load_or_parse_book(pdf_name: str) -> str:
    stem = Path(pdf_name).stem
    txt_path = PARSED_DIR / f"{stem}.txt"
    if txt_path.exists():
        return txt_path.read_text(encoding="utf-8")
    pdf_path = PDF_DIR / pdf_name
    if not pdf_path.exists():
        raise FileNotFoundError(f"Missing PDF: {pdf_path}")
    print(f"Parsing PDF → {txt_path.name}", file=sys.stderr)
    return extract_pdf_text(pdf_path, txt_path)


def chunk_by_words(text: str, max_words: int) -> list[str]:
    words = text.split()
    chunks: list[str] = []
    i = 0
    while i < len(words):
        chunk = words[i : i + max_words]
        chunks.append(" ".join(chunk))
        i += max_words
    return chunks if chunks else [""]


def load_progress() -> dict[str, Any]:
    if not PROGRESS_PATH.exists():
        return {
            "extracted_books": [],
            "book_chunk": {},
            "recall_extracted_books": [],
            "recall_book_chunk": {},
        }
    data = json.loads(PROGRESS_PATH.read_text(encoding="utf-8"))
    data.setdefault("extracted_books", [])
    data.setdefault("book_chunk", {})
    data.setdefault("recall_extracted_books", [])
    data.setdefault("recall_book_chunk", {})
    return data


def save_progress(data: dict[str, Any]) -> None:
    QUOTES_DIR.mkdir(parents=True, exist_ok=True)
    PROGRESS_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")


def get_openai_client():
    ensure_openai()
    from openai import OpenAI

    key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not key:
        print(
            "ERROR: Set OPENAI_API_KEY (e.g. in .env at project root or in books/.env).",
            file=sys.stderr,
        )
        sys.exit(1)
    return OpenAI()


def call_chat_json(client, system: str, user: str, max_tokens: int = 8192) -> Any:
    """Return parsed JSON object from model (json_object mode)."""
    resp = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        response_format={"type": "json_object"},
        temperature=0.2,
        max_tokens=max_tokens,
    )
    content = resp.choices[0].message.content or "{}"
    return json.loads(content)


def call_chat_json_extraction(
    client,
    system: str,
    user: str,
    *,
    max_tokens: int = 8192,
    max_attempts: int = 5,
) -> dict[str, Any]:
    """
    Like call_chat_json, but retries on truncated/malformed JSON (common with long quotes).
    """
    last_err: Exception | None = None
    tokens = max_tokens
    for attempt in range(max_attempts):
        try:
            resp = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                response_format={"type": "json_object"},
                temperature=0.2,
                max_tokens=tokens,
            )
            choice = resp.choices[0]
            content = choice.message.content or "{}"
            if getattr(choice, "finish_reason", None) == "length":
                raise ValueError("response truncated (finish_reason=length)")
            out = json.loads(content)
            if not isinstance(out, dict):
                raise ValueError("expected JSON object")
            return out
        except (json.JSONDecodeError, ValueError, TypeError) as e:
            last_err = e
            tokens = min(tokens + 4096, 16384)
            time.sleep(1.5 * (attempt + 1))
    assert last_err is not None
    raise last_err


# Strict pass: high-precision clips for anti-school narration (see INTENT_AND_VALUES.md — no pro-school softening).
EXTRACTION_SYSTEM = """You are a careful literary extractor for spoken YouTube narration about critiques of compulsory schooling.

Non-negotiable:
- Extract ONLY verbatim sentences or short passages from the SOURCE TEXT. Do not invent, summarize as if it were a quote, or paraphrase the "text" field.
- Remove footnote markers, page numbers, and citation clutter from "text" when present in the source.
- Return strictly valid JSON. In "text" and "narration", escape any " as \\" and use \\n for line breaks. If the chunk is huge, return fewer quotes rather than risk truncation.
- Reject passages that praise compulsory schooling as necessary, or that add "balanced" caveats meant to rehabilitate school — this project uses only critique-aligned material.

Themes to prefer (still anti-school / institutional critique): schooling vs learning; coercion vs freedom; control, surveillance, compliance; testing, grades, authority; curiosity and creativity crushed; childhood and time stolen; inequality, class, race and school; work-like or carceral structure; sleep, stress, health; superficial test-prep "learning".

Output fields per quote:
- "type": "quote" (1–2 sentences) or "passage" (2–5 sentences, high impact).
- "length": "short" or "medium".
- "tone": exactly one of: provocative, reflective, critical.
- "themes": 1–4 short snake_case labels.
- "narration": voiceover version — same meaning, smoother rhythm; rephrase ONLY here, not in "text".
- Skip boring filler, dense jargon, or lines that need heavy context unless they sharply serve the themes above.
- If nothing qualifies, return {"quotes": []}.

Output JSON only: {"quotes": [ { "type", "text", "themes", "tone", "length", "narration" }, ... ]}"""


# Broad recall: more candidates per chunk, scored; still must be usable for anti-school VO — not generic book summary.
EXTRACTION_RECALL_SYSTEM = """You are extracting EXTRA candidate lines for anti-school YouTube narration (broad recall pass).

Non-negotiable:
- Verbatim "text" only from SOURCE TEXT — no invention or paraphrase in "text".
- Valid JSON only; escape quotes in strings as \\" and newlines as \\n.
- Reject: praise for compulsory schooling, "school is necessary," both-sides apologia, or language that softens the critique of forced schooling.
- Include passages that could support themes such as: childhood, inequality, work, class, race, health, stress, sleep, surveillance, compliance, freedom, self-education, institutional waste, test culture, boredom, fear, labeling — ONLY when they can serve anti-school narration.

This pass allows slightly weaker "standalone" lines if you mark lower "confidence".

Per quote you MUST output:
- "type", "text", "themes", "tone", "length", "narration" (same meaning as strict pass).
- "confidence": number from 0.0 to 1.0 (how strong for VO + intent fit).
- "recall_rationale": one short phrase (why it might work).

The user message states MAX_QUOTES_FOR_THIS_CHUNK — return at most that many quotes; prefer diversity over redundancy. If nothing qualifies, return {"quotes": []}.

Output JSON only: {"quotes": [ { "type", "text", "themes", "tone", "length", "narration", "confidence", "recall_rationale" }, ... ]}"""


SEGMENTS_SYSTEM_BATCH = """You group quote records into themed segments for a YouTube narration video.

This is ONE BATCH of quotes (more batches exist elsewhere). Use ONLY the ids present in this batch.

Rules:
- Propose 3–8 segments with clear titles, e.g. "School vs Learning", "Control and Obedience", "Death of Curiosity", "Freedom and Self-Education".
- Each segment: include as many relevant quotes from THIS batch as fit (typically 3–15 per segment). A quote may appear in only one segment.
- Order quote_ids within each segment for spoken flow.
- Output JSON only: {"segments": [ {"title": "...", "quote_ids": ["..."] }, ... ]}"""


def merge_segment_maps(
    maps: list[dict[str, list[str]]],
) -> list[dict[str, Any]]:
    """Merge segments by normalized title; preserve first-seen title casing."""
    canonical: dict[str, tuple[str, list[str]]] = {}
    for m in maps:
        for title, ids in m.items():
            key = title.strip().lower()
            if key not in canonical:
                canonical[key] = (title.strip(), [])
            seen = set(canonical[key][1])
            for qid in ids:
                if qid not in seen:
                    canonical[key][1].append(qid)
                    seen.add(qid)
    return [{"title": t, "quote_ids": ids} for t, ids in canonical.values()]


def quote_fingerprint(text: str) -> str:
    """Normalize quote text for deduplication (recall vs existing JSONL)."""
    t = (text or "").lower()
    t = re.sub(r"\s+", " ", t).strip()
    t = re.sub(r"[^\w\s]", "", t)
    return t


def build_fingerprint_set(quotes: list[dict[str, Any]]) -> set[str]:
    out: set[str] = set()
    for q in quotes:
        fp = quote_fingerprint(str(q.get("text", "")))
        if fp:
            out.add(fp)
    return out


def migrate_schema_jsonl() -> None:
    """Add extraction_pass + confidence to existing rows; no API calls."""
    if not JSONL_PATH.exists():
        print(f"No file to migrate: {JSONL_PATH}", file=sys.stderr)
        return
    out_lines: list[str] = []
    n = 0
    for line in JSONL_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if "extraction_pass" not in obj:
            obj["extraction_pass"] = "legacy"
        if "confidence" not in obj:
            obj["confidence"] = 1.0
        out_lines.append(json.dumps(obj, ensure_ascii=False))
        n += 1
    tmp = JSONL_PATH.with_suffix(".jsonl.tmp")
    tmp.write_text("\n".join(out_lines) + ("\n" if out_lines else ""), encoding="utf-8")
    tmp.replace(JSONL_PATH)
    print(f"Migrated {n} rows → extraction_pass + confidence in {JSONL_PATH}", file=sys.stderr)


def resolve_book_key(match: str) -> str:
    """Resolve user substring to a key in BOOKS."""
    m = match.strip()
    if m in BOOKS:
        return m
    low = m.lower()
    hits = [k for k in BOOKS if low in k.lower()]
    if len(hits) == 1:
        return hits[0]
    if not hits:
        raise SystemExit(
            f"Unknown book {match!r}. Use a substring of the PDF filename from books/pdfs/."
        )
    raise SystemExit(f"Ambiguous book {match!r}. Matches:\n  " + "\n  ".join(hits))


def reextract_reset_progress(book_key: str, from_chunk_1based: int | None) -> None:
    """
    Drop book from completed list and resume extraction at a 1-based chunk number
    (same numbering as log lines 'Chunk 13'). Default 1 = whole book (may duplicate JSONL rows).
    """
    progress = load_progress()
    done = set(progress.get("extracted_books", []))
    done.discard(book_key)
    progress["extracted_books"] = sorted(done)
    bc = {k: int(v) for k, v in progress.get("book_chunk", {}).items()}
    start_1 = from_chunk_1based if from_chunk_1based is not None else 1
    bc[book_key] = max(0, start_1 - 1)
    progress["book_chunk"] = bc
    save_progress(progress)
    print(
        f"Re-extract scheduled: {book_key} starting at chunk {start_1} (of text chunks).",
        file=sys.stderr,
    )
    if start_1 == 1:
        print(
            "WARNING: Re-running from chunk 1 appends new quotes; remove old lines for this "
            "source from anti_school_quotes.jsonl first if you want to avoid duplicates.",
            file=sys.stderr,
        )


def reextract_recall_reset_progress(book_key: str, from_chunk_1based: int | None) -> None:
    """Drop book from recall completion and set recall resume chunk (1-based chunk index)."""
    progress = load_progress()
    done = set(progress.get("recall_extracted_books", []))
    done.discard(book_key)
    progress["recall_extracted_books"] = sorted(done)
    bc = {k: int(v) for k, v in progress.get("recall_book_chunk", {}).items()}
    start_1 = from_chunk_1based if from_chunk_1based is not None else 1
    bc[book_key] = max(0, start_1 - 1)
    progress["recall_book_chunk"] = bc
    save_progress(progress)
    print(
        f"Recall re-extract scheduled: {book_key} starting at chunk {start_1}.",
        file=sys.stderr,
    )


def normalize_entry(
    raw: dict[str, Any],
    author: str,
    source: str,
    quote_id: str,
    *,
    extraction_pass: str = "strict",
) -> dict[str, Any] | None:
    text = (raw.get("text") or "").strip()
    if len(text) < 20:
        return None
    narration = (raw.get("narration") or "").strip() or text
    t = raw.get("type")
    if t not in ("quote", "passage"):
        t = "quote" if len(text) < 350 else "passage"
    tone = raw.get("tone") or "critical"
    if tone not in ("provocative", "reflective", "critical"):
        tone = "critical"
    length = raw.get("length") or ("short" if t == "quote" else "medium")
    if length not in ("short", "medium"):
        length = "short" if t == "quote" else "medium"
    themes = raw.get("themes")
    if not isinstance(themes, list):
        themes = []
    themes = [str(x) for x in themes if x][:6]

    conf: float | None = None
    raw_conf = raw.get("confidence")
    if isinstance(raw_conf, (int, float)):
        conf = max(0.0, min(1.0, float(raw_conf)))
    if extraction_pass == "strict":
        conf = 1.0
    elif extraction_pass == "recall" and conf is None:
        conf = 0.5

    entry: dict[str, Any] = {
        "id": quote_id,
        "author": author,
        "source": source,
        "type": t,
        "text": text,
        "themes": themes,
        "tone": tone,
        "length": length,
        "narration": narration,
        "extraction_pass": extraction_pass,
        "confidence": conf,
    }
    if extraction_pass == "recall":
        rationale = (raw.get("recall_rationale") or raw.get("rationale") or "").strip()
        if rationale:
            entry["recall_rationale"] = rationale
    return entry


def run_extraction_phase(
    client,
    *,
    only_pdf: str | None = None,
    chunk_end_1based: int | None = None,
) -> None:
    progress = load_progress()
    done: set[str] = set(progress.get("extracted_books", []))
    book_chunk: dict[str, int] = {
        k: int(v) for k, v in progress.get("book_chunk", {}).items()
    }
    QUOTES_DIR.mkdir(parents=True, exist_ok=True)
    PARSED_DIR.mkdir(parents=True, exist_ok=True)

    # Next global id from existing jsonl
    next_id = 0
    if JSONL_PATH.exists():
        for line in JSONL_PATH.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                qid = str(obj.get("id", ""))
                if qid.startswith("q") and qid[1:].isdigit():
                    next_id = max(next_id, int(qid[1:]) + 1)
            except json.JSONDecodeError:
                continue

    mode = "a" if JSONL_PATH.exists() else "w"
    with JSONL_PATH.open(mode, encoding="utf-8") as out_f:
        for pdf_name, meta in BOOKS.items():
            if only_pdf is not None and pdf_name != only_pdf:
                continue
            if pdf_name in done:
                print(f"Skip (done): {pdf_name}", file=sys.stderr)
                continue
            author = meta["author"]
            source = meta["source"]
            try:
                full_text = load_or_parse_book(pdf_name)
            except Exception as e:
                print(f"ERROR parsing {pdf_name}: {e}", file=sys.stderr)
                continue
            chunks = chunk_by_words(full_text, CHUNK_WORDS)
            start_ci = book_chunk.get(pdf_name, 0)
            print(
                f"Extracting {pdf_name} ({len(chunks)} chunks, resume @ {start_ci}, "
                f"{len(full_text)} chars)",
                file=sys.stderr,
            )
            broken_out = False
            bounded_early_exit = False
            for ci, chunk in enumerate(chunks):
                if ci < start_ci:
                    continue
                if chunk_end_1based is not None and (ci + 1) > chunk_end_1based:
                    bounded_early_exit = True
                    break
                if not chunk.strip():
                    book_chunk[pdf_name] = ci + 1
                    progress["book_chunk"] = book_chunk
                    save_progress(progress)
                    continue
                user = textwrap.dedent(
                    f"""
                    AUTHOR: {author}
                    SOURCE: {source}
                    PDF_FILE: {pdf_name}

                    SOURCE TEXT (chunk {ci + 1}/{len(chunks)}):
                    ---
                    {chunk[:120000]}
                    ---
                    """
                )
                try:
                    data = call_chat_json_extraction(
                        client,
                        EXTRACTION_SYSTEM,
                        user,
                        max_tokens=8192,
                    )
                except Exception as e:
                    print(
                        f"  Chunk {ci + 1}/{len(chunks)} failed after retries: {e}",
                        file=sys.stderr,
                    )
                    book_chunk[pdf_name] = ci
                    progress["book_chunk"] = book_chunk
                    save_progress(progress)
                    broken_out = True
                    break
                quotes = data.get("quotes")
                if not isinstance(quotes, list):
                    quotes = []
                for raw in quotes:
                    if not isinstance(raw, dict):
                        continue
                    qid = f"q{next_id}"
                    next_id += 1
                    entry = normalize_entry(
                        raw, author, source, qid, extraction_pass="strict"
                    )
                    if entry:
                        out_f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                        out_f.flush()
                book_chunk[pdf_name] = ci + 1
                progress["book_chunk"] = book_chunk
                save_progress(progress)
                time.sleep(0.15)

            if not broken_out and not bounded_early_exit:
                done.add(pdf_name)
                book_chunk.pop(pdf_name, None)
            progress["extracted_books"] = sorted(done)
            progress["book_chunk"] = book_chunk
            save_progress(progress)

    print(f"Wrote {JSONL_PATH}", file=sys.stderr)


def run_recall_phase(
    client,
    *,
    only_pdf: str | None = None,
    chunk_end_1based: int | None = None,
    recall_max_per_chunk: int = RECALL_MAX_PER_CHUNK_DEFAULT,
) -> None:
    """Second pass: broad recall, append-only, deduped by quote_fingerprint."""
    progress = load_progress()
    done: set[str] = set(progress.get("recall_extracted_books", []))
    book_chunk: dict[str, int] = {
        k: int(v) for k, v in progress.get("recall_book_chunk", {}).items()
    }
    QUOTES_DIR.mkdir(parents=True, exist_ok=True)
    PARSED_DIR.mkdir(parents=True, exist_ok=True)

    existing_quotes = load_all_quotes()
    fp_seen = build_fingerprint_set(existing_quotes)

    next_id = 0
    if JSONL_PATH.exists():
        for line in JSONL_PATH.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                qid = str(obj.get("id", ""))
                if qid.startswith("q") and qid[1:].isdigit():
                    next_id = max(next_id, int(qid[1:]) + 1)
            except json.JSONDecodeError:
                continue

    mode = "a" if JSONL_PATH.exists() else "w"
    with JSONL_PATH.open(mode, encoding="utf-8") as out_f:
        for pdf_name, meta in BOOKS.items():
            if only_pdf is not None and pdf_name != only_pdf:
                continue
            if pdf_name in done:
                print(f"Skip recall (done): {pdf_name}", file=sys.stderr)
                continue
            author = meta["author"]
            source = meta["source"]
            try:
                full_text = load_or_parse_book(pdf_name)
            except Exception as e:
                print(f"ERROR parsing {pdf_name}: {e}", file=sys.stderr)
                continue
            chunks = chunk_by_words(full_text, CHUNK_WORDS)
            start_ci = book_chunk.get(pdf_name, 0)
            print(
                f"Recall {pdf_name} ({len(chunks)} chunks, resume @ {start_ci}, "
                f"max {recall_max_per_chunk}/chunk)",
                file=sys.stderr,
            )
            broken_out = False
            bounded_early_exit = False
            for ci, chunk in enumerate(chunks):
                if ci < start_ci:
                    continue
                if chunk_end_1based is not None and (ci + 1) > chunk_end_1based:
                    bounded_early_exit = True
                    break
                if not chunk.strip():
                    book_chunk[pdf_name] = ci + 1
                    progress["recall_book_chunk"] = book_chunk
                    save_progress(progress)
                    continue
                user = textwrap.dedent(
                    f"""
                    AUTHOR: {author}
                    SOURCE: {source}
                    PDF_FILE: {pdf_name}
                    MAX_QUOTES_FOR_THIS_CHUNK: {recall_max_per_chunk}

                    SOURCE TEXT (chunk {ci + 1}/{len(chunks)}):
                    ---
                    {chunk[:120000]}
                    ---
                    """
                )
                try:
                    data = call_chat_json_extraction(
                        client,
                        EXTRACTION_RECALL_SYSTEM,
                        user,
                        max_tokens=16384,
                    )
                except Exception as e:
                    print(
                        f"  Recall chunk {ci + 1}/{len(chunks)} failed after retries: {e}",
                        file=sys.stderr,
                    )
                    book_chunk[pdf_name] = ci
                    progress["recall_book_chunk"] = book_chunk
                    save_progress(progress)
                    broken_out = True
                    break
                quotes = data.get("quotes")
                if not isinstance(quotes, list):
                    quotes = []
                for raw in quotes:
                    if not isinstance(raw, dict):
                        continue
                    entry_try = normalize_entry(
                        raw, author, source, "_", extraction_pass="recall"
                    )
                    if not entry_try:
                        continue
                    fp = quote_fingerprint(entry_try["text"])
                    if not fp or fp in fp_seen:
                        continue
                    fp_seen.add(fp)
                    qid = f"q{next_id}"
                    next_id += 1
                    entry_try["id"] = qid
                    out_f.write(json.dumps(entry_try, ensure_ascii=False) + "\n")
                    out_f.flush()
                book_chunk[pdf_name] = ci + 1
                progress["recall_book_chunk"] = book_chunk
                save_progress(progress)
                time.sleep(0.15)

            if not broken_out and not bounded_early_exit:
                done.add(pdf_name)
                book_chunk.pop(pdf_name, None)
            progress["recall_extracted_books"] = sorted(done)
            progress["recall_book_chunk"] = book_chunk
            save_progress(progress)

    print(f"Recall pass appended to {JSONL_PATH}", file=sys.stderr)


SCORE_EXISTING_SYSTEM = """You score existing quote lines for an anti-school narration project (see values: critique compulsory schooling; no rehabilitating school as necessary).

Input: JSON array of { "id", "text" }.

For each id, output confidence 0.0–1.0 (narration fit + alignment with anti-school use) and intent_fit: one of "high", "medium", "low".

Output JSON only: {"scores": [ {"id", "confidence", "intent_fit"}, ... ]}"""


def run_score_existing_phase(client) -> None:
    """Add confidence + intent_fit to rows missing intent_fit; does not change text/narration."""
    quotes = load_all_quotes()
    if not quotes:
        print("No quotes to score.", file=sys.stderr)
        return
    to_score = [q for q in quotes if "intent_fit" not in q]
    if not to_score:
        print("All quotes already have intent_fit.", file=sys.stderr)
        return

    by_id: dict[str, dict[str, Any]] = {}
    for q in quotes:
        i = q.get("id")
        if i is not None:
            by_id[str(i)] = q

    batch_size = 35
    for start in range(0, len(to_score), batch_size):
        batch = to_score[start : start + batch_size]
        payload = json.dumps(
            [{"id": str(q.get("id", "")), "text": (q.get("text") or "")[:1200]} for q in batch],
            ensure_ascii=False,
        )
        try:
            data = call_chat_json(
                client,
                SCORE_EXISTING_SYSTEM,
                "QUOTES:\n" + payload,
                max_tokens=4096,
            )
        except Exception as e:
            print(f"Score batch API error: {e}", file=sys.stderr)
            continue
        scores = data.get("scores")
        if not isinstance(scores, list):
            continue
        for row in scores:
            if not isinstance(row, dict):
                continue
            qid = str(row.get("id", ""))
            if qid not in by_id:
                continue
            c = row.get("confidence")
            if isinstance(c, (int, float)):
                by_id[qid]["confidence"] = max(0.0, min(1.0, float(c)))
            fit = row.get("intent_fit")
            if fit in ("high", "medium", "low"):
                by_id[qid]["intent_fit"] = fit
        time.sleep(0.2)

    tmp = JSONL_PATH.with_suffix(".jsonl.tmp")
    tmp.write_text(
        "\n".join(json.dumps(q, ensure_ascii=False) for q in quotes) + "\n",
        encoding="utf-8",
    )
    tmp.replace(JSONL_PATH)
    print(f"Updated intent_fit/confidence for scored rows in {JSONL_PATH}", file=sys.stderr)


def load_all_quotes() -> list[dict[str, Any]]:
    if not JSONL_PATH.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in JSONL_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def run_segments_phase(client) -> None:
    quotes = load_all_quotes()
    if not quotes:
        print("No quotes in JSONL; skip youtube_segments.json", file=sys.stderr)
        SEGMENTS_PATH.write_text("[]", encoding="utf-8")
        return

    def compact_row(q: dict[str, Any], max_text: int = 400) -> dict[str, str]:
        return {
            "id": str(q.get("id", "")),
            "author": q.get("author", ""),
            "source": q.get("source", ""),
            "text": (q.get("text", "") or "")[:max_text],
        }

    batch_size = 90
    per_batch_maps: list[dict[str, list[str]]] = []

    for start in range(0, len(quotes), batch_size):
        batch = [compact_row(q) for q in quotes[start : start + batch_size]]
        payload = json.dumps(batch, ensure_ascii=False)
        user = f"BATCH {start // batch_size + 1} (size {len(batch)})\nQUOTES_JSON:\n{payload}"
        try:
            data = call_chat_json(
                client,
                SEGMENTS_SYSTEM_BATCH,
                user,
                max_tokens=8192,
            )
        except Exception as e:
            print(f"Segment batch API error: {e}", file=sys.stderr)
            continue
        segs = data.get("segments")
        if not isinstance(segs, list):
            continue
        m: dict[str, list[str]] = {}
        for seg in segs:
            if not isinstance(seg, dict):
                continue
            title = str(seg.get("title", "")).strip() or "Uncategorized"
            qids = seg.get("quote_ids")
            if not isinstance(qids, list):
                continue
            clean_ids = [str(x) for x in qids if x]
            m[title] = clean_ids
        if m:
            per_batch_maps.append(m)
        time.sleep(0.2)

    merged = merge_segment_maps(per_batch_maps)
    QUOTES_DIR.mkdir(parents=True, exist_ok=True)
    SEGMENTS_PATH.write_text(
        json.dumps(merged, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"Wrote {SEGMENTS_PATH}", file=sys.stderr)


def run_markdown_phase() -> None:
    quotes = load_all_quotes()
    QUOTES_DIR.mkdir(parents=True, exist_ok=True)
    lines: list[str] = ["# Anti-school quotes (extracted)", ""]

    def sort_key(q: dict[str, Any]) -> tuple[str, str]:
        return (q.get("author", ""), q.get("source", ""))

    quotes.sort(key=sort_key)
    current: tuple[str, str] | None = None
    for q in quotes:
        k = sort_key(q)
        if k != current:
            current = k
            author, source = k
            lines.append(f"## {author} — *{source}*")
            lines.append("")
        meta_bits = [
            str(q.get("type", "")),
            str(q.get("tone", "")),
            str(q.get("length", "")),
        ]
        if q.get("extraction_pass"):
            meta_bits.append(f"pass={q.get('extraction_pass')}")
        if q.get("confidence") is not None:
            meta_bits.append(f"conf={q.get('confidence')}")
        if q.get("intent_fit"):
            meta_bits.append(f"intent={q.get('intent_fit')}")
        lines.append(f"**[{' | '.join(meta_bits)}]**")
        lines.append("")
        lines.append(f"> {q.get('text', '')}")
        lines.append("")
        lines.append(f"*Narration:* {q.get('narration', '')}")
        lines.append("")
        if q.get("themes"):
            lines.append(f"*Themes:* {', '.join(q['themes'])}")
            lines.append("")
        if q.get("recall_rationale"):
            lines.append(f"*Recall note:* {q.get('recall_rationale')}")
            lines.append("")
        lines.append("---")
        lines.append("")
    MD_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {MD_PATH}", file=sys.stderr)


def run_parse_only() -> None:
    """Extract text from all known PDFs into books/parsed/ (no OpenAI)."""
    ensure_pdfplumber()
    PARSED_DIR.mkdir(parents=True, exist_ok=True)
    missing = [n for n in BOOKS if not (PDF_DIR / n).exists()]
    if missing:
        print("WARNING: Missing PDFs (skipped):", file=sys.stderr)
        for m in missing:
            print(f"  {m}", file=sys.stderr)
    for pdf_name in BOOKS:
        if not (PDF_DIR / pdf_name).exists():
            continue
        try:
            load_or_parse_book(pdf_name)
            print(f"Parsed OK: {pdf_name}", file=sys.stderr)
        except Exception as e:
            print(f"ERROR: {pdf_name}: {e}", file=sys.stderr)


def main() -> None:
    load_env_from_dotenv()

    ap = argparse.ArgumentParser(description="Anti-school quote extraction pipeline")
    ap.add_argument(
        "--parse-only",
        action="store_true",
        help="Only run pdfplumber → books/parsed/*.txt (no OpenAI)",
    )
    ap.add_argument(
        "--migrate-schema",
        action="store_true",
        help="Add extraction_pass + confidence to existing JSONL rows (no API); then exit",
    )
    ap.add_argument(
        "--recall-only",
        action="store_true",
        help="Run only the broad recall pass (+ segments + md), not strict extraction",
    )
    ap.add_argument(
        "--with-recall",
        action="store_true",
        help="After strict extraction, also run broad recall pass",
    )
    ap.add_argument(
        "--recall-max-per-chunk",
        type=int,
        default=RECALL_MAX_PER_CHUNK_DEFAULT,
        metavar="N",
        help=f"Max recall quotes per text chunk (default {RECALL_MAX_PER_CHUNK_DEFAULT})",
    )
    ap.add_argument(
        "--score-existing",
        action="store_true",
        help="LLM-score quotes missing intent_fit; updates JSONL only (+ segments + md)",
    )
    ap.add_argument(
        "--reextract-book",
        metavar="MATCH",
        help=(
            "Substring of a PDF filename in BOOKS to run again (updates progress.json). "
            "Pair with --from-chunk to resume only failed chunks."
        ),
    )
    ap.add_argument(
        "--for-recall",
        action="store_true",
        help="With --reextract-book, reset recall progress instead of strict extraction progress",
    )
    ap.add_argument(
        "--from-chunk",
        type=int,
        default=None,
        metavar="N",
        help="1-based chunk index to resume (same as 'Chunk N' in logs). Default 1 with --reextract-book.",
    )
    ap.add_argument(
        "--to-chunk",
        type=int,
        default=None,
        metavar="N",
        help=(
            "1-based last chunk to process (inclusive). Requires --reextract-book. "
            "Example: --from-chunk 13 --to-chunk 13 fills only that chunk."
        ),
    )
    args = ap.parse_args()

    if args.migrate_schema:
        QUOTES_DIR.mkdir(parents=True, exist_ok=True)
        migrate_schema_jsonl()
        return

    if args.parse_only and args.reextract_book:
        print("Cannot combine --parse-only and --reextract-book.", file=sys.stderr)
        sys.exit(2)
    if args.parse_only and args.recall_only:
        print("Cannot combine --parse-only and --recall-only.", file=sys.stderr)
        sys.exit(2)
    if args.recall_only and args.with_recall:
        print("Use either --recall-only or --with-recall, not both.", file=sys.stderr)
        sys.exit(2)
    if args.for_recall and not args.reextract_book:
        print("--for-recall requires --reextract-book.", file=sys.stderr)
        sys.exit(2)
    if args.score_existing and (
        args.recall_only or args.with_recall or args.reextract_book
    ):
        print(
            "Use --score-existing alone (it regenerates segments + md).",
            file=sys.stderr,
        )
        sys.exit(2)

    if args.parse_only:
        run_parse_only()
        return

    only_pdf: str | None = None
    chunk_end_1based: int | None = None
    if args.to_chunk is not None and not args.reextract_book:
        print("--to-chunk requires --reextract-book.", file=sys.stderr)
        sys.exit(2)
    if args.reextract_book:
        key = resolve_book_key(args.reextract_book)
        if args.for_recall:
            reextract_recall_reset_progress(key, args.from_chunk)
        else:
            reextract_reset_progress(key, args.from_chunk)
        only_pdf = key
    if args.to_chunk is not None:
        chunk_end_1based = args.to_chunk

    ensure_pdfplumber()
    client = get_openai_client()
    QUOTES_DIR.mkdir(parents=True, exist_ok=True)

    if args.score_existing:
        run_score_existing_phase(client)
        run_segments_phase(client)
        run_markdown_phase()
        return

    missing = [n for n in BOOKS if not (PDF_DIR / n).exists()]
    if missing:
        print("WARNING: Missing PDFs (skipped):", file=sys.stderr)
        for m in missing:
            print(f"  {m}", file=sys.stderr)

    if args.recall_only:
        run_recall_phase(
            client,
            only_pdf=only_pdf,
            chunk_end_1based=chunk_end_1based,
            recall_max_per_chunk=max(1, args.recall_max_per_chunk),
        )
    else:
        run_extraction_phase(
            client,
            only_pdf=only_pdf,
            chunk_end_1based=chunk_end_1based,
        )
        if args.with_recall:
            run_recall_phase(
                client,
                only_pdf=only_pdf,
                chunk_end_1based=chunk_end_1based,
                recall_max_per_chunk=max(1, args.recall_max_per_chunk),
            )

    run_segments_phase(client)
    run_markdown_phase()


if __name__ == "__main__":
    main()
