# Narrations

Anti-school mini-documentary scripts and research live here. Substance and values: root **`INTENT_AND_VALUES.md`** and **`.cursor/rules/anti-school-narrations.mdc`**.

## Voiceover deliverable

For **new** narration drafts, use a clear split:

| Section | Purpose |
|--------|---------|
| **`## VOICEOVER`** | Text for recording only: script, quotes, inline citations. No raw URLs/DOIs or large tables. |
| **`## SOURCES AND PRODUCER NOTES`** | Links, DOIs, fact-check tables, delivery notes. |

Start from **`TEMPLATE_voiceover.md`**. For structure and production cues (timestamps, `[VOICE]` / `[PAUSE]` / `[B-ROLL]`), the reference example is **`new/formatted/school_profits_formatted.md`**.

When **adding studies or quotes** to any script, follow **VO rhythm and research blocks** in **`.cursor/rules/narration-voiceover-format.mdc`**: keep short-line pacing, layer attribution → quote → narrator bridge, and do not drop academic paragraph dumps into the VO.

Cursor applies **`.cursor/rules/narration-voiceover-format.mdc`** when you work on files under `narrations/**/*.md`.

## Automatic format check (Cursor hooks)

**`.cursor/hooks.json`** runs the checker after **Agent** or **Tab** saves an edit to a narration `.md` file, **but only if the file already contains `## VOICEOVER`**. That avoids noise on older scripts that do not use the two-part layout yet. Watch **Hooks** in the Output panel for results.

Requires Cursor hooks to be enabled (see [Hooks docs](https://cursor.com/docs/agent/hooks)).

## Manual format check

From the repo root:

```bash
python scripts/check_narration_format.py path/to/your_draft.md
```

Use this on **new** drafts that use the `## VOICEOVER` / `## SOURCES…` split. Older files (including some under `formatted/`) may not pass until you add those headings—they can still be the reference for pacing and cues.

## Workflow reminder

One narration at a time until complete (see project rules).
