# Agent / AI notes

- **Project values and narration substance:** `INTENT_AND_VALUES.md`, `.cursor/rules/anti-school-narrations.mdc`
- **Voiceover file shape (VO vs appendix, cues, template):** `narrations/README.md`, `.cursor/rules/narration-voiceover-format.mdc`
- **VO rhythm (no academic paragraph dumps; stagger quotes; layer research beats):** `.cursor/rules/narration-voiceover-format.mdc` → section **“VO rhythm and research blocks”** — applies when editing **any** narration markdown, including legacy `formatted/` scripts without `## VOICEOVER`.
- **New narration skeleton:** `narrations/TEMPLATE_voiceover.md`
- **Structural reference for timestamps and cues:** `narrations/new/formatted/school_profits_formatted.md`
- **Format check:** Project hook in **`.cursor/hooks.json`** runs after Agent/Tab edits when the file contains `## VOICEOVER` (see Hooks output). Manual: `python scripts/check_narration_format.py <file.md>`

Do not run `git commit` / `git push` unless the user asks. Work on one narration at a time under `narrations/`.
