"""
Backfill script for claude-memory-compiler.

Processes historical Claude Code JSONL transcripts into the knowledge base.

Usage:
    uv run python scripts/backfill.py extract
    uv run python scripts/backfill.py flush --dates 2026-03-09,2026-03-10,...
    uv run python scripts/backfill.py compile --dates 2026-03-09,2026-03-10,...
"""

from __future__ import annotations

import os
os.environ["CLAUDE_INVOKED_BY"] = "memory_backfill"

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DAILY_DIR = ROOT / "daily"
SCRIPTS_DIR = ROOT / "scripts"
BACKFILL_DIR = ROOT / "backfill-context"

TRANSCRIPT_DIR = Path.home() / ".claude" / "projects" / "-Users-lovabo--openclaw"

logging.basicConfig(
    filename=str(SCRIPTS_DIR / "backfill.log"),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# Also log to stderr for real-time progress
console = logging.StreamHandler(sys.stderr)
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%H:%M:%S"))
logging.getLogger().addHandler(console)


# ── Extraction (reused from session-end.py) ───────────────────────────

MAX_TURNS = 30
MAX_CONTEXT_CHARS = 15_000


def extract_conversation_context(transcript_path: Path) -> tuple[str, int]:
    """Read JSONL transcript and extract last ~N conversation turns as markdown."""
    turns: list[str] = []

    with open(transcript_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue

            msg = entry.get("message", {})
            if isinstance(msg, dict):
                role = msg.get("role", "")
                content = msg.get("content", "")
            else:
                role = entry.get("role", "")
                content = entry.get("content", "")

            if role not in ("user", "assistant"):
                continue

            if isinstance(content, list):
                text_parts = []
                for block in content:
                    if isinstance(block, dict) and block.get("type") == "text":
                        text_parts.append(block.get("text", ""))
                    elif isinstance(block, str):
                        text_parts.append(block)
                content = "\n".join(text_parts)

            if isinstance(content, str) and content.strip():
                label = "User" if role == "user" else "Assistant"
                turns.append(f"**{label}:** {content.strip()}\n")

    recent = turns[-MAX_TURNS:]
    context = "\n".join(recent)

    if len(context) > MAX_CONTEXT_CHARS:
        context = context[-MAX_CONTEXT_CHARS:]
        boundary = context.find("\n**")
        if boundary > 0:
            context = context[boundary + 1:]

    return context, len(recent)


def get_session_date(path: Path) -> str:
    """Get session date from file modification time."""
    mtime = path.stat().st_mtime
    dt = datetime.fromtimestamp(mtime, tz=timezone.utc).astimezone()
    return dt.strftime("%Y-%m-%d")


# ── Phase 1: Extract ──────────────────────────────────────────────────

def cmd_extract():
    """Extract contexts from all JSONL transcripts and group by date."""
    BACKFILL_DIR.mkdir(parents=True, exist_ok=True)

    # Find all top-level JSONL files (not subagent transcripts)
    jsonl_files = sorted(TRANSCRIPT_DIR.glob("*.jsonl"))
    logging.info("Found %d top-level JSONL transcripts", len(jsonl_files))

    # Group by date
    by_date: dict[str, list[Path]] = {}
    for f in jsonl_files:
        date = get_session_date(f)
        by_date.setdefault(date, []).append(f)

    logging.info("Sessions span %d days: %s to %s",
                 len(by_date), min(by_date.keys()), max(by_date.keys()))

    # Extract context for each date
    total_sessions = 0
    skipped = 0
    for date in sorted(by_date.keys()):
        sessions = by_date[date]
        combined_parts = []

        for session_path in sessions:
            try:
                context, turn_count = extract_conversation_context(session_path)
                if context.strip() and turn_count >= 1:
                    session_id = session_path.stem
                    combined_parts.append(
                        f"---\n\n## Session {session_id[:8]} ({turn_count} turns)\n\n{context}"
                    )
                    total_sessions += 1
                else:
                    skipped += 1
            except Exception as e:
                logging.warning("Failed to extract %s: %s", session_path.name, e)
                skipped += 1

        if combined_parts:
            context_file = BACKFILL_DIR / f"{date}.md"
            header = f"# Backfill Context: {date}\n\n{len(combined_parts)} sessions\n\n"
            context_file.write_text(header + "\n".join(combined_parts), encoding="utf-8")

    logging.info("Extraction complete: %d sessions extracted, %d skipped, %d date files written",
                 total_sessions, skipped, len(list(BACKFILL_DIR.glob("*.md"))))

    # Print summary for the agent
    print(f"\nExtraction complete:")
    print(f"  Sessions: {total_sessions} extracted, {skipped} skipped")
    print(f"  Date files: {len(list(BACKFILL_DIR.glob('*.md')))}")
    print(f"  Output: {BACKFILL_DIR}/")
    print(f"\nDates available:")
    for date in sorted(by_date.keys()):
        ctx_file = BACKFILL_DIR / f"{date}.md"
        size = ctx_file.stat().st_size if ctx_file.exists() else 0
        print(f"  {date}: {len(by_date[date])} sessions, {size // 1024}KB context")


# ── Phase 2: Flush ────────────────────────────────────────────────────

def append_to_daily_log_for_date(content: str, date_str: str, section: str = "Session") -> None:
    """Append content to a specific date's daily log (NOT today)."""
    log_path = DAILY_DIR / f"{date_str}.md"

    if not log_path.exists():
        DAILY_DIR.mkdir(parents=True, exist_ok=True)
        log_path.write_text(
            f"# Daily Log: {date_str}\n\n## Sessions\n\n## Memory Maintenance\n\n",
            encoding="utf-8",
        )

    entry = f"### {section} (backfill)\n\n{content}\n\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(entry)


async def run_flush(context: str) -> str:
    """Use Claude Agent SDK to extract knowledge from conversation context."""
    from claude_agent_sdk import (
        AssistantMessage,
        ClaudeAgentOptions,
        ResultMessage,
        TextBlock,
        query,
    )

    prompt = f"""Review the conversation context below and respond with a concise summary
of important items that should be preserved in the daily log.
Do NOT use any tools — just return plain text.

Format your response as a structured daily log entry with these sections:

**Context:** [One line about what the user was working on]

**Key Exchanges:**
- [Important Q&A or discussions]

**Decisions Made:**
- [Any decisions with rationale]

**Lessons Learned:**
- [Gotchas, patterns, or insights discovered]

**Action Items:**
- [Follow-ups or TODOs mentioned]

Skip anything that is:
- Routine tool calls or file reads
- Content that's trivial or obvious
- Trivial back-and-forth or clarification exchanges

Only include sections that have actual content. If nothing is worth saving,
respond with exactly: FLUSH_OK

## Conversation Context

{context}"""

    response = ""
    try:
        async for message in query(
            prompt=prompt,
            options=ClaudeAgentOptions(
                cwd=str(ROOT),
                allowed_tools=[],
                max_turns=2,
            ),
        ):
            if isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, TextBlock):
                        response += block.text
            elif isinstance(message, ResultMessage):
                pass
    except Exception as e:
        logging.error("Agent SDK error for flush: %s", e)
        response = f"FLUSH_ERROR: {type(e).__name__}: {e}"

    return response


def cmd_flush(dates: list[str]):
    """Flush extracted contexts through the Agent SDK to produce daily logs."""
    DAILY_DIR.mkdir(parents=True, exist_ok=True)

    for i, date_str in enumerate(dates, 1):
        context_file = BACKFILL_DIR / f"{date_str}.md"
        if not context_file.exists():
            logging.warning("[%d/%d] No context file for %s, skipping", i, len(dates), date_str)
            continue

        # Skip if daily log already exists (already backfilled)
        daily_log = DAILY_DIR / f"{date_str}.md"
        if daily_log.exists():
            logging.info("[%d/%d] %s already has daily log, skipping", i, len(dates), date_str)
            continue

        context = context_file.read_text(encoding="utf-8")
        logging.info("[%d/%d] Flushing %s (%d chars)", i, len(dates), date_str, len(context))

        response = asyncio.run(run_flush(context))

        if "FLUSH_OK" in response:
            logging.info("[%d/%d] %s: FLUSH_OK", i, len(dates), date_str)
            append_to_daily_log_for_date(
                "FLUSH_OK - Nothing notable from these sessions", date_str, "Backfill"
            )
        elif "FLUSH_ERROR" in response:
            logging.error("[%d/%d] %s: %s", i, len(dates), date_str, response)
            append_to_daily_log_for_date(response, date_str, "Backfill Error")
        else:
            logging.info("[%d/%d] %s: saved (%d chars)", i, len(dates), date_str, len(response))
            append_to_daily_log_for_date(response, date_str, "Backfill")

    logging.info("Flush complete for %d dates", len(dates))
    print(f"\nFlush complete for {len(dates)} dates")


# ── Phase 3: Compile ──────────────────────────────────────────────────

def cmd_compile(dates: list[str]):
    """Compile specific daily logs into knowledge articles."""
    import subprocess

    for i, date_str in enumerate(dates, 1):
        daily_log = DAILY_DIR / f"{date_str}.md"
        if not daily_log.exists():
            logging.warning("[%d/%d] No daily log for %s, skipping", i, len(dates), date_str)
            continue

        logging.info("[%d/%d] Compiling %s", i, len(dates), date_str)
        result = subprocess.run(
            ["uv", "run", "--directory", str(ROOT), "python", "scripts/compile.py",
             "--file", f"daily/{date_str}.md"],
            capture_output=True, text=True, cwd=str(ROOT),
            env={**os.environ, "CLAUDE_INVOKED_BY": "memory_backfill"},
        )
        if result.returncode != 0:
            logging.error("[%d/%d] compile.py failed for %s: %s", i, len(dates), date_str, result.stderr)
        else:
            logging.info("[%d/%d] %s compiled", i, len(dates), date_str)
            if result.stdout.strip():
                print(result.stdout.strip())

    logging.info("Compilation complete for %d dates", len(dates))
    print(f"\nCompilation complete for {len(dates)} dates")


# ── CLI ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Backfill memory compiler knowledge base")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("extract", help="Extract contexts from JSONL transcripts")

    flush_p = sub.add_parser("flush", help="Flush contexts through Agent SDK")
    flush_p.add_argument("--dates", required=True, help="Comma-separated dates (YYYY-MM-DD)")

    compile_p = sub.add_parser("compile", help="Compile daily logs into knowledge articles")
    compile_p.add_argument("--dates", required=True, help="Comma-separated dates (YYYY-MM-DD)")

    args = parser.parse_args()

    if args.command == "extract":
        cmd_extract()
    elif args.command == "flush":
        dates = [d.strip() for d in args.dates.split(",")]
        cmd_flush(dates)
    elif args.command == "compile":
        dates = [d.strip() for d in args.dates.split(",")]
        cmd_compile(dates)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
