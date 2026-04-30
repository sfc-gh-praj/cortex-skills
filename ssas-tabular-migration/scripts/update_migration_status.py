#!/usr/bin/env python3
"""
update_migration_status.py - Update or read a phase row in migration_status.md.

Called at the end of each migration phase to record status, Snowflake objects
created, and a ballpark token estimate. Safe to call multiple times (idempotent).

Usage — update a phase:
    uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/update_migration_status.py \
        --phase "Phase 4" \
        --status completed \
        [--status-file ./migration_status.md] \
        [--objects "CREATE TABLE FactSales, CREATE INTERACTIVE TABLE DimDate"] \
        [--tokens 0] \
        [--notes "12 tables: 2 interactive, 9 regular, 1 view"]

Usage — read current state (for resume detection):
    uv run --project <SKILL_DIR> python <SKILL_DIR>/scripts/update_migration_status.py \
        --status-file ./migration_status.md \
        --read
    # Outputs JSON to stdout:
    # {"last_completed": 3, "in_progress": null, "pending": [4,5,6,7],
    #  "resume_from": 4, "summary": "Phases 1-3 complete - resume from Phase 4"}
"""

import argparse
import json
import re
from pathlib import Path

STATUS_ICONS = {
    "completed":   "✅ Completed",
    "in_progress": "🔄 In Progress",
    "pending":     "⏳ Pending",
    "skipped":     "⏭ Skipped",
    "failed":      "❌ Failed",
}


def _parse_table_row(line: str) -> list:
    """
    Split a markdown table row into a list of cell strings.
    Returns an empty list if the line is not a data row.
    """
    stripped = line.strip()
    if not stripped.startswith("|") or not stripped.endswith("|"):
        return []
    inner = stripped[1:-1]
    return [c.strip() for c in inner.split("|")]


def _build_table_row(cols: list) -> str:
    return "| " + " | ".join(cols) + " |"


def update_status_file(
    status_file: Path,
    phase_prefix: str,
    new_status: str,
    objects: str,
    tokens: int,
    notes: str,
) -> bool:
    """
    Find the row whose first cell starts with phase_prefix (e.g. "Phase 4")
    and update Status, Objects, Tokens, Notes columns in-place.

    Returns True if a matching row was found and updated.
    """
    content = status_file.read_text(encoding="utf-8")
    lines = content.splitlines()

    icon = STATUS_ICONS.get(new_status.lower(), new_status)
    pattern = re.compile(r"^\|\s*" + re.escape(phase_prefix) + r"\b")
    updated = False

    for i, line in enumerate(lines):
        if not pattern.match(line):
            continue
        cols = _parse_table_row(line)
        # Expected: [Phase, Status, Objects, Tokens, Notes]
        if len(cols) < 5:
            continue
        cols[1] = icon
        if objects is not None:
            cols[2] = objects
        if tokens is not None:
            cols[3] = str(tokens)
        if notes is not None:
            cols[4] = notes
        lines[i] = _build_table_row(cols)
        updated = True
        break

    if updated:
        status_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return updated


def read_status_file(status_file: Path) -> dict:
    """
    Parse migration_status.md and return phase completion state.

    Returns:
    {
        "last_completed": <int or None>,
        "in_progress": <int or None>,
        "pending": [<int>, ...],
        "resume_from": <int>,
        "summary": "<string>",
        "phases": [{"number": N, "name": "...", "status": "completed|..."}, ...]
    }
    """
    content = status_file.read_text(encoding="utf-8")
    lines = content.splitlines()

    # Reverse icon lookup: icon string → canonical status key
    icon_to_status = {v: k for k, v in STATUS_ICONS.items()}

    phases = []
    for line in lines:
        cols = _parse_table_row(line)
        if len(cols) < 2:
            continue
        phase_cell  = cols[0].strip()
        status_cell = cols[1].strip()

        # Extract phase number from "Phase N — Name" or "Phase N"
        m = re.match(r"Phase\s+(\d+)", phase_cell, re.I)
        if not m:
            continue

        phase_num = int(m.group(1))

        # Map status cell to canonical status key
        status = "unknown"
        for icon, sname in icon_to_status.items():
            if status_cell.startswith(icon) or status_cell == icon:
                status = sname
                break

        phases.append({"number": phase_num, "name": phase_cell, "status": status})

    if not phases:
        return {
            "last_completed": None,
            "in_progress": None,
            "pending": [],
            "resume_from": 1,
            "summary": "No phases found in status file — start from Phase 1",
            "phases": [],
        }

    completed    = [p["number"] for p in phases if p["status"] == "completed"]
    in_progress  = [p["number"] for p in phases if p["status"] == "in_progress"]
    pending      = [p["number"] for p in phases
                    if p["status"] in ("pending", "skipped", "failed", "unknown")]

    last_completed = max(completed) if completed else None
    in_prog_num    = in_progress[0] if in_progress else None

    # resume_from: first in_progress → first pending → last_completed+1 → 1
    if in_prog_num:
        resume_from = in_prog_num
    elif pending:
        resume_from = min(pending)
    elif last_completed:
        resume_from = last_completed + 1
    else:
        resume_from = 1

    if in_prog_num:
        summary = (
            f"Phase {in_prog_num} is in progress — resume from Phase {resume_from}"
        )
    elif last_completed:
        # Build a compact range like "1–3"
        if last_completed > 1:
            summary = f"Phases 1–{last_completed} complete — resume from Phase {resume_from}"
        else:
            summary = f"Phase 1 complete — resume from Phase {resume_from}"
    else:
        summary = "No phases complete — start from Phase 1"

    return {
        "last_completed": last_completed,
        "in_progress":    in_prog_num,
        "pending":        pending,
        "resume_from":    resume_from,
        "summary":        summary,
        "phases":         phases,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Update a phase row in migration_status.md, or read phase state"
    )
    parser.add_argument(
        "--status-file", default="./migration_status.md",
        help="Path to migration_status.md (default: ./migration_status.md)",
    )
    # --- Read mode ---
    parser.add_argument(
        "--read", action="store_true",
        help="Read and return current phase state as JSON (no update performed)",
    )
    # --- Update mode args (required when not using --read) ---
    parser.add_argument(
        "--phase", default=None,
        help="Phase prefix to match, e.g. 'Phase 4' (required when not using --read)",
    )
    parser.add_argument(
        "--status", default=None,
        choices=["completed", "in_progress", "pending", "skipped", "failed"],
        help="New status for this phase (required when not using --read)",
    )
    parser.add_argument(
        "--objects", default=None,
        help="Comma-separated Snowflake objects created in this phase (optional)",
    )
    parser.add_argument(
        "--tokens", type=int, default=None,
        help="Ballpark token count for this phase (optional; estimate only)",
    )
    parser.add_argument(
        "--notes", default=None,
        help="Short free-text note to record for this phase (optional)",
    )
    args = parser.parse_args()

    status_path = Path(args.status_file)

    # -----------------------------------------------------------------------
    # --read mode: output JSON state to stdout
    # -----------------------------------------------------------------------
    if args.read:
        if not status_path.exists():
            result = {
                "last_completed": None,
                "in_progress": None,
                "pending": [],
                "resume_from": 1,
                "summary": f"{status_path} not found — start from Phase 1",
                "phases": [],
            }
            print(json.dumps(result, indent=2))
            return

        result = read_status_file(status_path)
        print(json.dumps(result, indent=2))

        # Also print a human-readable banner to stderr
        import sys
        print(f"\n{result['summary']}", file=sys.stderr)
        phase_symbols = {"completed": "✅", "in_progress": "◐", "pending": "⬚",
                         "skipped": "⏭", "failed": "❌", "unknown": "⬚"}
        for p in result["phases"]:
            sym = phase_symbols.get(p["status"], "⬚")
            print(f"  {sym}  {p['name']}", file=sys.stderr)
        return

    # -----------------------------------------------------------------------
    # --update mode: validate required args then update
    # -----------------------------------------------------------------------
    if not args.phase:
        parser.error("--phase is required when not using --read")
    if not args.status:
        parser.error("--status is required when not using --read")

    if not status_path.exists():
        print(f"ERROR: {status_path} not found.")
        print("Run generate_migration_plan.py first to create the status file.")
        raise SystemExit(1)

    found = update_status_file(
        status_path,
        args.phase,
        args.status,
        args.objects,
        args.tokens,
        args.notes,
    )

    if found:
        icon = STATUS_ICONS.get(args.status.lower(), args.status)
        print(f"Updated {args.phase} → {icon}")
        if args.objects:
            preview = args.objects[:80] + ("..." if len(args.objects) > 80 else "")
            print(f"  Objects: {preview}")
        if args.tokens is not None:
            print(f"  Tokens:  {args.tokens:,}  (estimate only — not actual usage)")
        if args.notes:
            print(f"  Notes:   {args.notes}")
        print(f"  Status file: {status_path.resolve()}")
    else:
        print(f"WARNING: No row matching '{args.phase}' found in {status_path}")
        print("Ensure the phase name matches exactly, e.g. 'Phase 4' (not 'phase 4' or 'Phase 4 — Schema DDL')")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
