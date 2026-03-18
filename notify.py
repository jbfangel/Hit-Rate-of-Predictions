"""
Read the run report from scraper_auto.py and:
  - Write a markdown summary to $GITHUB_STEP_SUMMARY
  - Exit with code 1 if there are warnings (triggers GitHub failure email)

Usage:
    python notify.py
"""

import json
import os
import re
import sqlite3
import sys
from datetime import date, datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent / "data" / "predictions.db"
REPORT_PATH = Path(__file__).parent / "data" / "run_report.json"
PENDING_WARNING_DAYS = 5  # warn if a prediction has been pending this many days


def main():
    if not REPORT_PATH.exists():
        print("No report file found.", file=sys.stderr)
        sys.exit(1)

    report = json.loads(REPORT_PATH.read_text(encoding="utf-8"))
    results_output = report.get("results_output", "")

    # Parse results.py output
    matched = re.findall(r"Found: (.+?) \[", results_output)
    warnings = re.findall(r"\[WARN\].+", results_output)
    errors = re.findall(r"ERROR.+", results_output)

    # Find long-pending predictions from DB
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        """
        SELECT race_name, date, url FROM predictions
        WHERE actual_winner IS NULL
          AND cancelled IS NOT 1
          AND date IS NOT NULL
          AND date != ''
        """
    ).fetchall()
    conn.close()

    long_pending = []
    today = date.today()
    for race_name, date_str, url in rows:
        try:
            race_date = date.fromisoformat(date_str[:10])
            days_ago = (today - race_date).days
            if days_ago >= PENDING_WARNING_DAYS:
                long_pending.append((race_name, date_str[:10], days_ago, url))
        except Exception:
            continue

    # Build markdown summary
    lines = ["# Daily scrape report", ""]
    run_at = report.get("run_at", "")
    lines += [f"**Run at:** {run_at}", ""]

    # Scraper section
    lines += ["## Scraper (TV2)"]
    new = report.get("new", [])
    updated = report.get("updated", [])
    no_star = report.get("no_star", [])

    if new:
        lines += ["### New predictions"]
        for p in new:
            lines.append(f"- **{p['race_name']}** — {p['predicted_winner'] or '⚠️ no prediction found'}")
        lines.append("")

    if updated:
        lines += ["### Updated pending predictions"]
        for p in updated:
            lines.append(f"- **{p['race_name']}** — {p['predicted_winner'] or '⚠️ no prediction found'}")
        lines.append("")

    if no_star:
        lines += ["### ⚠️ Articles with no ⭐⭐⭐⭐⭐ line"]
        for p in no_star:
            lines.append(f"- [{p['race_name']}]({p['url']})")
        lines.append("")

    if not new and not updated:
        lines += ["_No new or updated predictions._", ""]

    # Results section
    lines += ["## Results (PCS)"]
    if matched:
        lines += ["### Matched results"]
        for m in matched:
            lines.append(f"- {m}")
        lines.append("")

    if warnings:
        lines += ["### ⚠️ Unmatched races"]
        for w in warnings:
            lines.append(f"- {w}")
        lines.append("")

    if errors:
        lines += ["### ❌ Errors"]
        for e in errors:
            lines.append(f"- {e}")
        lines.append("")

    if not matched and not warnings and not errors:
        lines += ["_No new results processed._", ""]

    # Long-pending section
    if long_pending:
        lines += [f"## ⚠️ Predictions pending for {PENDING_WARNING_DAYS}+ days"]
        for race_name, race_date, days_ago, url in sorted(long_pending, key=lambda x: -x[2]):
            lines.append(f"- **{race_name}** ({race_date}) — {days_ago} days ago")
        lines.append("")

    summary = "\n".join(lines)

    # Write to GitHub Actions step summary
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write(summary)
    else:
        print(summary)

    # Exit 1 if there are warnings (triggers GitHub failure notification email)
    has_warnings = bool(warnings or errors or no_star or long_pending)
    if has_warnings:
        print("\n⚠️ Warnings detected — exiting with code 1 to trigger notification.")
        sys.exit(1)


if __name__ == "__main__":
    main()
