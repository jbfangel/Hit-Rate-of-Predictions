"""
Update the date field for all resolved predictions to the actual race date.

Visits each result_source (PCS URL), extracts the race date from the infolist,
and overwrites the date column.

Usage:
    python update_dates.py
    python update_dates.py --limit 50   # process only first N rows
"""

import argparse
import re
import sqlite3
import time
from pathlib import Path

from patchright.sync_api import sync_playwright

DB_PATH = Path(__file__).parent / "data" / "predictions.db"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def extract_race_date(page, year_hint: str | None = None) -> str | None:
    try:
        items = page.locator(".infolist li").all()
        for li in items:
            try:
                text = li.inner_text(timeout=1_000)
            except Exception:
                continue
            m = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", text)
            if m:
                day, month, year = m.group(1), m.group(2), m.group(3)
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            m = re.search(r"\b(\d{1,2})/(\d{1,2})\b", text)
            if m:
                day, month = m.group(1), m.group(2)
                year = year_hint or re.search(r"/(\d{4})/", page.url)
                if hasattr(year, "group"):
                    year = year.group(1)
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    except Exception:
        pass
    return None


def find_last_stage_date(page, overview_url: str) -> str | None:
    """For a GC overview page with no infolist date, navigate to the last stage and get its date."""
    try:
        # Collect all stage links from the overview page (already loaded)
        links = page.locator("a[href*='/stage-']").all()
        stage_urls = set()
        for link in links:
            try:
                href = link.get_attribute("href", timeout=500)
                if not href:
                    continue
                m = re.search(r"(/race/[^/]+/\d{4}/stage-\d+)", href)
                if m:
                    base = "https://www.procyclingstats.com" + m.group(1)
                    stage_urls.add(base)
            except Exception:
                continue

        if not stage_urls:
            return None

        # Sort by stage number and take the last
        def stage_num(u):
            m = re.search(r"/stage-(\d+)$", u)
            return int(m.group(1)) if m else 0

        last_stage_url = max(stage_urls, key=stage_num)
        print(f"[last stage] {last_stage_url} ...", end=" ", flush=True)

        page.goto(last_stage_url, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(500)

        year_hint = re.search(r"/(\d{4})/", last_stage_url)
        year_hint = year_hint.group(1) if year_hint else None
        return extract_race_date(page, year_hint)
    except Exception:
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--url-filter", type=str, default=None,
                        help="Only process rows whose result_source contains this string")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT id, result_source, date
        FROM predictions
        WHERE actual_winner IS NOT NULL
          AND result_source IS NOT NULL
          AND result_source != ''
    """
    if args.url_filter:
        query += f"  AND result_source LIKE '%{args.url_filter}%'\n"
    query += "ORDER BY date"
    rows = conn.execute(query).fetchall()

    if args.limit:
        rows = rows[: args.limit]

    print(f"Processing {len(rows)} rows...\n")

    updated = 0
    skipped = 0

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=USER_AGENT, locale="da-DK")
        page = ctx.new_page()

        for i, (row_id, result_source, current_date) in enumerate(rows, 1):
            print(f"[{i}/{len(rows)}] id={row_id} {result_source} ...", end=" ", flush=True)
            try:
                page.goto(result_source, wait_until="domcontentloaded", timeout=30_000)
                page.wait_for_timeout(500)
                year_hint = re.search(r"/(\d{4})/", result_source)
                year_hint = year_hint.group(1) if year_hint else None
                race_date = extract_race_date(page, year_hint)
                if not race_date:
                    race_date = find_last_stage_date(page, result_source)
            except Exception as e:
                print(f"ERROR: {e}")
                skipped += 1
                continue

            if race_date:
                conn.execute(
                    "UPDATE predictions SET date=? WHERE id=?",
                    (race_date, row_id),
                )
                conn.commit()
                print(f"{current_date} → {race_date}")
                updated += 1
            else:
                print(f"no date found, keeping {current_date}")
                skipped += 1

            time.sleep(0.3)

        browser.close()

    conn.close()
    print(f"\nDone. {updated} updated, {skipped} skipped.")


if __name__ == "__main__":
    main()
