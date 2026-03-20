"""
Find missing prediction articles for the 2025 gap by using 2026 URLs as templates.

For each 2026 URL in the DB (Jan–Mar 2026), extracts the slug, shifts the date
back 1 year, and tries ±3 days around that date. Adds any valid articles to the DB.

Usage:
    python find_gap_urls.py
"""

import re
import sqlite3
from datetime import date, timedelta
from pathlib import Path

from playwright.sync_api import sync_playwright

DB_PATH = Path(__file__).parent / "data" / "predictions.db"
BASE_TV2 = "https://sport.tv2.dk/cykling"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

DATE_WINDOW = 3  # try ±3 days around the shifted date


def get_template_urls(conn: sqlite3.Connection) -> list[tuple[str, date]]:
    """Return (slug, shifted_date) for all 2026 Jan–Mar URLs, shifted back 1 year."""
    rows = conn.execute(
        """
        SELECT url, date FROM predictions
        WHERE date >= '2026-01-01' AND date < '2026-03-11'
        ORDER BY date
        """
    ).fetchall()

    templates = []
    for url, date_str in rows:
        # Extract slug: everything after the date prefix
        m = re.search(r"/\d{4}-\d{2}-\d{2}-(.+)$", url)
        if not m:
            continue
        slug = m.group(1)

        # Parse date and shift back 1 year
        d = date.fromisoformat(date_str[:10])
        shifted = d.replace(year=d.year - 1)
        templates.append((slug, shifted))

    return templates


def already_in_db(conn: sqlite3.Connection, url: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM predictions WHERE url = ?", (url,)
    ).fetchone() is not None


def is_valid_prediction(page, url: str) -> bool:
    try:
        resp = page.goto(url, wait_until="domcontentloaded", timeout=15_000)
        if not resp or resp.status == 404:
            return False
        title = page.locator("h1").first.inner_text(timeout=3_000).strip()
        return "axelgaards optakt til" in title.lower()
    except Exception:
        return False


def extract_and_insert(page, conn: sqlite3.Connection, url: str) -> bool:
    """Extract race_name, date, predicted_winner and insert into DB."""
    from datetime import datetime, timezone
    import re as _re

    try:
        title = page.locator("h1").first.inner_text(timeout=3_000).strip()
        race_name = _re.sub(r"(?i)^axelgaards optakt til\s*", "", title).strip()

        date_val = None
        try:
            date_val = page.locator("time[datetime]").first.get_attribute("datetime", timeout=3_000)
        except Exception:
            pass

        full_text = page.locator("body").inner_text(timeout=10_000)
        predicted_winner = None
        for stars in (5, 3):
            m = _re.search(rf"⭐{{{stars}}}\s*[:\-–]?\s*(.+)", full_text)
            if m:
                predicted_winner = m.group(1).strip()
                break

        scraped_at = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT OR IGNORE INTO predictions (url, race_name, date, predicted_winner, scraped_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (url, race_name, date_val, predicted_winner, scraped_at),
        )
        conn.commit()
        print(f"  ✓ Inserted: {race_name} | {predicted_winner}")
        return True
    except Exception as e:
        print(f"  [ERROR] {e}")
        return False


def main():
    conn = sqlite3.connect(DB_PATH)
    templates = get_template_urls(conn)
    print(f"Found {len(templates)} template URLs from 2026 Jan–Mar\n")

    inserted = 0
    checked = 0

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        ctx = browser.new_context(user_agent=USER_AGENT, locale="da-DK")
        page = ctx.new_page()

        for slug, base_date in templates:
            # Try ±DATE_WINDOW days around the shifted date
            found = False
            for delta in range(-DATE_WINDOW, DATE_WINDOW + 1):
                candidate_date = base_date + timedelta(days=delta)
                url = f"{BASE_TV2}/{candidate_date.strftime('%Y-%m-%d')}-{slug}"
                checked += 1

                if already_in_db(conn, url):
                    found = True
                    break

                print(f"Trying {url} ...", end=" ", flush=True)
                if is_valid_prediction(page, url):
                    print("✓")
                    extract_and_insert(page, conn, url)
                    inserted += 1
                    found = True
                    break
                else:
                    print("✗")

            if not found:
                print(f"  [MISS] No match found for slug: {slug}")

        browser.close()

    conn.close()
    print(f"\nDone. Checked {checked} URLs, inserted {inserted} new predictions.")


if __name__ == "__main__":
    main()
