"""
Manually add specific article URLs to the predictions DB.

Usage:
    python add_urls.py <url1> <url2> ...

For each URL:
  - Navigates to the article and tries to extract race_name, date, and predicted_winner
  - If predicted_winner can't be found automatically, prompts you to enter it
  - Skips URLs already in the DB
"""

import argparse
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright

DB_PATH = Path(__file__).parent / "data" / "predictions.db"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def already_in_db(conn, url):
    return conn.execute("SELECT 1 FROM predictions WHERE url = ?", (url,)).fetchone() is not None


def extract(page, url):
    page.goto(url, wait_until="domcontentloaded", timeout=30_000)
    page.wait_for_timeout(2000)

    # Title
    title = None
    for sel in ["h1", "article h1", "[class*='headline']"]:
        try:
            text = page.locator(sel).first.inner_text(timeout=3_000).strip()
            if text:
                title = text
                break
        except Exception:
            continue

    if not title:
        print("  [WARN] Could not extract title")
        return None, None, None

    # Race name
    if "axelgaards optakt til" in title.lower():
        race_name = re.sub(r"(?i)^axelgaards optakt til\s*", "", title).strip()
    else:
        race_name = title.strip()

    # Date
    date = None
    try:
        date = page.locator("time[datetime]").first.get_attribute("datetime", timeout=3_000)
    except Exception:
        pass

    # Predicted winner
    predicted_winner = None
    full_text = page.locator("body").inner_text(timeout=10_000)
    pattern = re.compile(r"⭐{5}\s*[:\-–]?\s*(.+)", re.MULTILINE)
    match = pattern.search(full_text)
    if match:
        predicted_winner = match.group(1).strip()

    return race_name, date, predicted_winner


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("urls", nargs="+")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        context = browser.new_context(user_agent=USER_AGENT, locale="da-DK")
        page = context.new_page()

        for url in args.urls:
            print(f"\nURL: {url}")

            if already_in_db(conn, url):
                print("  Already in DB — skipping.")
                continue

            try:
                race_name, date, predicted_winner = extract(page, url)
            except Exception as e:
                print(f"  [ERROR] {e}")
                continue

            if not race_name:
                race_name = input("  Race name not found. Enter manually: ").strip()
            else:
                print(f"  Race:   {race_name}")

            print(f"  Date:   {date}")

            if predicted_winner:
                print(f"  Winner: {predicted_winner}")
            else:
                print("  Could not extract predicted winner automatically.")
                predicted_winner = input("  Enter predicted winner manually: ").strip()

            scraped_at = datetime.now(timezone.utc).isoformat()
            conn.execute(
                "INSERT OR IGNORE INTO predictions (url, race_name, date, predicted_winner, scraped_at) VALUES (?, ?, ?, ?, ?)",
                (url, race_name, date, predicted_winner, scraped_at),
            )
            conn.commit()
            print(f"  Inserted: {race_name} | {predicted_winner}")

        browser.close()

    conn.close()


if __name__ == "__main__":
    main()
