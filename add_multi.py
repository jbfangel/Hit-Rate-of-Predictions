"""
Add multiple race predictions from a single TV2 article structured with
'Vinderbud' sections (e.g. national championship previews).

Usage:
    python add_multi.py <url>

For each 'Vinderbud' section the script:
  - Finds the nearest preceding section header and extracts the country name
    (text before the first '(' in the header)
  - Combines it with the race name from the article title to form race_name
    e.g. "de nationale mesterskaber i linjeløb" + "Danmark"
       → "de nationale mesterskaber i linjeløb Danmark"
  - Extracts the predicted winner (line after 'Vinderbud')
  - Prompts you to confirm before inserting
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


def extract_article_race_name(title: str) -> str:
    """Strip 'Axelgaards optakt til' prefix to get base race name."""
    return re.sub(r"(?i)^axelgaards optakt til\s*", "", title).strip()


def find_country_headings(lines: list[str]) -> list[tuple[int, str]]:
    """
    Return (line_index, country_name) for all lines that look like a country
    section heading: a single capitalised word, optionally followed by (…).
    E.g. "Danmark", "Belgien", "Nederlandene (29. juni)"
    """
    headings = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        m = re.match(r"^([A-ZÆØÅ][a-zA-ZÆØÅæøå]+)\s*\([^)]*\)$", stripped)
        if m:
            headings.append((i, m.group(1)))
    return headings


def extract_sections(full_text: str) -> list[dict]:
    """
    Find all standalone 'Vinderbud' lines. For each, assign the nearest
    preceding country heading. Returns list of dicts with country,
    predicted_winner, and context.
    """
    lines = full_text.splitlines()
    headings = find_country_headings(lines)
    results = []

    for i, line in enumerate(lines):
        if not re.fullmatch(r"\s*vinderbud\s*", line, re.IGNORECASE):
            continue

        # Predicted winner: next non-empty line after 'Vinderbud'
        predicted_winner = ""
        for j in range(i + 1, min(i + 5, len(lines))):
            candidate = lines[j].strip()
            if candidate:
                predicted_winner = candidate
                break

        # Nearest country heading before this 'Vinderbud'
        country = ""
        for h_idx, h_name in reversed(headings):
            if h_idx < i:
                country = h_name
                break

        # Context for display
        start = max(0, i - 8)
        context = "\n".join(lines[start:i + 2])

        results.append({
            "country": country,
            "predicted_winner": predicted_winner,
            "context": context,
        })

    return results


def already_in_db(conn, url: str, race_name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM predictions WHERE url = ? AND race_name = ?",
        (url, race_name),
    ).fetchone() is not None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("url")
    args = parser.parse_args()
    url = args.url

    conn = sqlite3.connect(DB_PATH)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        ctx = browser.new_context(user_agent=USER_AGENT, locale="da-DK")
        page = ctx.new_page()

        print(f"Loading {url} ...")
        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(2000)

        date = None
        try:
            date = page.locator("time[datetime]").first.get_attribute("datetime", timeout=3_000)
        except Exception:
            pass

        title = ""
        for sel in ["h1", "article h1", "[class*='headline']"]:
            try:
                text = page.locator(sel).first.inner_text(timeout=3_000).strip()
                if text:
                    title = text
                    break
            except Exception:
                continue

        full_text = page.locator("body").inner_text(timeout=10_000)
        browser.close()

    base_race_name = extract_article_race_name(title)
    print(f"Article race name: '{base_race_name}'")

    # Try star system first (single prediction article)
    star_match = re.search(r"⭐{5}\s*[:\-–]?\s*(.+)", full_text)
    if not star_match:
        star_match = re.search(r"⭐{3}\s*[:\-–]?\s*(.+)", full_text)
    if star_match:
        predicted_winner = star_match.group(1).strip()
        print(f"Found star prediction: '{predicted_winner}'")
        override = input("Press Enter to accept, or type a new name: ").strip()
        if override:
            predicted_winner = override
        name_override = input(f"Race name '{base_race_name}' — press Enter to accept or type new: ").strip()
        race_name = name_override if name_override else base_race_name
        if already_in_db(conn, url, race_name):
            print("Already in DB — skipping.")
            conn.close()
            return
        scraped_at = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT OR IGNORE INTO predictions (url, race_name, date, predicted_winner, scraped_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (url, race_name, date, predicted_winner, scraped_at),
        )
        conn.commit()
        print(f"Inserted: {race_name} | {predicted_winner}")
        conn.close()
        return

    sections = extract_sections(full_text)
    if not sections:
        print("No 'Vinderbud' sections found in article.")
        conn.close()
        return

    print(f"Found {len(sections)} predictions. Date: {date}\n")

    inserted = 0
    for i, sec in enumerate(sections, 1):
        race_name = f"{base_race_name} {sec['country']}".strip() if sec["country"] else base_race_name

        print(f"{'='*60}")
        print(f"Prediction {i}/{len(sections)}\n")
        print(f"Context:\n{sec['context']}\n")
        print(f"Race name: {race_name}")
        print(f"Predicted winner: {sec['predicted_winner']}")

        name_override = input("Press Enter to accept race name, or type a new one: ").strip()
        if name_override:
            race_name = name_override

        winner_override = input("Press Enter to accept predicted winner, or type a new one: ").strip()
        predicted_winner = winner_override if winner_override else sec["predicted_winner"]

        if not predicted_winner:
            print("  No predicted winner — skipping.")
            continue

        # Give each child row a unique URL by appending the country as a fragment
        row_url = f"{url}#{sec['country']}" if sec["country"] else url

        if already_in_db(conn, row_url, race_name):
            print(f"  Already in DB — skipping.")
            continue

        scraped_at = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT OR IGNORE INTO predictions (url, race_name, date, predicted_winner, scraped_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (row_url, race_name, date, predicted_winner, scraped_at),
        )
        conn.commit()
        print(f"  Inserted: {race_name} | {predicted_winner}")
        inserted += 1

    conn.close()
    print(f"\nDone. {inserted} rows inserted.")


if __name__ == "__main__":
    main()
