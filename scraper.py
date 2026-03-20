"""
TV2 scraper — collects Emil Axelgaard's cycling predictions from sport.tv2.dk.

Usage:
    python scraper.py

Output:
    data/predictions.db — SQLite database with predictions table
"""

import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright

AUTHOR_URL = "https://sport.tv2.dk/profil/emil-axels"
DB_PATH = Path(__file__).parent / "data" / "predictions.db"


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            url            TEXT UNIQUE,
            race_name      TEXT,
            date           TEXT,
            predicted_winner TEXT,
            actual_winner  TEXT,
            correct        INTEGER,
            scraped_at     TEXT
        )
    """)
    conn.commit()


def already_scraped(conn: sqlite3.Connection, url: str) -> bool:
    row = conn.execute("SELECT 1 FROM predictions WHERE url = ?", (url,)).fetchone()
    return row is not None


def insert_prediction(conn: sqlite3.Connection, url: str, race_name: str,
                      date: str, predicted_winner: str) -> None:
    scraped_at = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT OR IGNORE INTO predictions (url, race_name, date, predicted_winner, scraped_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (url, race_name, date, predicted_winner, scraped_at),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Article list — collect URLs from author page
# ---------------------------------------------------------------------------

def collect_article_urls(page) -> list[str]:
    """Navigate to author page, click 'Load more' until exhausted, return all article URLs."""
    print(f"Navigating to {AUTHOR_URL}")
    page.goto(AUTHOR_URL, wait_until="domcontentloaded")

    # Dismiss GDPR cookie consent banner
    _dismiss_cookie_banner(page)

    # Debug: print all buttons visible on the page so we can see what to click
    buttons = page.eval_on_selector_all("button", "els => els.map(e => e.innerText.trim()).filter(t => t)")
    print(f"  Buttons found on page: {buttons}")

    # Click "Load more" until the button disappears or article count stops growing
    load_more_btn = page.locator("button", has_text=re.compile(r"Vis flere", re.IGNORECASE))

    prev_count = 0
    max_clicks = 200  # safety cap (loop already stops when button disappears)
    for click_num in range(max_clicks):
        if load_more_btn.count() == 0:
            print("  'Vis flere' button gone — all articles loaded.")
            break

        current_count = page.locator("a[href*='sport.tv2.dk/cykling/']").count()
        print(f"  Click {click_num + 1}: {current_count} cycling links so far, clicking 'Vis flere'...")

        if current_count == prev_count and click_num > 0:
            print("  Article count didn't grow after last click — stopping.")
            break
        prev_count = current_count

        try:
            load_more_btn.scroll_into_view_if_needed(timeout=5_000)
            time.sleep(0.5)
            load_more_btn.click(timeout=10_000)
            # Wait for new articles to appear (up to 10s) rather than fixed sleep
            try:
                page.wait_for_function(
                    f"document.querySelectorAll(\"a[href*='sport.tv2.dk/cykling/']\").length > {current_count}",
                    timeout=10_000,
                )
            except Exception:
                time.sleep(3.0)  # fallback if wait times out
        except Exception as e:
            print(f"  Load more stopped: {e}")
            break
    else:
        print(f"  Reached max clicks ({max_clicks}) — stopping.")

    # Collect all article links
    hrefs = page.eval_on_selector_all(
        "a[href]",
        "els => els.map(e => e.href)",
    )

    # Filter: keep only cycling prediction articles
    urls = []
    seen = set()
    for href in hrefs:
        if not isinstance(href, str):
            continue
        # Must be on tv2.dk and look like an article (not a profile/tag page)
        if "sport.tv2.dk" not in href:
            continue
        if "/live/" in href:
            continue
        if href in seen:
            continue
        seen.add(href)
        urls.append(href)

    return urls


def _dismiss_cookie_banner(page) -> None:
    page.wait_for_timeout(2000)  # give banner time to appear

    # Try direct page selectors first
    selectors = [
        "button#accept-all-button",
        "button[id*='accept']",
        "button[class*='accept']",
        "#onetrust-accept-btn-handler",
        "button:has-text('Accepter alle')",
        "button:has-text('Accepter')",
        "button:has-text('Tillad alle')",
        "button:has-text('Accept all')",
        "button:has-text('Accept')",
    ]
    for sel in selectors:
        try:
            btn = page.locator(sel)
            if btn.count() > 0:
                btn.first.click(timeout=3_000)
                page.wait_for_timeout(1000)
                print("  Cookie banner dismissed (direct).")
                return
        except Exception:
            continue

    # Try inside iframes (common for CMP platforms)
    for frame in page.frames:
        if frame == page.main_frame:
            continue
        for sel in selectors:
            try:
                btn = frame.locator(sel)
                if btn.count() > 0:
                    btn.first.click(timeout=3_000)
                    page.wait_for_timeout(1000)
                    print(f"  Cookie banner dismissed (iframe: {frame.url}).")
                    return
            except Exception:
                continue

    print("  No cookie banner found (or already dismissed).")


# ---------------------------------------------------------------------------
# Article extraction
# ---------------------------------------------------------------------------

def extract_article(page, url: str) -> dict | None:
    """
    Navigate to an article and extract:
      - title  → race_name
      - date   → publication date (ISO string)
      - predicted_winner → text after ⭐⭐⭐⭐⭐
    Returns None if the article doesn't look like a prediction.
    """
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
    except Exception as e:
        print(f"  [WARN] Failed to load {url}: {e}")
        return None

    # Title
    title = _extract_title(page)
    if not title:
        print(f"  [WARN] No title found at {url}")
        return None

    # Filter: "Axelgaards optakt til" articles OR titles starting with "Tour de France"
    title_lower = title.lower()
    if "axelgaards optakt til" in title_lower:
        race_name = re.sub(r"(?i)^axelgaards optakt til\s*", "", title).strip()
    elif title_lower.startswith("tour de france"):
        race_name = title.strip()
    else:
        return None

    # Date
    date = _extract_date(page)

    # Predicted winner
    predicted_winner = _extract_predicted_winner(page)
    if not predicted_winner:
        print(f"  [WARN] No ⭐⭐⭐⭐⭐ line found in {url}")

    return {
        "race_name": race_name,
        "date": date,
        "predicted_winner": predicted_winner,
    }


def _extract_title(page) -> str | None:
    selectors = ["h1", "article h1", "[class*='title'] h1", "[class*='headline']"]
    for sel in selectors:
        el = page.locator(sel).first
        try:
            text = el.inner_text(timeout=3_000).strip()
            if text:
                return text
        except Exception:
            continue
    return None


def _extract_date(page) -> str | None:
    """Try common date selectors; return ISO date string or None."""
    # Try <time> element first
    time_el = page.locator("time[datetime]").first
    try:
        dt = time_el.get_attribute("datetime", timeout=3_000)
        if dt:
            return dt
    except Exception:
        pass

    # Try meta tag
    for meta_name in ["article:published_time", "og:pubdate", "pubdate"]:
        try:
            content = page.locator(f"meta[property='{meta_name}'], meta[name='{meta_name}']").first
            val = content.get_attribute("content", timeout=2_000)
            if val:
                return val
        except Exception:
            continue

    return None


def _extract_predicted_winner(page) -> str | None:
    """Find the line containing ⭐⭐⭐⭐⭐ and extract the rider name after it.
    Falls back to ⭐⭐⭐ if no 5-star line is found."""
    full_text = page.locator("body").inner_text(timeout=10_000)
    for stars in (5, 3):
        pattern = re.compile(rf"⭐{{{stars}}}\s*[:\-–]?\s*(.+)", re.MULTILINE)
        match = pattern.search(full_text)
        if match:
            return match.group(1).strip()
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)  # headless=False to watch it work
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            locale="da-DK",
        )
        page = context.new_page()

        # Step 1: Collect all article URLs
        all_urls = collect_article_urls(page)
        print(f"\nFound {len(all_urls)} total article links on author page.")

        # Step 2: Filter to prediction articles (title check happens inside extract_article)
        # Pre-filter by URL pattern — "/cykling/" appears in cycling articles
        candidate_urls = [u for u in all_urls if "/cykling/" in u or "axelgaard" in u.lower()]
        print(f"Candidates after /cykling/ filter: {len(candidate_urls)} of {len(all_urls)} total links")
        if not candidate_urls:
            # Fall back to all article-like URLs
            candidate_urls = [u for u in all_urls if re.search(r"sport\.tv2\.dk/\w+/\d{4}-\d{2}-\d{2}-", u)]
            print(f"Fallback filter: {len(candidate_urls)} candidates")
        # Print a sample so we can verify the filter is working
        print(f"Sample URLs: {candidate_urls[:3]}")

        new_count = 0
        skip_count = 0

        for i, url in enumerate(candidate_urls, 1):
            if already_scraped(conn, url):
                skip_count += 1
                continue

            print(f"\n[{i}/{len(candidate_urls)}] {url}")
            data = extract_article(page, url)

            if data is None:
                # Not a prediction article or failed to parse
                continue

            print(f"  Race:      {data['race_name']}")
            print(f"  Date:      {data['date']}")
            print(f"  Winner:    {data['predicted_winner']}")

            insert_prediction(
                conn,
                url=url,
                race_name=data["race_name"],
                date=data["date"],
                predicted_winner=data["predicted_winner"],
            )
            new_count += 1

            # Rate limiting
            time.sleep(1.5)

        browser.close()

    conn.close()

    print(f"\nDone. {new_count} new predictions inserted, {skip_count} already in DB.")
    print(f"Database: {DB_PATH}")


if __name__ == "__main__":
    main()
