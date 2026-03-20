"""
Automated scraper — scrapes only the first page of Emil Axelgaard's TV2 author
page (no 'Vis flere' clicks), updates pending predictions with fresh data, then
runs results.py to match actual race results.

Designed to run headless in GitHub Actions on a daily schedule.

Usage:
    python scraper_auto.py
"""

import json
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import results_cn

from playwright.sync_api import sync_playwright

AUTHOR_URL = "https://sport.tv2.dk/profil/emil-axels"
DB_PATH = Path(__file__).parent / "data" / "predictions.db"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_row_status(conn: sqlite3.Connection, url: str) -> str:
    """Return 'new', 'pending', or 'resolved'."""
    row = conn.execute(
        "SELECT actual_winner FROM predictions WHERE url = ?", (url,)
    ).fetchone()
    if row is None:
        return "new"
    return "resolved" if row[0] is not None else "pending"


def insert_prediction(conn, url, race_name, date, predicted_winner):
    conn.execute(
        "INSERT OR IGNORE INTO predictions (url, race_name, date, predicted_winner, scraped_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (url, race_name, date, predicted_winner, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


def update_pending(conn, url, race_name, date, predicted_winner):
    conn.execute(
        "UPDATE predictions SET race_name=?, date=?, predicted_winner=?, scraped_at=? "
        "WHERE url=? AND actual_winner IS NULL",
        (race_name, date, predicted_winner, datetime.now(timezone.utc).isoformat(), url),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Page helpers
# ---------------------------------------------------------------------------

def dismiss_cookie_banner(page) -> None:
    page.wait_for_timeout(2000)
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
                page.wait_for_timeout(500)
                return
        except Exception:
            continue
    for frame in page.frames:
        if frame == page.main_frame:
            continue
        for sel in selectors:
            try:
                btn = frame.locator(sel)
                if btn.count() > 0:
                    btn.first.click(timeout=3_000)
                    page.wait_for_timeout(500)
                    return
            except Exception:
                continue


def collect_first_page_urls(page) -> list[str]:
    """Load author page without clicking 'Vis flere' and return cycling article URLs."""
    print(f"Loading {AUTHOR_URL} ...")
    page.goto(AUTHOR_URL, wait_until="domcontentloaded")
    dismiss_cookie_banner(page)
    page.wait_for_load_state("domcontentloaded")
    page.wait_for_timeout(2000)

    hrefs = page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
    seen = set()
    urls = []
    for href in hrefs:
        if not isinstance(href, str):
            continue
        if "sport.tv2.dk" not in href or "/live/" in href:
            continue
        if "/cykling/" not in href:
            continue
        if href in seen:
            continue
        seen.add(href)
        urls.append(href)
    print(f"Found {len(urls)} cycling URLs on first page.")
    return urls


def extract_article(page, url: str) -> dict | None:
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
    except Exception as e:
        print(f"  [WARN] Failed to load {url}: {e}")
        return None

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
        return None

    title_lower = title.lower()
    if "axelgaards optakt til" in title_lower:
        race_name = re.sub(r"(?i)^axelgaards optakt til\s*", "", title).strip()
    elif title_lower.startswith("tour de france"):
        race_name = title.strip()
    else:
        return None

    # Date
    date = None
    try:
        date = page.locator("time[datetime]").first.get_attribute("datetime", timeout=3_000)
    except Exception:
        pass

    # Predicted winner
    predicted_winner = None
    full_text = page.locator("body").inner_text(timeout=10_000)
    for stars in (5, 3):
        m = re.search(rf"⭐{{{stars}}}\s*[:\-–]?\s*(.+)", full_text, re.MULTILINE)
        if m:
            predicted_winner = m.group(1).strip()
            break

    return {"race_name": race_name, "date": date, "predicted_winner": predicted_winner}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

REPORT_PATH = Path(__file__).parent / "data" / "run_report.json"


def main():
    conn = sqlite3.connect(DB_PATH)

    report = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "new": [],
        "updated": [],
        "no_star": [],
        "results_output": "",
    }

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_context(user_agent=USER_AGENT, locale="da-DK").new_page()

        urls = collect_first_page_urls(page)

        for url in urls:
            status = get_row_status(conn, url)

            if status == "resolved":
                continue

            data = extract_article(page, url)
            if data is None:
                continue

            if not data["predicted_winner"]:
                report["no_star"].append({"url": url, "race_name": data["race_name"]})

            if status == "new":
                print(f"  [NEW] {data['race_name']} | {data['predicted_winner']}")
                insert_prediction(conn, url, data["race_name"], data["date"], data["predicted_winner"])
                report["new"].append({"race_name": data["race_name"], "predicted_winner": data["predicted_winner"], "url": url})
            elif status == "pending":
                print(f"  [UPDATE] {data['race_name']} | {data['predicted_winner']}")
                update_pending(conn, url, data["race_name"], data["date"], data["predicted_winner"])
                report["updated"].append({"race_name": data["race_name"], "predicted_winner": data["predicted_winner"], "url": url})

            time.sleep(1.0)

        browser.close()

    conn.close()
    print(f"\nScrape done. {len(report['new'])} new, {len(report['updated'])} updated.")

    # Match results via Cyclingnews (no Cloudflare, plain HTTP)
    print("\nRunning results_cn.py ...")
    unmatched = results_cn.main(pages=1)
    report["unmatched"] = [r["race_name"] for r in unmatched]

    REPORT_PATH.write_text(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"\nReport written to {REPORT_PATH}")

    # Write GitHub Actions job summary
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        lines = ["## Hit Rate of Predictions — Daily Run\n"]

        if report["new"]:
            lines.append("### New predictions scraped")
            for r in report["new"]:
                lines.append(f"- **{r['race_name']}** — predicted: {r['predicted_winner']}")
            lines.append("")

        if report["updated"]:
            lines.append("### Updated predictions")
            for r in report["updated"]:
                lines.append(f"- **{r['race_name']}** — predicted: {r['predicted_winner']}")
            lines.append("")

        if unmatched:
            lines.append("### ⚠️ Results not found — run `results.py` locally")
            for r in unmatched:
                lines.append(f"- **{r['race_name']}** — predicted: {r['predicted_winner']}")
            lines.append("")
        else:
            lines.append("### ✅ All results matched automatically")
            lines.append("")

        if report["no_star"]:
            lines.append("### ⚠️ Articles with no predicted winner (no ⭐⭐⭐⭐⭐ found)")
            for r in report["no_star"]:
                lines.append(f"- {r['race_name']} — {r['url']}")
            lines.append("")

        with open(summary_path, "w") as f:
            f.write("\n".join(lines))


if __name__ == "__main__":
    main()
