"""
Results matcher — populates actual_winner and correct for each prediction.

Usage:
    python results.py              # full run (writes to DB)
    python results.py --dry-run    # validate first 20 rows, no DB writes
    python results.py --dry-run --limit 5

Goes directly to ProCyclingStats result pages, extracts the winner from
the structured results table, fuzzy-matches against predicted_winner,
and updates the DB.

Only processes rows where actual_winner IS NULL — safe to re-run.
"""

import argparse
import re
import sqlite3
import time
import unicodedata
from pathlib import Path

from patchright.sync_api import sync_playwright

DB_PATH = Path(__file__).parent / "data" / "predictions.db"
BASE = "https://www.procyclingstats.com/race"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

WINNER_SELECTORS = [
    "table.results tbody tr:first-child td.gc_winner a",
    "table.results tbody tr:first-child a",
    ".result-cont tr:first-child a",
    "ul.result-cont li:first-child a",
]

SLUG_OVERRIDES = {
    "Ename Samyn Classic":                          "ename-classic-samyn",
    "Vuelta a Andalucia Ruta Ciclista del Sol":     "vuelta-a-andalucia",
    "Volta ao Algarve em Bicicleta":                "volta-ao-algarve",
    "Vuelta a la Region de Murcia Costa Calida":    "vuelta-a-murcia",
    "CIC Tour de la Provence":                      "tour-de-la-provence",
    "Faun Drome Classic":                           "drome-classic",
    "Faun-Ardèche Classic":                         "ardeche-classic",
    "Classic Var":                                  "classic-var",
    "Tour des Alpes-Maritimes":                     "tour-des-alpes-maritimes-et-du-var",
    "Figueira Champions Classic":                   "figueira-champions-classic",
    "VM-linjeløbet":                                "world-championship-road-race",
    "VM i enkeltstart":                             "world-championship-itt",
    "EM-linjeløbet":                                "european-championship-road-race",
    "EM-enkeltstarten":                             "european-championship-itt",
    "U23-rytternes VM-linjeløb":                    "world-championship-u23-road-race",
    "U23-rytternes VM-enkeltstart":                 "world-championship-u23-itt",
    "de nationale mesterskaber i linjeløb":         None,
    "de nationale mesterskaber i enkeltstart":      None,
    "de australske mesterskaber i linjeløb":        None,
    "de australske mesterskaber i enkeltstart":     None,
    "bjergkonkurrencen i Tour de France":           None,
    "pointkonkurrencen i Tour de France":           None,
    "ungdomskonkurrencen i Tour de France":         None,
    "holdkonkurrencen i Tour de France":            None,
}


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def add_result_source_column(conn: sqlite3.Connection) -> None:
    """Add result_source column if it doesn't exist (idempotent)."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(predictions)")}
    if "result_source" not in cols:
        conn.execute("ALTER TABLE predictions ADD COLUMN result_source TEXT")
        conn.commit()
        print("Added result_source column.")


def fetch_null_rows(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT id, race_name, date, predicted_winner FROM predictions WHERE actual_winner IS NULL"
    ).fetchall()
    return [
        {"id": r[0], "race_name": r[1], "date": r[2], "predicted_winner": r[3]}
        for r in rows
    ]


def update_result(conn: sqlite3.Connection, row_id: int, actual_winner: str,
                  correct: int, result_source: str) -> None:
    conn.execute(
        "UPDATE predictions SET actual_winner=?, correct=?, result_source=? WHERE id=?",
        (actual_winner, correct, result_source, row_id),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# URL / slug construction
# ---------------------------------------------------------------------------

def to_slug(name: str) -> str | None:
    if name in SLUG_OVERRIDES:
        return SLUG_OVERRIDES[name]  # may be None → caller skips
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = name.lower()
    name = re.sub(r"[^a-z0-9\s-]", "", name)
    name = re.sub(r"[\s]+", "-", name.strip())
    return name


def build_pcs_url(race_name: str, date: str) -> tuple[str | None, str | None]:
    """
    Returns (url, slug_or_None).
    Returns (None, None) when the race should be skipped.
    """
    year = date[:4] if date else "2026"

    # Stage: "8. etape af Paris-Nice"
    m = re.match(r"(\d+)\. etape af (.+)", race_name, re.IGNORECASE)
    if m:
        slug = to_slug(m.group(2).strip())
        if slug is None:
            return None, None
        return f"{BASE}/{slug}/{year}/stage-{m.group(1)}", slug

    # Prologue: "prologen til Santos Tour Down Under"
    m = re.match(r"prologen til (.+)", race_name, re.IGNORECASE)
    if m:
        slug = to_slug(m.group(1).strip())
        if slug is None:
            return None, None
        return f"{BASE}/{slug}/{year}/prologue", slug

    # One-day / GC
    slug = to_slug(race_name)
    if slug is None:
        return None, None
    return f"{BASE}/{slug}/{year}", slug


# ---------------------------------------------------------------------------
# Name matching
# ---------------------------------------------------------------------------

def normalize(name: str) -> set[str]:
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    return set(name.lower().split())


def names_match(predicted: str, found: str) -> bool:
    return bool(normalize(predicted) & normalize(found))


# ---------------------------------------------------------------------------
# Winner extraction from PCS
# ---------------------------------------------------------------------------

def extract_winner(page) -> str | None:
    for sel in WINNER_SELECTORS:
        try:
            el = page.locator(sel).first
            if el.count() > 0:
                text = el.inner_text(timeout=3_000).strip()
                if text:
                    return text
        except Exception:
            continue
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Print results without writing to DB")
    parser.add_argument("--limit", type=int, default=None, help="Process only first N rows")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    if not args.dry_run:
        add_result_source_column(conn)

    rows = fetch_null_rows(conn)
    limit = args.limit if args.limit is not None else (20 if args.dry_run else None)
    if limit:
        rows = rows[:limit]

    prefix = "[DRY RUN] " if args.dry_run else ""
    print(f"{prefix}Processing {len(rows)} rows.")

    if not rows:
        print("Nothing to do.")
        conn.close()
        return

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        context = browser.new_context(user_agent=USER_AGENT, locale="en-US")
        page = context.new_page()

        matched = 0
        unmatched = 0
        skipped = 0

        for i, row in enumerate(rows, 1):
            race_name = row["race_name"]
            date = row["date"] or "2026-01-01"

            url, slug = build_pcs_url(race_name, date)

            if url is None:
                print(f"[{i}/{len(rows)}] Skipping '{race_name}' (not on PCS)")
                skipped += 1
                continue

            print(f"\n[{i}/{len(rows)}] {race_name} | predicted: {row['predicted_winner']}")
            print(f"  URL: {url}")

            try:
                response = page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                status = response.status if response else None
                if status == 404:
                    print(f"  [WARNING] 404 for id={row['id']} url='{url}'")
                    unmatched += 1
                    time.sleep(2)
                    continue
            except Exception as e:
                print(f"  [WARNING] Navigation failed for id={row['id']} url='{url}': {e}")
                unmatched += 1
                time.sleep(2)
                continue

            # Give the page a moment to render the table
            page.wait_for_timeout(1500)

            winner = extract_winner(page)

            if winner:
                correct = 1 if names_match(row["predicted_winner"] or "", winner) else 0
                status_str = "CORRECT" if correct else "WRONG"
                print(f"  Found: {winner} → {status_str}")
                if args.dry_run:
                    print(f"  [DRY RUN] Would write: actual_winner={winner}, correct={correct}")
                else:
                    update_result(conn, row["id"], winner, correct, url)
                matched += 1
            else:
                print(f"  [WARNING] No winner found for id={row['id']} race='{race_name}' url='{url}'")
                unmatched += 1

            time.sleep(2)

        browser.close()

    conn.close()
    print(f"\n{prefix}Done. Matched: {matched}, No result found: {unmatched}, Skipped: {skipped}.")
    print(f"Database: {DB_PATH}")


if __name__ == "__main__":
    main()
