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


SLUG_OVERRIDES = {
    "Ename Samyn Classic":                          "gp-samyn",
    "Kuurne-Bruxelles-Kuurne":                      "kuurne-brussel-kuurne",
    "Omloop Nieuwsblad":                            "omloop-het-nieuwsblad",
    "Etoile de Bessèges – Tour du Gard":            "etoile-de-besseges",
    "Giro della Sardegna":                          "giro-di-sardegna",
    "Vuelta a Andalucia Ruta Ciclista del Sol":     "ruta-del-sol",
    "Volta ao Algarve em Bicicleta":                "volta-ao-algarve",
    "Vuelta a la Region de Murcia Costa Calida":    "vuelta-a-murcia",
    "CIC Tour de la Provence":                      "tour-de-la-provence",
    "Faun Drome Classic":                           "la-drome-classic",
    "Faun-Ardèche Classic":                         "faun-ardeche-classic",
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


def build_url_from_slug(race_name: str, slug: str, year: str) -> str:
    """Reconstruct a PCS URL given a slug, preserving stage/prologue suffix."""
    m = re.match(r"(\d+)\. etape af .+", race_name, re.IGNORECASE)
    if m:
        return f"{BASE}/{slug}/{year}/stage-{m.group(1)}"
    if re.match(r"prologen til .+", race_name, re.IGNORECASE):
        return f"{BASE}/{slug}/{year}/prologue"
    return f"{BASE}/{slug}/{year}"


_pcs_race_list: list[tuple[str, str, str]] | None = None  # (slug, name, year)


def _load_pcs_race_list(page) -> list[tuple[str, str, str]]:
    """
    Fetch and parse search_list26.js — the race index PCS uses for its search autocomplete.
    Returns a list of (slug, name, year). Cached after the first fetch.
    """
    global _pcs_race_list
    if _pcs_race_list is not None:
        return _pcs_race_list
    try:
        response = page.goto(
            "https://www.procyclingstats.com/search_list26.js",
            wait_until="domcontentloaded", timeout=30_000,
        )
        if not response or response.status != 200:
            _pcs_race_list = []
            return []
    except Exception:
        _pcs_race_list = []
        return []
    content = page.inner_text("body")
    races = []
    for m in re.finditer(r'\["race","([^"]+)","([^"]+)",\d+\]', content):
        slug_year = m.group(1)   # e.g. "giro-di-sardegna/2026"
        name = m.group(2)
        parts = slug_year.split("/")
        if len(parts) == 2:
            races.append((parts[0], name, parts[1]))
    _pcs_race_list = races
    return races


def _race_tokens(name: str) -> set[str]:
    STOP = {"de", "la", "le", "du", "di", "da", "van", "the", "a", "al", "en", "et"}
    return set(re.sub(r"[^a-z0-9 ]", "", name.lower()).split()) - STOP


def search_pcs_slug(page, race_name: str, year: str) -> str | None:
    """
    Find the PCS slug for a race by matching against PCS's own race index (search_list26.js).
    Strips stage/prologue prefixes and uses token overlap to find the best match.
    """
    m = re.match(r"\d+\. etape af (.+)", race_name, re.IGNORECASE)
    query = m.group(1).strip() if m else race_name
    m = re.match(r"prologen til (.+)", query, re.IGNORECASE)
    if m:
        query = m.group(1).strip()

    races = _load_pcs_race_list(page)
    query_tokens = _race_tokens(query)
    if not query_tokens:
        return None

    best_slug, best_score = None, 0
    for slug, name, race_year in races:
        if race_year != year:
            continue
        score = len(query_tokens & _race_tokens(name))
        if score > best_score:
            best_score = score
            best_slug = slug

    if best_score > 0:
        return best_slug

    # JS index found nothing — fall back to Google search
    return _search_pcs_slug_via_google(page, query, year)


def _search_pcs_slug_via_google(page, race_name: str, year: str) -> str | None:
    """
    Last-resort fallback: Google 'site:procyclingstats.com/race {race_name} {year}'
    and extract the slug from the first matching result URL.
    """
    import urllib.parse
    q = urllib.parse.quote_plus(f"site:procyclingstats.com/race {race_name} {year}")
    try:
        response = page.goto(f"https://www.google.com/search?q={q}",
                             wait_until="domcontentloaded", timeout=30_000)
        if not response or response.status != 200:
            return None
    except Exception:
        return None
    page.wait_for_timeout(1500)
    for link in page.locator("a").all():
        href = link.get_attribute("href") or ""
        m = re.search(r"procyclingstats\.com/race/([^/&?]+)/" + year, href)
        if m:
            return m.group(1)
    return None


# ---------------------------------------------------------------------------
# Name matching
# ---------------------------------------------------------------------------

PARTICLES = {"van", "de", "der", "den", "du", "del", "di", "da", "la", "le", "el", "los", "von", "af"}


def normalize(name: str) -> set[str]:
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    return {t for t in name.lower().split() if t not in PARTICLES}


def names_match(predicted: str, found: str) -> bool:
    return bool(normalize(predicted) & normalize(found))


# ---------------------------------------------------------------------------
# Winner extraction from PCS
# ---------------------------------------------------------------------------

def is_cancelled(page) -> bool:
    """Returns True if the PCS page indicates the stage/race was cancelled."""
    text = page.inner_text("body").lower()
    return any(kw in text for kw in ("cancelled", "annulled", "neutralized", "stage cancelled"))


def extract_winner(page) -> tuple[str | None, str | None]:
    """
    Returns (rider_winner, team_winner) from the stage result tables —
    the non-Prev table(s). Standings tables always have a Prev column.
    - rider_winner: from the first non-Prev table whose headers include 'Rider'
    - team_winner: from the first non-Prev table whose headers include 'Team' but not 'Rider' (TTT team result)
    """
    rider_winner = None
    team_winner = None
    for table in page.locator("table.results").all():
        headers = [th.inner_text().strip() for th in table.locator("thead th").all()]
        if "Prev" in headers:
            continue
        first_row = table.locator("tbody tr:first-child")
        if "Rider" in headers and rider_winner is None:
            link = first_row.locator("a").first
            if link.count() > 0:
                text = link.inner_text(timeout=3_000).strip()
                if text:
                    rider_winner = text
        elif "Team" in headers and "Rider" not in headers and team_winner is None:
            # Team-only table (TTT team result)
            idx = headers.index("Team")
            cells = first_row.locator("td").all()
            if idx < len(cells):
                text = cells[idx].inner_text(timeout=3_000).strip()
                if text:
                    team_winner = text
    return rider_winner, team_winner


def extract_winner_from_startlist(page, base_url: str) -> str | None:
    """
    Fallback: try the /statistics/start page which uses a plain <table> (not table.results)
    with columns [#, Rider, Team, Time]. The first row is the race winner.
    """
    stats_url = base_url.rstrip("/") + "/statistics/start"
    try:
        response = page.goto(stats_url, wait_until="domcontentloaded", timeout=30_000)
        if not response or response.status != 200:
            return None
    except Exception:
        return None
    page.wait_for_timeout(1500)
    for table in page.locator("table").all():
        headers = [th.inner_text().strip() for th in table.locator("th").all()]
        if "Rider" not in headers:
            continue
        link = table.locator("tbody tr:first-child a").first
        if link.count() > 0:
            text = link.inner_text(timeout=3_000).strip()
            if text:
                return text
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
                    year = (row["date"] or "2026")[:4]
                    print(f"  404 — searching PCS for '{race_name}'...")
                    found_slug = search_pcs_slug(page, race_name, year)
                    if found_slug:
                        url = build_url_from_slug(race_name, found_slug, year)
                        print(f"  Retrying with: {url}")
                        try:
                            response = page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                            if response and response.status == 404:
                                print(f"  [WARNING] Still 404 after search for id={row['id']}")
                                unmatched += 1
                                time.sleep(2)
                                continue
                        except Exception as e:
                            print(f"  [WARNING] Retry failed for id={row['id']}: {e}")
                            unmatched += 1
                            time.sleep(2)
                            continue
                    else:
                        print(f"  [WARNING] No PCS result found via search for id={row['id']} race='{race_name}'")
                        unmatched += 1
                        time.sleep(2)
                        continue
            except Exception:
                year = (row["date"] or "2026")[:4]
                print(f"  Navigation failed — searching PCS for '{race_name}'...")
                found_slug = search_pcs_slug(page, race_name, year)
                if found_slug:
                    url = build_url_from_slug(race_name, found_slug, year)
                    print(f"  Retrying with: {url}")
                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                    except Exception as e:
                        print(f"  [WARNING] Retry failed for id={row['id']}: {e}")
                        unmatched += 1
                        time.sleep(2)
                        continue
                else:
                    print(f"  [WARNING] No PCS result found via search for id={row['id']} race='{race_name}'")
                    unmatched += 1
                    time.sleep(2)
                    continue

            # Give the page a moment to render the table
            page.wait_for_timeout(1500)

            if is_cancelled(page):
                print(f"  [CANCELLED] Stage/race was cancelled — skipping id={row['id']}")
                skipped += 1
                time.sleep(2)
                continue

            rider_winner, team_winner = extract_winner(page)
            if rider_winner is None and team_winner is None:
                print(f"  No result table found, trying /statistics/start fallback...")
                rider_winner = extract_winner_from_startlist(page, url)

            predicted = row["predicted_winner"] or ""
            # Prefer whichever winner matches the prediction.
            # Only fall back to team_winner if it explicitly matches (TTT team prediction).
            # Default to rider_winner when neither matches, to avoid ITT/TTT confusion.
            if rider_winner and names_match(predicted, rider_winner):
                winner = rider_winner
            elif team_winner and names_match(predicted, team_winner):
                winner = team_winner
            else:
                winner = rider_winner

            if winner:
                correct = 1 if names_match(predicted, winner) else 0
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
