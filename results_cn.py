"""
results_cn.py — fetch race results from Cyclingnews (no Cloudflare, plain HTTP).

Extracts winners from article aria-labels on the Cyclingnews race-results
listing page. Updates the predictions DB. Returns unmatched races so
scraper_auto.py can surface them in the GitHub Actions job summary.

Usage:
    python results_cn.py              # process all NULL rows, 1 page
    python results_cn.py --pages 3   # search first 3 listing pages
    python results_cn.py --dry-run   # print matches without writing to DB
"""

import argparse
import re
import sqlite3
import unicodedata
from pathlib import Path

import requests

DB_PATH = Path(__file__).parent / "data" / "predictions.db"
CN_RESULTS_URL = "https://www.cyclingnews.com/race-results/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
}

NAME_PARTICLES = {"van", "de", "der", "den", "du", "di", "da", "del", "el", "la", "le", "af"}
RACE_STOP = {"de", "la", "le", "du", "di", "da", "van", "the", "a", "al", "en", "et"}

# Maps DB race names to the name CN uses in article titles, for cases where
# token matching fails (e.g. "Milano-Sanremo" vs CN's "Milan-San Remo").
CN_RACE_ALIASES: dict[str, str] = {
    "Milano-Sanremo": "Milan-San Remo",
}

CANCELLATION_KEYWORDS = (
    "cancelled", "canceled", "neutralised", "neutralized",
    "abandoned", "annulled", "called off", "not held",
)


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

def fetch_articles(pages: int = 1) -> list[tuple[str, str]]:
    """
    Fetch article listing from Cyclingnews race-results pages.
    Returns list of (cn_race_name, description) parsed from aria-label attributes.
    e.g. ("Grand Prix de Denain", "Alec Segaert toys with Hagenes and fends off...")
    """
    articles = []
    for page_num in range(1, pages + 1):
        url = CN_RESULTS_URL if page_num == 1 else f"{CN_RESULTS_URL}?page={page_num}"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
        except Exception as e:
            print(f"  [WARN] Could not fetch {url}: {e}")
            break
        for m in re.finditer(r'class="article-link"[^>]+aria-label="([^"]+)"', resp.text):
            label = m.group(1)
            if ":" in label:
                race_part, _, desc = label.partition(":")
                articles.append((race_part.strip(), desc.strip()))
    return articles


# ---------------------------------------------------------------------------
# Race name matching
# ---------------------------------------------------------------------------

def _normalize(name: str) -> str:
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z0-9\s]", " ", name.lower())


def _race_tokens(name: str) -> frozenset[str]:
    return frozenset(_normalize(name).split()) - RACE_STOP


def races_match(db_name: str, cn_name: str) -> bool:
    """
    True if db_name and cn_name refer to the same race.
    Token-subset logic handles subtitle variants and sponsor name differences.
    """
    a = _race_tokens(db_name)
    b = _race_tokens(cn_name)
    if not a or not b:
        return False
    shared = a & b
    return a <= b or b <= a or len(shared) >= min(2, min(len(a), len(b)))


def _stage_number(race_name: str) -> int | None:
    m = re.match(r"(\d+)\. etape af ", race_name, re.IGNORECASE)
    return int(m.group(1)) if m else None


def _stage_base(race_name: str) -> str | None:
    m = re.match(r"\d+\. etape af (.+)", race_name, re.IGNORECASE)
    return m.group(1).strip() if m else None


def find_article(race_name: str, articles: list[tuple[str, str]]) -> tuple[str, str] | None:
    """
    Find the best matching CN article for a DB race name.
    Handles one-day races, stage races, and GC results.
    """
    stage_num = _stage_number(race_name)
    base_name = _stage_base(race_name) if stage_num else None
    lookup_name = CN_RACE_ALIASES.get(race_name, race_name)

    for cn_race, cn_desc in articles:
        if stage_num is not None:
            if not races_match(base_name, cn_race):
                continue
            desc_lower = cn_desc.lower()
            if re.search(rf"\bstage[- ]{stage_num}\b", desc_lower) or \
               re.search(rf"\b{stage_num}(st|nd|rd|th)? stage\b", desc_lower):
                return cn_race, cn_desc
        else:
            if races_match(lookup_name, cn_race):
                return cn_race, cn_desc

    return None


# ---------------------------------------------------------------------------
# Winner / cancellation extraction
# ---------------------------------------------------------------------------

def is_cancelled(desc: str) -> bool:
    """True if the article description indicates the race was cancelled/neutralised."""
    desc_lower = desc.lower()
    return any(kw in desc_lower for kw in CANCELLATION_KEYWORDS)


def extract_winner(desc: str) -> str | None:
    """
    Extract rider name from the description (text after the colon in the CN title).
    Takes capitalised words + known particles until the first lowercase non-particle word.
      "Alec Segaert toys with Hagenes..."  → "Alec Segaert"
      "Mathieu van der Poel wins..."        → "Mathieu van der Poel"
      "Isaac del Toro seals..."             → "Isaac del Toro"
    Returns None if fewer than 2 name words are found.
    """
    words = desc.split()
    name_words: list[str] = []
    for word in words:
        clean = re.sub(r"[^a-zA-ZÀ-ÿ'\-]", "", word)
        if not clean:
            break
        if clean.lower() in NAME_PARTICLES:
            if name_words:
                name_words.append(clean)
        elif clean[0].isupper():
            name_words.append(clean)
        else:
            break
    while name_words and name_words[-1].lower() in NAME_PARTICLES:
        name_words.pop()
    return " ".join(name_words) if len(name_words) >= 2 else None


def names_match(predicted: str, found: str) -> bool:
    """True if predicted and found share at least one meaningful name token."""
    def tokens(n: str) -> set[str]:
        n = unicodedata.normalize("NFD", n)
        n = "".join(c for c in n if unicodedata.category(c) != "Mn")
        return {t for t in n.lower().split() if t not in NAME_PARTICLES}
    return bool(tokens(predicted) & tokens(found))


# ---------------------------------------------------------------------------
# Race classification
# ---------------------------------------------------------------------------

_ITT_KEYWORDS = ("enkeltstart", "chrono", "time trial", "itt", "contre-la-montre")
_JERSEY_KEYWORDS = ("konkurrencen",)

KNOWN_STAGE_RACES = {"Tour des Alpes-Maritimes", "Tour des Alpes"}


def build_stage_races(conn: sqlite3.Connection) -> set[str]:
    """Build set of base race names that have stage entries in the DB."""
    stage_races: set[str] = KNOWN_STAGE_RACES.copy()
    for (name,) in conn.execute("SELECT race_name FROM predictions"):
        m = re.match(r"\d+\. etape af (.+)", name, re.IGNORECASE)
        if m:
            stage_races.add(m.group(1).strip())
    return stage_races


def get_race_context(race_name: str, stage_races: set[str]) -> str:
    """Return 'stage', 'gc', or 'one_day'."""
    if re.match(r"\d+\. etape af .+", race_name, re.IGNORECASE):
        return "stage"
    if re.match(r"prologen til .+", race_name, re.IGNORECASE):
        return "stage"
    if re.search(r"\w+konkurrencen\b", race_name, re.IGNORECASE):
        return "gc"
    if race_name in stage_races:
        return "gc"
    return "one_day"


def get_race_format(race_name: str, race_context: str) -> str | None:
    """Return 'itt', or 'rr'. GC races return None (no single format)."""
    if race_context == "gc":
        return None
    if any(kw in race_name.lower() for kw in _ITT_KEYWORDS):
        return "itt"
    return "rr"


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def fetch_null_rows(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT id, race_name, date, predicted_winner FROM predictions "
        "WHERE actual_winner IS NULL AND (cancelled IS NULL OR cancelled = 0)"
    ).fetchall()
    return [{"id": r[0], "race_name": r[1], "date": r[2], "predicted_winner": r[3]} for r in rows]


def update_result(conn: sqlite3.Connection, row_id: int, actual_winner: str, correct: int,
                  race_context: str, race_format: str | None) -> None:
    conn.execute(
        "UPDATE predictions SET actual_winner=?, correct=?, result_source=?, "
        "race_context=?, race_format=? WHERE id=?",
        (actual_winner, correct, "cyclingnews.com", race_context, race_format, row_id),
    )
    conn.commit()


def mark_cancelled(conn: sqlite3.Connection, row_id: int,
                   race_context: str, race_format: str | None) -> None:
    conn.execute(
        "UPDATE predictions SET cancelled=1, result_source=?, race_context=?, race_format=? WHERE id=?",
        ("cyclingnews.com", race_context, race_format, row_id),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(pages: int = 1, dry_run: bool = False) -> dict:
    """
    Match and update results from Cyclingnews.
    Returns a dict with keys: matched, cancelled, unmatched — each a list of row dicts
    (matched/cancelled rows include 'actual_winner' and 'correct' fields).
    """
    conn = sqlite3.connect(DB_PATH)
    rows = fetch_null_rows(conn)
    if not rows:
        print("Nothing to do.")
        conn.close()
        return {"matched": [], "cancelled": [], "unmatched": []}

    stage_races = build_stage_races(conn)

    print(f"Processing {len(rows)} unresolved rows against Cyclingnews ({pages} page(s))...")
    articles = fetch_articles(pages)
    print(f"Fetched {len(articles)} articles.")

    matched_rows: list[dict] = []
    cancelled_rows: list[dict] = []
    unmatched: list[dict] = []

    for row in rows:
        race_name = row["race_name"]
        predicted = row["predicted_winner"] or ""
        race_context = get_race_context(race_name, stage_races)
        race_format = get_race_format(race_name, race_context)

        article = find_article(race_name, articles)
        if article is None:
            unmatched.append(row)
            continue

        cn_race, cn_desc = article

        if is_cancelled(cn_desc):
            print(f"  [CANCELLED] {race_name}")
            if not dry_run:
                mark_cancelled(conn, row["id"], race_context, race_format)
            cancelled_rows.append(row)
            continue

        winner = extract_winner(cn_desc)
        if winner is None:
            print(f"  [WARN] Could not extract winner for '{race_name}' from: {cn_desc[:80]}")
            unmatched.append(row)
            continue

        correct = 1 if names_match(predicted, winner) else 0
        symbol = "✓" if correct else "✗"
        print(f"  [{symbol}] {race_name}: {winner} (predicted: {predicted})")

        if not dry_run:
            update_result(conn, row["id"], winner, correct, race_context, race_format)
        matched_rows.append({**row, "actual_winner": winner, "correct": correct})

    conn.close()
    print(f"\nDone. Matched: {len(matched_rows)}, Cancelled: {len(cancelled_rows)}, Unmatched: {len(unmatched)}.")
    return {"matched": matched_rows, "cancelled": cancelled_rows, "unmatched": unmatched}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pages", type=int, default=1)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    result = main(pages=args.pages, dry_run=args.dry_run)
    if result["unmatched"]:
        print("\nUnmatched races (run results.py locally):")
        for r in result["unmatched"]:
            print(f"  - {r['race_name']} (predicted: {r['predicted_winner']})")
