# Hit Rate of Predictions — Implementation Plan

## Context
Build a tool that tracks Emil Axelgaard's men's cycling predictions and calculates his all-time hit rate. Predictions are scraped from TV2.dk and Feltet.dk. His top pick is always marked with 5 stars (⭐⭐⭐⭐⭐). Results come from ProCyclingStats.com. Output is a web dashboard.

Project folder: `~/Desktop/Projects/Hit Rate of Predictions/`

## Key Findings
- **TV2.dk** author page: `https://sport.tv2.dk/profil/emil-axels` — JS-rendered, needs headless browser
- **Feltet.dk** author page: `https://feltet.dk/author/emil-axelgaard/` — JS-rendered, needs headless browser
- **Feltet.dk results** — `feltet.dk/rdb/løbskalender/{id}` loads via plain HTTP; results embedded as JSON. Used as primary results source.
- **ProCyclingStats.com** — race pages return 403 on plain HTTP; Playwright bypasses this. Used as fallback when Feltet doesn't have the race. URL pattern: `/race/[slug]/[year]/[result|gc|points-classification|kom-classification|youth-classification]`
- Prediction format: line starting with ⭐⭐⭐⭐⭐ followed by rider name

## Tech Stack
- **Language:** Python
- **Scraping:** Playwright / Patchright (handles JS-rendered pages; patchright used for PCS to bypass Cloudflare)
- **Data storage:** CSV or SQLite (to be decided)
- **Dashboard:** Streamlit (simplest Python dashboard)

## Implementation Steps

### Phase 1 — TV2 Scraper (start here)
1. Use Playwright to load `sport.tv2.dk/profil/emil-axels` and collect all article URLs
2. For each article, extract: race name, date, predicted winner (5-star line)
3. Save to data file (CSV or SQLite, to be decided)

<details>
<summary>Detailed implementation plan</summary>

#### Scraper Flow (`scraper.py`)

**Step 1 — Collect article URLs**
1. Launch Playwright, navigate to author page
2. Dismiss GDPR cookie consent banner
3. Loop: click "Load more" button, wait for new articles to render, repeat until button is gone
4. Collect all article URLs from the fully-loaded page
5. Filter to cycling prediction articles by URL pattern (e.g. `/cykling/`) or title keywords

**Step 2 — Per-article extraction**
For each URL not already in the database:
1. Navigate to the article
2. Extract: article title (→ race name), publication date, `⭐⭐⭐⭐⭐` line (→ predicted winner)
3. Save to SQLite
4. Wait 1–2 seconds between requests (rate limiting)

#### SQLite Schema (`data/predictions.db`)

```sql
CREATE TABLE predictions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT UNIQUE,
    race_name TEXT,
    date TEXT,
    predicted_winner TEXT,
    actual_winner TEXT,      -- filled in Phase 2
    correct INTEGER,         -- NULL until Phase 2; 1=correct, 0=wrong
    scraped_at TEXT
);
```

`UNIQUE` on `url` allows re-running safely — already-seen articles are skipped via `INSERT OR IGNORE`.

#### Files to Create
- `scraper.py` — TV2 scraper
- `data/predictions.db` — SQLite database

#### Verification
1. Run `python scraper.py` — should open Chromium, click through all "Load more" pages, scrape each article
2. Check DB: `sqlite3 data/predictions.db "SELECT * FROM predictions LIMIT 10;"` — rows should have race_name, date, predicted_winner populated
3. Re-run scraper — should add 0 new rows (idempotent)

#### Open Questions
- **Article filtering:** What URL patterns identify prediction articles? (does `/cykling/` appear in all prediction URLs?)
- **Pagination volume:** 100+ articles — confirm "Load more" approach covers all of them

</details>

### Phase 2 — Results matching
Use **patchright** (drop-in Playwright replacement that bypasses Cloudflare JS challenges) to scrape ProCyclingStats race result pages directly. The script is source-agnostic — it processes any row where `actual_winner IS NULL`, so it works now against the TV2 predictions and again after Phase 4 adds Feltet predictions.

1. For each unmatched row, construct the PCS URL from a stored `pcs_url` column (or a `race_overrides.json` mapping)
2. Use patchright to load the page and extract the winner from the results table
3. Fuzzy-match the winner name against `predicted_winner`, mark `correct`
4. Print loud warnings for any row where no result is found

<details>
<summary>Detailed implementation plan</summary>

#### Key Facts
- Emil predicts: one-day race winners, stage race GC winners, and jersey winners (points/KOM/youth)
- Script is re-runnable at any time — only processes rows where `actual_winner IS NULL` (idempotent)
- Same script handles TV2 predictions now and Feltet predictions later
- Requires patchright (`pip install patchright && patchright install chromium`) — PCS blocks plain Playwright with Cloudflare

#### Scraping Strategy
Go directly to the ProCyclingStats result page for each race:
- URL pattern: `https://www.procyclingstats.com/race/[slug]/[year]/[result|gc|…]`
- Uses **patchright** (not plain Playwright) to bypass Cloudflare bot protection
- Installed via: `pip install patchright && patchright install chromium`
- Import: `from patchright.sync_api import sync_playwright` — no other code changes vs Playwright

#### Schema Update
Add two columns to `predictions`:

```sql
ALTER TABLE predictions ADD COLUMN result_source TEXT;
-- URL or domain where result was found, useful for debugging

ALTER TABLE predictions ADD COLUMN prediction_type TEXT;
-- 'one_day', 'gc', 'points', 'kom', 'youth' — extracted from article text
```

`prediction_type` is extracted from the article text (e.g. "bjergtrøjen" → `kom`, "pointtrøjen" → `points`, "sammenlagt" → `gc`, otherwise → `one_day`).

#### Scraper Flow (`results.py`)

1. Read all rows from DB where `actual_winner IS NULL`
2. For each row:
   a. Extract year from `date` field
   b. Build search query from `race_name` + year + `prediction_type`
   c. Search the web, extract winner name from top result snippet or page
   d. Fuzzy-match winner against `predicted_winner` (normalize to lowercase, strip accents, compare last name)
   e. If confident match found: update `actual_winner`, `result_source`, set `correct`
   f. **If not found: print a loud WARNING** with row id, race_name, and the query used — leave `actual_winner` NULL
3. Rate-limit between requests to avoid blocks

#### Name Matching
Rider names may differ in order (`Pogačar Tadej` vs `Tadej Pogačar`) — normalize both to lowercase, strip accents, compare on last name as minimum signal.

#### Verification
1. Run `python results.py` — processes all NULL rows
2. Check DB: `sqlite3 data/predictions.db "SELECT race_name, predicted_winner, actual_winner, correct, result_source FROM predictions LIMIT 10;"`
3. Review WARNINGs for unmatched races — adjust search query or manually fill `actual_winner`
4. Re-run `results.py` — only processes remaining NULL rows

</details>

### Phase 3 — Dashboard
1. Build a Streamlit app showing:
   - Table of all predictions (race, date, predicted winner, actual winner, correct/wrong)
   - All-time hit rate percentage at the top

### Phase 4 — Feltet.dk (later)
- Add historical predictions from Feltet.dk once TV2 is working
- Need to verify: does Feltet.dk require a subscription for older articles?

#### Scraping strategy — keyword filtering (no dedicated author page)
Feltet doesn't have a usable Emil Axelgaard author page — filtering must happen at two levels:

**URL / article discovery**
- Browse or search Feltet's site and collect article links
- Pre-filter by title/slug keyword: **"Optakt"** (appears in all prediction article titles)

**Article-level validation** (inside the per-article extractor)
- Check author byline contains **"Emil Axelgaard"** — discard articles by other authors
- Check title contains **"Optakt"** — discard non-prediction articles

**Why two-level filtering:** Feltet publishes "optakt" articles from multiple authors. Filtering by title alone would pull in other writers' previews. Combining author + title keyword ensures only Emil Axelgaard's prediction pieces are captured.

**Implementation steps**
1. Start from a search URL or category page on feltet.dk
2. Collect all article links from the page
3. Pre-filter links where the slug or title contains "optakt"
4. For each candidate, fetch the page and verify author == "Emil Axelgaard" before parsing predictions

## Automation — CI Results Scraping Problem & Fix

### Problem: ProCyclingStats blocked in GitHub Actions

`results.py` uses **patchright** (a Playwright fork) to bypass Cloudflare on ProCyclingStats.com. This works fine when run locally on a residential IP. However, when run in GitHub Actions, the runner uses **Azure datacenter IPs**, which Cloudflare's bot detection specifically flags — even a fully headless patchright browser gets served a Managed Challenge page (HTTP 200, but body is "Performing security verification…" with no result table).

Confirmed by adding a page-body diagnostic: every PCS page returned the Cloudflare challenge string instead of race content.

**Sites investigated as alternatives:**
| Site | CDN | Cloudflare? |
|---|---|---|
| ProCyclingStats.com | Cloudflare | ✅ Yes — Managed Challenge, blocks CI |
| FirstCycling.com | Cloudflare | ✅ Yes — 403 immediately |
| Feltet.dk | Fastly/Varnish | ❌ No — plain JSON API |
| Cyclingnews.com | Fastly/Varnish | ❌ No — plain HTML |

### Fix: Cyclingnews results scraper (`results_cn.py`)

`https://www.cyclingnews.com/race-results/` lists recent race result articles. Each article link has an `aria-label` attribute in the format:

```
"Grand Prix de Denain: Alec Segaert toys with Hagenes and fends off peloton for extraordinary solo win"
```

The winner is always the first capitalised words after the colon (using known name particles like *van*, *de*, *del* to handle compound surnames). Cancelled races are detected by keywords ("cancelled", "neutralised", etc.) and marked in the DB.

**`results_cn.py`** fetches this page with plain `requests` (no browser needed), matches race names against DB rows using token-based fuzzy matching, extracts winners, handles cancelled races, and returns unmatched races.

**Integration:** `scraper_auto.py` calls `results_cn.main()` directly instead of `results.py`. Unmatched races are surfaced in the GitHub Actions job summary so they can be resolved locally with `results.py`.

**`results.py` (PCS scraper) is kept** for local use — it still works perfectly on a residential IP and handles edge cases (jersey classifications, stage races, prologue) that Cyclingnews may not always cover.

### Race classification in `results_cn.py`

`race_context` (`one_day` / `stage` / `gc`) and `race_format` (`rr` / `itt` / `None`) are derived directly from the race name — same logic as `results.py` — and written to the DB alongside the result. This enables race-type statistics in the dashboard without needing to visit any external page.

### GitHub Actions job summary

`scraper_auto.py` writes a markdown summary to `$GITHUB_STEP_SUMMARY` after each run:
- **New/updated predictions** scraped from TV2
- **Results matched** — each race with ✅ (correct) or ❌ (wrong prediction)
- **Cancelled races** — marked with 🚫
- **Unmatched races** — flagged with ⚠️ and a note that the race has likely not been run yet, or to run `results.py` locally

## Open Questions
- **TV2 pagination:** "Load more" button confirmed — need to verify it covers all 100+ articles in practice
- **Article filtering:** Does `/cykling/` appear in all prediction article URLs, or do we need title keyword filtering?
- **Prediction type detection:** Reliable Danish keywords in article text for jersey types? (e.g. "bjergtrøjen", "pointtrøjen", "sammenlagt") — verify against real articles
- **PCS name format:** Confirm PCS uses `Lastname Firstname` order in results tables — needed for matching
- **Feltet.dk:** Subscription required for historical articles?

## Files to Create
- `scraper.py` — scrapes predictions from TV2 (Feltet later)
- `results.py` — fetches actual race results
- `dashboard.py` — Streamlit dashboard
- `data/predictions.db` — SQLite database (decided: SQLite over CSV)
- `data/race_overrides.json` — manual PCS URL overrides for edge cases (Danish names, unusual slugs)
