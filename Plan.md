# Hit Rate of Predictions — Implementation Plan

## Context
Build a tool that tracks Emil Axelgaard's men's cycling predictions and calculates his all-time hit rate. Predictions are scraped from TV2.dk and Feltet.dk. His top pick is always marked with 5 stars (⭐⭐⭐⭐⭐). Results come from ProCyclingStats.com. Output is a web dashboard.

Project folder: `~/Desktop/Projects/Hit Rate of Predictions/`

## Key Findings
- **TV2.dk** author page: `https://sport.tv2.dk/profil/emil-axels` — JS-rendered, needs headless browser
- **Feltet.dk** author page: `https://feltet.dk/author/emil-axelgaard/` — JS-rendered, needs headless browser
- **ProCyclingStats.com** — returns 403 on direct requests, needs headless browser or alternative
- Prediction format: line starting with ⭐⭐⭐⭐⭐ followed by rider name

## Tech Stack
- **Language:** Python
- **Scraping:** Playwright (handles JS-rendered pages)
- **Data storage:** CSV or SQLite (to be decided)
- **Dashboard:** Streamlit (simplest Python dashboard)

## Implementation Steps

### Phase 1 — TV2 Scraper (start here)
1. Use Playwright to load `sport.tv2.dk/profil/emil-axels` and collect all article URLs
2. For each article, extract: race name, date, predicted winner (5-star line)
3. Save to data file (CSV or SQLite, to be decided)

### Phase 2 — Results matching
1. Find the actual race winner for each prediction
2. ProCyclingStats returns 403 on direct requests — need to verify if Playwright bypasses this, or find an alternative source
3. Compare predicted vs actual, mark correct/wrong, update data file

### Phase 3 — Dashboard
1. Build a Streamlit app showing:
   - Table of all predictions (race, date, predicted winner, actual winner, correct/wrong)
   - All-time hit rate percentage at the top

### Phase 4 — Feltet.dk (later)
- Add historical predictions from Feltet.dk once TV2 is working
- Need to verify: does Feltet.dk require a subscription for older articles?

## Open Questions
- TV2 author page pagination — load more button or numbered pages? (check manually)
- ProCyclingStats — does Playwright bypass the 403, or do we need an alternative results source?
- Feltet.dk — subscription required for historical articles?

## Files to Create
- `scraper.py` — scrapes predictions from TV2 (Feltet later)
- `results.py` — fetches actual race results
- `dashboard.py` — Streamlit dashboard
- `data/predictions.csv` or `data/predictions.db` — stored predictions + results (to be decided)
