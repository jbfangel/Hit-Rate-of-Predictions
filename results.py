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
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
import unicodedata
from pathlib import Path

from patchright.sync_api import sync_playwright

DB_PATH = Path(__file__).parent / "data" / "predictions.db"
CAPTCHA_WAIT_TIMEOUT = 10  # seconds to wait for manual CAPTCHA solving


def wait_for_enter(timeout: int = CAPTCHA_WAIT_TIMEOUT) -> bool:
    """Wait for the user to press Enter, with a timeout. Returns True if Enter was pressed."""
    pressed = [False]

    def _read():
        try:
            input()
            pressed[0] = True
        except Exception:
            pass

    t = threading.Thread(target=_read, daemon=True)
    t.start()
    t.join(timeout)
    return pressed[0]
BASE = "https://www.procyclingstats.com/race"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


SLUG_OVERRIDES = {
    # Classics / one-day
    "Ename Samyn Classic":                                          "gp-samyn",
    "Kuurne-Bruxelles-Kuurne":                                      "kuurne-brussel-kuurne",
    "Omloop Nieuwsblad":                                            "omloop-het-nieuwsblad",
    "Etoile de Bessèges – Tour du Gard":                            "etoile-de-besseges",
    "Faun Drome Classic":                                           "la-drome-classic",
    "Faun-Ardèche Classic":                                         "faun-ardeche-classic",
    "Classic Var":                                                  "classic-var",
    "Figueira Champions Classic":                                   "figueira-champions-classic",
    "Clasica Jaén":                                                 "clasica-jaen-paraiso-interior",
    "Trofeo Mallorca Fashion Outlet-Paseo Maritimo Palma":          "trofeo-palma",
    "Trofeo Andratx-Mirador d'es Colomer":                         "trofeo-pollenca-port-d-andratx",
    "Trofeo Andratx-Mirador d'Es Colomer":                         "trofeo-pollenca-port-d-andratx",
    "Trofeo Serra de Tramuntana":                                   "deia-trophy",
    "Trofeo Ses Salines- Colònia de Sant Jordi":                   "trofeo-ses-salines-felanitx",
    "Trofeo Ses Salines-Colònia de Sant Jordi":                    "trofeo-ses-salines-felanitx",
    "Trofeo Tessile & Moda - Valdengo Oropa":                      "trofeo-baracchi",
    "Classica Comunitat Valenciana - Gran Premi Valencia":          "gp-de-valence",
    "Gran Premio Castellon – Ruta de la Ceramica":                  "ruta-de-la-ceramica-gran-premio-castellon",
    "Utsunomiya Japan Cup Road Race":                               "japan-cup",
    "Grand Prix Cycliste de Marseille la Marseillaise":             "gp-d-ouverture",
    "Mapei Cadel Evans Great Ocean Road Race":                      "great-ocean-road-race",
    "Cadel Evans Great Ocean Road Race":                            "great-ocean-road-race",
    "Binche-Chimay-Binche / Memorial Frank Vandenbroucke":          "memorial-frank-vandenbroucke",
    "Coppa Bernocchi – GP Banco BPM":                              "coppa-bernocchi",
    "Coppa Agostoni – Giro delle Brianza":                         "coppa-agostoni",
    "Sparkassen Münsterland Giro":                                  "munsterland-giro",
    "Paris-Chauny":                                                 "paris-chauny-classique",
    "Lotto Gooikse Pijl":                                          "gooikse-pijl",
    "Super 8 Classic":                                              "gp-impanis-van-petegem",
    "Kampioenschap van Vlaanderen":                                 "kampioenschap-van-vlaanderen1",
    "Grand Prix de Wallonie":                                       "gp-de-wallonie",
    "Grand Prix Cycliste de Montréal":                              "gp-montreal",
    "Grand Prix Cycliste de Québec":                                "gp-quebec",
    "Gran Premio città di Peccioli - Coppa Sabatini":              "coppa-sabatini",
    "Giro della Toscana":                                           "giro-di-toscana",
    "Bretagne Classic – Ouest France":                              "bretagne-classic",
    "Tour du Poitou-Charentes en Nouvelle Aquitaine":               "tour-du-poitou-charentes-et-de-la-vienne",
    "GP de Fourmies / La Voix du Nord":                            "gp-de-fourmies",
    "Muur Classic Geraardsbergen":                                  "muur-classic-geraardsbergen",
    "ADAC Cyclassics":                                              "cyclassics-hamburg",
    "Circuit Franco-Belge":                                         "circuit-franco-belge",
    "La Polynormande":                                              "la-poly-normande",
    "Omloop van het Houtland":                                      "omloop-van-het-houtland-lichtervelde",
    "Grand Prix d'Isbergues – Pas de Calais":                      "gp-d-isbergues",
    "Lotto Famenne Ardenne Classic":                                "famenne-ardenne-classic",
    "Elfstedenronde Brugge":                                        "circuit-des-xi-villes",
    "Duracell Dwars door Het Hageland":                             "dwars-door-het-hageland",
    "Grosser Preis des Kantons Aargau":                             "gp-du-canton-d-argovie",
    "Classique Dunkerque / Grand prix des Hauts de France":         "classique-dunkerque",
    "Grand Prix du Morbihan":                                       "gp-de-plumelec",
    "Tour du Finistère Pays de Quimper":                           "tour-du-finistere",
    "Boucles de l'Aulne – Châteaulin":                             "boucles-de-l-aulne",
    "Mercan'Tour Classic Alpes-Maritimes":                          "mercan-tour-classic-alpes-maritimes",
    "De Brabantse Pijl - La Flèche Brabançonne":                   "brabantse-pijl",
    "Ronde van Limburg":                                            "ronde-van-limburg",
    "Gran Premio Miguel Indurain":                                  "gp-miguel-indurain",
    "Route Adelié de Vitré":                                       "route-adélie-de-vitré",
    "Paris-Camembert":                                              "paris-camembert",
    "La Roue Tourangelle Centre Val de Loire – Groupama P.V.L.":   "la-roue-tourangelle",
    "Cholet Agglo Tour":                                            "cholet-pays-de-loire",
    "Grand Prix de Denain":                                         "gp-de-denain",
    "Danilith Nokere Koerse":                                       "nokere-koerse",
    "Volta NXT Classic":                                            "volta-nxt-classic",
    "Gent-Wevelgem In Flanders Fields":                             "gent-wevelgem",
    "E3 Saxo Classic":                                              "e3-harelbeke",
    "Classic Brugge-De Panne":                                      "brugge-de-panne",
    "Scheldeprijs":                                                 "scheldeprijs",
    "Lotto Famenne Ardenne Classic":                                "famenne-ardenne-classic",
    "Circuito de Getxo – Memorial Hermanos Otxoa":                 "circuito-de-getxo",
    "Donostia San Sebastian Klasikoa":                              "san-sebastian",
    "Clasica Ciclista a Castilla y Leon":                           "vuelta-a-castilla-y-leon",
    "Clàssica Terres de l'Ebre":                                   "clasica-terres-de-l-ebre",
    "Andorra Morabanc Clàssica":                                    "classica-andorra",
    "Copenhagen Sprint":                                            "copenhagen-sprint",
    "Giro dell'Appennino":                                          "giro-dell-appennino",
    "Tour du Doubs":                                                "tour-du-doubs",
    "Tour du Jura Cycliste":                                        "tour-du-jura",
    "Classic Grand Besançon Doubs":                                 "classic-grand-besancon-doubs",
    "Tro-Bro Léon":                                                "tro-bro-leon",
    "Lotto Famenne Ardenne Classic":                                "famenne-ardenne-classic",
    "Maryland Cycling Classic":                                     "maryland-cycling-classic",
    "Giro dell'Emilia":                                             "giro-dell-emilia",
    "Paris-Tours Elite":                                            "paris-tours",
    "Tour de Vendée":                                               "tour-de-vendee",
    "Gran Piemonte":                                                "gran-piemonte",
    "Tre Valli Varesine":                                           "tre-valli-varesine",
    "GP Industria & Artigianato":                                   "gp-industria-artigianato-larciano",
    "Petronas Le Tour de Langkawi":                                 "tour-de-langkawi",
    "La Flèche Wallonne":                                           "fleche-wallonne",
    "Tour du Doubs":                                                "tour-du-doubs",
    "Veneto Classic":                                               "veneto-classic",
    "Eschborn-Frankfurt":                                           "eschborn-frankfurt",
    "Giro del Veneto":                                              "giro-del-veneto",
    "Chrono des Nations":                                           "chrono-des-nations",
    "Muscat Classic":                                               "muscat-classic",
    "La Roue Tourangelle Centre Val de Loire – Groupama P.V.L.":   "la-roue-tourangelle",

    # Stage races
    "Giro della Sardegna":                                          "giro-di-sardegna",
    "Vuelta a Andalucia Ruta Ciclista del Sol":                     "ruta-del-sol",
    "Volta ao Algarve em Bicicleta":                                "volta-ao-algarve",
    "Vuelta a la Region de Murcia Costa Calida":                    "vuelta-ciclista-a-la-region-de-murcia",
    "CIC Tour de la Provence":                                      "tour-cycliste-international-la-provence",
    "Tour de la Provence":                                          "tour-cycliste-international-la-provence",
    "Tour des Alpes-Maritimes":                                     "tour-des-alpes-maritimes-et-du-var",
    "Volta a la Comunitat Valenciana":                              "vuelta-a-la-comunidad-valenciana",
    "Santos Tour Down Under":                                       "tour-down-under",
    "NIBC Tour of Holland":                                         "tour-of-holland",
    "Gree – Tour of Guangxi":                                       "tour-of-guangxi",
    "Okolo Slovenska/Tour de Slovaquie":                            "okolo-slovenska",
    "Skoda Tour de Luxembourg":                                     "tour-de-luxembourg",
    "Lloyds Bank Tour of Britain Men":                              "tour-of-britain",
    "Lidl Deutschland Tour":                                        "deutschland-tour",
    "Tour du Limousin-Périgord – Nouvelle Aquitaine":              "tour-du-limousin",
    "PostNord Danmark Rundt":                                       "tour-of-denmark",
    "Tour de l'Ain":                                                "tour-de-l-ain",
    "Ethias-Tour de Wallonie":                                      "tour-de-wallonie",
    "Giro d'Italia":                                                "giro-d-italia",
    "Presidential Cycling Tour of Türkiye":                         "tour-of-turkey",
    "Critérium du Dauphiné":                                        "dauphine",
    "Boucles de la Mayenne – Crédit Mutuel":                       "boucles-de-la-mayenne",
    "4 Jours de Dunkerque / Grand prix des Hauts de France":        "4-jours-de-dunkerque",
    "Il Giro d'Abruzzo":                                            "giro-d-abruzzo",
    "Région Pays de la Loire Tour":                                 "region-pays-de-la-loire",
    "Volta Ciclista a Catalunya":                                   "volta-a-catalunya",
    "Baloise Belgium Tour":                                         "tour-of-belgium",
    "La Route d'Occitanie – CIC":                                   "la-route-d-occitanie",
    "Vuelta Asturias Julio Alvarez Mendo":                          "vuelta-asturias",
    "Tour of the Alps":                                             "tour-of-the-alps",
    "Tour de Hongrie":                                              "tour-de-hongrie",
    "Czech Tour":                                                   "czech-tour",
    "Arctic Race of Norway":                                        "arctic-race-of-norway",
    "Vuelta a Burgos":                                              "vuelta-a-burgos",
    "Okolo Slovenska/Tour de Slovaquie":                            "okolo-slovenska",


    # Classics / one-day (additional variants)
    "Surf Coast Classic – Men":                                     "race-torquay",
    "Prueba Villafranca - Ordiziako Klasika":                       "prueba-villafranca",
    "Prueba Villafranca-Ordiziako Klasika":                         "prueba-villafranca",
    "Elfstedenrace":                                                "circuit-des-xi-villes",
    "Grand Prix de Montreal":                                       "gp-montreal",
    "Grand Prix de Quebec":                                         "gp-quebec",
    "GP d'Isbergues":                                               "gp-d-isbergues",
    "Bretagne Classic - Ouest-France":                              "bretagne-classic",
    "Donostia San Sebastian Klasikoa (Clasica San Sebastian)":      "san-sebastian",
    "BEMER Cyclassics":                                             "cyclassics-hamburg",

    # Stage races (additional base name variants)
    "Tour de Limousin":                                             "tour-du-limousin",

    # Championships / special (skip)
    "VM-linjeløbet":                                                "world-championship",
    "VM i enkeltstart":                                             "world-championship-itt",
    "VM-enkeltstarten":                                             "world-championship-itt",
    "EM-linjeløbet":                                                "uec-road-european-championships-me",
    "EM-enkeltstarten":                                             "uec-road-european-championships-itt",
    "U23-rytternes VM-linjeløb":                                    "world-championships-u23",
    "U23-rytternes VM-enkeltstart":                                 "world-championships-itt-u23",
    "de nationale mesterskaber i linjeløb":                         None,
    "de nationale mesterskaber i enkeltstart":                      None,
    "de australske mesterskaber i linjeløb":                        "nc-australia",
    "de australske mesterskaber i enkeltstart":                     "nc-australia-itt",
    "bjergkonkurrencen i Tour de France":                           None,
    "pointkonkurrencen i Tour de France":                           None,
    "ungdomskonkurrencen i Tour de France":                         None,
    "holdkonkurrencen i Tour de France":                            None,
    "OL-linjeløbet":                                                "olympic-games",
    "OL-enkeltstarten":                                             "olympic-games-itt",
}

# Year overrides: when the article date year doesn't match the race year
# (e.g. Emil writes TdF 2025 predictions in late 2024)
YEAR_OVERRIDES = {
    "Tour de France": "2025",
}

# Jersey competition prefixes → jersey_type
# On the /gc page, Prev tables appear in order: GC, Points, KOM, Youth, Teams
JERSEY_TYPES = {
    "holdkonkurrencen":    "teams",
    "bjergkonkurrencen":   "kom",
    "pointkonkurrencen":   "points",
    "ungdomskonkurrencen": "youth",
}

# Danish country name → PCS national championship slug fragment
COUNTRY_NC_SLUGS = {
    "Danmark":        "denmark",
    "Holland":        "netherlands",
    "Belgien":        "belgium",
    "Slovenien":      "slovenia",
    "Spanien":        "spain",
    "Storbritannien": "great-britain",
    "Italien":        "italy",
    "Frankrig":       "france",
    "Schweiz":        "switzerland",
    "Tyskland":       "germany",
    "Norge":          "norway",
    "Portugal":       "portugal",
    "Australien":     "australia",
    "Irland":         "ireland",
    "Sverige":        "sweden",
    "Polen":          "poland",
    "Østrig":         "austria",
    "USA":            "usa",
    "Kasakhstan":     "kazakhstan",
    "Colombia":       "colombia",
}

JERSEY_PREV_INDEX = {
    "points": 1,  # 2nd Prev table
    "kom":    2,  # 3rd Prev table
    "youth":  3,  # 4th Prev table
    "teams":  4,  # 5th Prev table
}


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def add_columns(conn: sqlite3.Connection) -> None:
    """Add all derived columns if missing (idempotent)."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(predictions)")}
    for col, typ in [
        ("result_source", "TEXT"),
        ("race_context", "TEXT"),
        ("race_format", "TEXT"),
        ("total_stages", "INTEGER"),
        ("cancelled", "INTEGER"),
    ]:
        if col not in cols:
            conn.execute(f"ALTER TABLE predictions ADD COLUMN {col} {typ}")
    conn.commit()


KNOWN_STAGE_RACES = {
    "Tour des Alpes-Maritimes",
    "Tour des Alpes",
}


def build_stage_races(conn: sqlite3.Connection) -> set[str]:
    """Return set of base race names that have stage entries in the DB."""
    stage_races: set[str] = KNOWN_STAGE_RACES.copy()
    for (name,) in conn.execute("SELECT race_name FROM predictions"):
        m = re.match(r"\d+\. etape af (.+)", name, re.IGNORECASE)
        if m:
            stage_races.add(m.group(1).strip())
    return stage_races


def fetch_null_rows(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT id, race_name, date, predicted_winner FROM predictions "
        "WHERE actual_winner IS NULL AND (cancelled IS NULL OR cancelled = 0)"
    ).fetchall()
    return [
        {"id": r[0], "race_name": r[1], "date": r[2], "predicted_winner": r[3]}
        for r in rows
    ]


def mark_cancelled(conn: sqlite3.Connection, row_id: int, race_context: str,
                   race_format: str | None) -> None:
    conn.execute(
        "UPDATE predictions SET cancelled=1, race_context=?, race_format=? WHERE id=?",
        (race_context, race_format, row_id),
    )
    conn.commit()


def update_result(conn: sqlite3.Connection, row_id: int, actual_winner: str,
                  correct: int, result_source: str, race_context: str,
                  race_format: str, total_stages: int | None,
                  race_date: str | None = None) -> None:
    if race_date:
        conn.execute(
            """UPDATE predictions
               SET actual_winner=?, correct=?, result_source=?,
                   race_context=?, race_format=?, total_stages=?, date=?
               WHERE id=?""",
            (actual_winner, correct, result_source, race_context, race_format, total_stages, race_date, row_id),
        )
    else:
        conn.execute(
            """UPDATE predictions
               SET actual_winner=?, correct=?, result_source=?,
                   race_context=?, race_format=?, total_stages=?
               WHERE id=?""",
            (actual_winner, correct, result_source, race_context, race_format, total_stages, row_id),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# URL / slug construction
# ---------------------------------------------------------------------------

def _normalize_quotes(name: str) -> str:
    """Replace curly apostrophes with straight ASCII equivalents."""
    return name.replace("\u2019", "'").replace("\u2018", "'")


def to_slug(name: str) -> str | None:
    normalized = _normalize_quotes(name)
    if normalized in SLUG_OVERRIDES:
        return SLUG_OVERRIDES[normalized]  # may be None → caller skips
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = name.lower()
    name = re.sub(r"[^a-z0-9\s-]", "", name)
    name = re.sub(r"[\s]+", "-", name.strip())
    return name


def _resolve_year(base_name: str, date: str) -> str:
    """
    Determine the race year from the article date, with overrides.
    Also handles race names that contain a year suffix (e.g. "Tour de France 2025").
    """
    # Year embedded in the name takes highest priority
    m = re.search(r"\b(20\d{2})\b", base_name)
    if m:
        return m.group(1)
    # Manual override (e.g. TdF written in late 2024 but predicting 2025)
    clean = re.sub(r"\s+20\d{2}$", "", base_name).strip()
    if clean in YEAR_OVERRIDES:
        return YEAR_OVERRIDES[clean]
    return date[:4] if date else "2026"


def _clean_base_name(base_name: str) -> str:
    """Strip trailing year suffix from a race base name for slug lookup."""
    return re.sub(r"\s+20\d{2}$", "", base_name).strip()


def build_pcs_url(race_name: str, date: str) -> tuple[str | None, str | None]:
    """
    Returns (url, slug_or_None).
    Returns (None, None) when the race should be skipped.
    """
    # National championship: "de nationale mesterskaber i linjeløb Danmark"
    m = re.match(r"de \w+ mesterskaber i (linjeløb|enkeltstart)\s+(\w+)", race_name, re.IGNORECASE)
    if m:
        race_type, country = m.group(1).lower(), m.group(2)
        country_slug = COUNTRY_NC_SLUGS.get(country)
        # Some countries have non-standard road race slugs on PCS
        NC_ROAD_SLUG_OVERRIDES = {
            "Danmark":        "danish-championships",
            "Storbritannien": "ncgreat-britain",
            "Schweiz":        "nc-switserland",
            "Portugal":       "nc-portugal2",
        }
        NC_ITT_SLUG_OVERRIDES = {
            "Portugal": "nc-portugal",
        }
        if country_slug:
            if race_type == "linjeløb" and country in NC_ROAD_SLUG_OVERRIDES:
                slug = NC_ROAD_SLUG_OVERRIDES[country]
            elif race_type == "linjeløb":
                slug = f"nc-{country_slug}"
            elif country in NC_ITT_SLUG_OVERRIDES:
                slug = NC_ITT_SLUG_OVERRIDES[country]
            else:
                slug = f"nc-{country_slug}-itt"
            year = _resolve_year(race_name, date)
            return f"{BASE}/{slug}/{year}", slug

    # Jersey competition: "bjergkonkurrencen i Tour de France"
    jersey = get_jersey_type(race_name)
    if jersey:
        _, base = jersey
        year = _resolve_year(base, date)
        slug = to_slug(_clean_base_name(base))
        if slug is None:
            return None, None
        return f"{BASE}/{slug}/{year}/gc", slug

    # Stage: "8. etape af Paris-Nice"
    m = re.match(r"(\d+)\. etape af (.+)", race_name, re.IGNORECASE)
    if m:
        base = m.group(2).strip()
        year = _resolve_year(base, date)
        slug = to_slug(_clean_base_name(base))
        if slug is None:
            return None, None
        return f"{BASE}/{slug}/{year}/stage-{m.group(1)}/result/result", slug

    # Prologue: "prologen til Santos Tour Down Under"
    m = re.match(r"prologen til (.+)", race_name, re.IGNORECASE)
    if m:
        base = m.group(1).strip()
        year = _resolve_year(base, date)
        slug = to_slug(_clean_base_name(base))
        if slug is None:
            return None, None
        return f"{BASE}/{slug}/{year}/prologue/result/result", slug

    # One-day / GC
    year = _resolve_year(race_name, date)
    slug = to_slug(_clean_base_name(race_name))
    if slug is None:
        return None, None
    return f"{BASE}/{slug}/{year}", slug


def build_url_from_slug(race_name: str, slug: str, year: str) -> str:
    """Reconstruct a PCS URL given a slug, preserving stage/prologue suffix."""
    m = re.match(r"(\d+)\. etape af .+", race_name, re.IGNORECASE)
    if m:
        return f"{BASE}/{slug}/{year}/stage-{m.group(1)}/result/result"
    if re.match(r"prologen til .+", race_name, re.IGNORECASE):
        return f"{BASE}/{slug}/{year}/prologue/result/result"
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

PARTICLES = {"van", "de", "der", "den", "du", "del", "di", "da", "la", "le", "el", "los", "von", "af", "team"}


def normalize(name: str) -> set[str]:
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    return {t for t in name.lower().split() if t not in PARTICLES}


def names_match(predicted: str, found: str) -> bool:
    return bool(normalize(predicted) & normalize(found))


# ---------------------------------------------------------------------------
# Winner extraction from PCS
# ---------------------------------------------------------------------------

def get_race_context(race_name: str, stage_races: set[str]) -> str:
    """Classify as 'stage', 'gc', or 'one_day' based on race name."""
    if re.match(r"\d+\. etape af .+", race_name, re.IGNORECASE):
        return "stage"
    if re.match(r"prologen til .+", race_name, re.IGNORECASE):
        return "stage"
    if re.match(r"\w+konkurrencen\b", race_name, re.IGNORECASE):
        return "gc"
    if race_name in stage_races:
        return "gc"
    return "one_day"


def get_race_format(page) -> str:
    """Read 'itt', 'ttt', or 'rr' from the PCS .page-title element."""
    try:
        title = page.locator(".page-title").first.inner_text(timeout=3_000)
        if "ITT" in title:
            return "itt"
        if "TTT" in title:
            return "ttt"
    except Exception:
        pass
    return "rr"


def extract_race_date(page) -> str | None:
    """
    Extract the race date from a PCS result page.
    PCS shows dates in .infolist as 'DD/MM/YYYY' or 'DD/MM' format.
    Returns ISO date string (YYYY-MM-DD) or None.
    """
    try:
        for li in page.locator(".infolist li").all():
            try:
                text = li.inner_text(timeout=1_000)
            except Exception:
                continue
            m = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", text)
            if m:
                day, month, year = m.group(1), m.group(2), m.group(3)
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            # Sometimes only DD/MM without year
            m = re.search(r"\b(\d{1,2})/(\d{1,2})\b", text)
            if m:
                day, month = m.group(1), m.group(2)
                # Extract year from page URL or title as fallback
                try:
                    url_year = re.search(r"/(\d{4})/", page.url)
                    year = url_year.group(1) if url_year else "2025"
                except Exception:
                    year = "2025"
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    except Exception:
        pass
    return None


def get_total_stages(page) -> int | None:
    """
    Extract total stage count from a GC race page.
    Only called for gc context rows — returns None if not found.
    """
    try:
        for selector in (".infolist", ".raceinfonav", ".page-title", "h1"):
            for el in page.locator(selector).all():
                text = el.inner_text(timeout=1_000)
                m = re.search(r"\b(\d+)\s+stages?\b", text, re.IGNORECASE)
                if m:
                    return int(m.group(1))
    except Exception:
        pass
    return None


def extract_jersey_winner(page, jersey_type: str) -> str | None:
    """
    Extract the winner of a jersey classification from the /gc page.
    Finds the Nth table with 'Prev' in its headers (order: GC, Points, KOM, Youth, Teams).
    """
    target_idx = JERSEY_PREV_INDEX.get(jersey_type)
    if target_idx is None:
        return None

    prev_tables = [
        t for t in page.locator("table.results").all()
        if "Prev" in [th.inner_text().strip() for th in t.locator("thead th").all()]
    ]
    if target_idx >= len(prev_tables):
        return None

    table = prev_tables[target_idx]
    headers = [th.inner_text().strip() for th in table.locator("thead th").all()]
    rnk_idx = headers.index("Rnk") if "Rnk" in headers else None
    team_idx = headers.index("Team") if "Team" in headers else None

    for tr in table.locator("tbody tr:nth-child(-n+5)").all():
        cells = tr.locator("td").all()
        if rnk_idx is not None and rnk_idx < len(cells):
            if cells[rnk_idx].inner_text(timeout=1_000).strip().strip(".") == "1":
                if jersey_type == "teams":
                    if team_idx is not None and team_idx < len(cells):
                        return cells[team_idx].inner_text(timeout=3_000).strip()
                else:
                    link = tr.locator("a").first
                    if link.count() > 0:
                        return link.inner_text(timeout=3_000).strip()
    return None


def is_cancelled(page) -> bool:
    """Fast early-exit: returns True if PCS explicitly labels the race as cancelled/neutralised."""
    text = page.inner_text("body").lower()
    return any(kw in text for kw in ("cancelled", "annulled", "neutralized", "stage cancelled"))


def get_jersey_type(race_name: str) -> tuple[str, str] | None:
    """
    Detect jersey competition race names.
    Returns (jersey_type, base_race_name) or None.
    E.g. "bjergkonkurrencen i Tour de France" → ("kom", "Tour de France")
    """
    m = re.match(r"(\w+konkurrencen) i (.+)", race_name, re.IGNORECASE)
    if m:
        prefix = m.group(1).lower()
        jersey_type = JERSEY_TYPES.get(prefix)
        if jersey_type:
            return jersey_type, m.group(2).strip()
    return None



def extract_winner(page) -> tuple[str | None, str | None, bool]:
    """
    Returns (rider_winner, team_winner, had_result_table).
    Skips standings tables (those with a 'Prev' header).
    Finds the winner by scanning rows for the one with Rnk=1.
    had_result_table is True if at least one non-standings result table was present —
    useful to distinguish "cancelled stage" (table present, no Rnk=1) from
    "page not yet populated" (no table at all).
    """
    rider_winner = None
    team_winner = None
    had_result_table = False
    for table in page.locator("table.results").all():
        headers = [th.inner_text().strip() for th in table.locator("thead th").all()]
        if "Prev" in headers:
            continue

        # This is the main result table — stop here regardless of outcome.
        # Sub-classification tables (KOM, points, youth, etc.) come after and must be ignored.
        had_result_table = True

        has_rnk = "Rnk" in headers
        rnk_idx = headers.index("Rnk") if has_rnk else None
        rider_idx = headers.index("Rider") if "Rider" in headers else None
        team_idx = headers.index("Team") if "Team" in headers else None

        # Find the row where Rnk = "1" — only check first 5 rows to avoid scanning huge tables
        winner_row = None
        for tr in table.locator("tbody tr:nth-child(-n+5)").all():
            cells = tr.locator("td").all()
            if has_rnk and rnk_idx is not None and rnk_idx < len(cells):
                rnk_text = cells[rnk_idx].inner_text(timeout=1_000).strip().strip(".")
                if rnk_text == "1":
                    winner_row = tr
                    break
            elif not has_rnk:
                winner_row = tr
                break

        if winner_row is not None:
            if rider_idx is not None:
                link = winner_row.locator("a").first
                if link.count() > 0:
                    text = link.inner_text(timeout=3_000).strip()
                    if text:
                        rider_winner = text
            if team_idx is not None:
                cells = winner_row.locator("td").all()
                if team_idx < len(cells):
                    text = cells[team_idx].inner_text(timeout=3_000).strip()
                    if text:
                        team_winner = text

        break  # never read sub-classification tables

    return rider_winner, team_winner, had_result_table


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
    parser.add_argument("--ids", type=int, nargs="+", help="Process only specific row IDs")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    add_columns(conn)

    stage_races = build_stage_races(conn)

    # Handle rows with no predicted_winner — run add_multi.py interactively,
    # then mark the parent row cancelled so it's not re-triggered next run.
    if not args.ids and not args.dry_run:
        missing = conn.execute(
            "SELECT id, url FROM predictions "
            "WHERE (predicted_winner IS NULL OR predicted_winner = '') "
            "AND actual_winner IS NULL AND (cancelled IS NULL OR cancelled = 0)"
        ).fetchall()
        seen_urls = set()
        for row_id, url in missing:
            if not url or url in seen_urls:
                conn.execute("UPDATE predictions SET cancelled=1 WHERE id=?", (row_id,))
                conn.commit()
                continue
            seen_urls.add(url)
            print(f"\nNo predicted winner for id={row_id} — running add_multi.py for {url}")
            add_multi = Path(__file__).parent / "add_multi.py"
            subprocess.run([sys.executable, str(add_multi), url])
            conn.execute("UPDATE predictions SET cancelled=1 WHERE id=?", (row_id,))
            conn.commit()

    rows = fetch_null_rows(conn)
    if args.ids:
        rows = [r for r in rows if r["id"] in args.ids]
    else:
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
        headless = "CI" in os.environ
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context(user_agent=USER_AGENT, locale="en-US")
        page = context.new_page()

        matched = 0
        unmatched = 0
        skipped = 0

        for i, row in enumerate(rows, 1):
            race_name = re.sub(r"(\d+\. etape) a ([^f])", r"\1 af \2", row["race_name"], flags=re.IGNORECASE)
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
                if status == 404 or (response and response.status >= 400):
                    year = (row["date"] or "2026")[:4]
                    # Prologue: some races use stage-0 instead of prologue on PCS
                    if "/prologue/" in url:
                        stage0_url = url.replace("/prologue/", "/stage-0/")
                        print(f"  {status} — trying stage-0: {stage0_url}")
                        try:
                            response = page.goto(stage0_url, wait_until="domcontentloaded", timeout=30_000)
                            if response and response.status < 400:
                                url = stage0_url
                                status = response.status
                        except Exception:
                            pass
                    if status >= 400:
                        print(f"  {status} — searching PCS for '{race_name}'...")
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
                recovered = False
                # Prologue: try stage-0 first
                if "/prologue/" in url:
                    stage0_url = url.replace("/prologue/", "/stage-0/")
                    print(f"  Navigation failed — trying stage-0: {stage0_url}")
                    try:
                        page.goto(stage0_url, wait_until="domcontentloaded", timeout=30_000)
                        url = stage0_url
                        recovered = True
                    except Exception:
                        pass
                if not recovered:
                    print(f"  Navigation failed — solve CAPTCHA in browser then press Enter (or wait {CAPTCHA_WAIT_TIMEOUT}s to skip)...")
                    if wait_for_enter():
                        recovered = True  # user solved CAPTCHA, page is loaded
                    else:
                        found_slug = search_pcs_slug(page, race_name, year)
                        if found_slug:
                            url = build_url_from_slug(race_name, found_slug, year)
                            print(f"  Retrying with: {url}")
                            try:
                                page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                                recovered = True
                            except Exception as e:
                                print(f"  [WARNING] Retry failed for id={row['id']}: {e}")
                        if not recovered:
                            print(f"  [WARNING] No PCS result found for id={row['id']} race='{race_name}'")
                            unmatched += 1
                            time.sleep(2)
                            continue

            # Wait for result table to appear (faster than a fixed sleep)
            try:
                page.wait_for_selector("table.results", timeout=5_000)
            except Exception:
                # Prologue URLs sometimes redirect silently to the main race page on PCS.
                # If no result table found, retry with stage-0.
                if "/prologue/" in url:
                    stage0_url = url.replace("/prologue/", "/stage-0/")
                    print(f"  No result on prologue page — trying stage-0: {stage0_url}")
                    try:
                        page.goto(stage0_url, wait_until="domcontentloaded", timeout=30_000)
                        page.wait_for_selector("table.results", timeout=5_000)
                        url = stage0_url
                    except Exception:
                        pass
                elif not url.endswith("/result/result"):
                    # One-day races sometimes need /result/result suffix
                    result_url = url.rstrip("/") + "/result/result"
                    print(f"  No result table — trying {result_url}")
                    try:
                        page.goto(result_url, wait_until="domcontentloaded", timeout=30_000)
                        page.wait_for_selector("table.results", timeout=5_000)
                        url = result_url
                    except Exception:
                        pass

            race_context = get_race_context(race_name, stage_races)
            # Prologues are always ITT; GCs have no format
            if race_context == "gc":
                race_format = None
            elif re.match(r"prologen til .+", race_name, re.IGNORECASE):
                race_format = "itt"
            else:
                race_format = get_race_format(page)
            total_stages = get_total_stages(page) if race_context == "gc" else None

            jersey = get_jersey_type(race_name)
            if jersey:
                jersey_type, _ = jersey
                winner = extract_jersey_winner(page, jersey_type)
                if winner is None:
                    print(f"  No {jersey_type} classification found — race not yet finished, skipping.")
                    skipped += 1
                    continue
                rider_winner = winner if jersey_type != "teams" else None
                team_winner = winner if jersey_type == "teams" else None
                had_result_table = True
            else:
                rider_winner, team_winner, had_result_table = extract_winner(page)

            if not had_result_table:
                if race_context == "gc":
                    # GC races never have table.results — try /statistics/start
                    print(f"  No result table found, trying /statistics/start fallback...")
                    rider_winner = extract_winner_from_startlist(page, url)
                    if rider_winner is None:
                        print(f"  No GC result found — race not yet finished, skipping.")
                        skipped += 1
                        continue
                else:
                    # No result table → race hasn't been run yet
                    print(f"  No result table — race not yet run, skipping.")
                    skipped += 1
                    continue
            elif rider_winner is None and team_winner is None:
                # Table found but no Rnk=1 — check if explicitly cancelled
                if is_cancelled(page):
                    print(f"  Result table found but no winner (Rnk=1) — race cancelled.")
                    if not args.dry_run:
                        mark_cancelled(conn, row["id"], race_context, race_format)
                    skipped += 1
                else:
                    print(f"  Result table found but no winner (Rnk=1) — race not yet finished, skipping.")
                    skipped += 1
                continue

            predicted = row["predicted_winner"] or ""
            if race_format == "ttt":
                winner = team_winner or rider_winner
            elif rider_winner and names_match(predicted, rider_winner):
                winner = rider_winner
            elif team_winner and names_match(predicted, team_winner):
                winner = team_winner
            else:
                winner = rider_winner

            if winner:
                correct = 1 if names_match(predicted, winner) else 0
                status_str = "CORRECT" if correct else "WRONG"
                race_date = extract_race_date(page)
                print(f"  Found: {winner} [{race_context}/{race_format}] → {status_str}")
                if args.dry_run:
                    print(f"  [DRY RUN] Would write: actual_winner={winner}, correct={correct}, context={race_context}, format={race_format}, total_stages={total_stages}, date={race_date}")
                else:
                    update_result(conn, row["id"], winner, correct, url, race_context, race_format, total_stages, race_date)
                matched += 1
            else:
                print(f"  [WARNING] Could not extract winner name from result table id={row['id']} race='{race_name}'")
                skipped += 1

        browser.close()

    conn.close()
    print(f"\n{prefix}Done. Matched: {matched}, No result found: {unmatched}, Skipped: {skipped}.")
    print(f"Database: {DB_PATH}")


if __name__ == "__main__":
    main()
