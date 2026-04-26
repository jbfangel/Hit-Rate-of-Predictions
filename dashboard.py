"""
Streamlit dashboard — Emil Axelgaard's cycling prediction hit rate.

Usage:
    streamlit run dashboard.py
"""

import re
import sqlite3
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

DB_PATH = Path(__file__).parent / "data" / "predictions.db"

st.set_page_config(
    page_title="Axelgaard Hit Rate",
    page_icon="🚴",
    layout="wide",
)


@st.cache_data(ttl=60)
def load_data() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        """
        SELECT
            id, url, race_name, date, predicted_winner, actual_winner,
            correct, race_context, race_format, cancelled, result_source
        FROM predictions
        ORDER BY date DESC
        """,
        conn,
    )
    conn.close()

    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True, format="mixed")
    df["year"] = df["date"].dt.year.astype("Int64")
    return df


def hit_rate(df: pd.DataFrame) -> tuple[int, int, float]:
    resolved = df[df["actual_winner"].notna() & (df["cancelled"] != 1)]
    total = len(resolved)
    correct = int(resolved["correct"].sum())
    rate = correct / total if total else 0.0
    return correct, total, rate


def race_type_label(context: str | None, fmt: str | None) -> str:
    mapping = {
        ("gc", None):        "GC",
        ("gc", ""):          "GC",
        ("one_day", "rr"):   "One-day (RR)",
        ("one_day", "itt"):  "One-day (ITT)",
        ("stage", "rr"):     "Stage (RR)",
        ("stage", "itt"):    "Stage (ITT)",
        ("stage", "ttt"):    "Stage (TTT)",
    }
    return mapping.get((context, fmt), f"{context or '?'}/{fmt or '?'}")


# ---------------------------------------------------------------------------
# Load & filter
# ---------------------------------------------------------------------------

if st.button("🔄 Refresh data"):
    st.cache_data.clear()

df_all = load_data()

st.title("🚴 Axelgaard's Prediction Hit Rate")

# Sidebar filters
with st.sidebar:
    st.header("Filters")

    years = sorted(df_all["year"].dropna().unique().tolist(), reverse=True)
    selected_years = st.multiselect("Year", years, default=years)

    type_options = sorted(
        df_all.apply(lambda r: race_type_label(r["race_context"], r["race_format"]), axis=1).unique()
    )
    selected_types = st.multiselect("Race type", type_options, default=type_options)

    show_pending = st.checkbox("Include pending (no result yet)", value=True)
    show_cancelled = st.checkbox("Include cancelled", value=True)

df = df_all.copy()

if selected_years:
    df = df[df["year"].isin(selected_years)]

df["_type_label"] = df.apply(lambda r: race_type_label(r["race_context"], r["race_format"]), axis=1)
if selected_types:
    df = df[df["_type_label"].isin(selected_types)]

if not show_cancelled:
    df = df[df["cancelled"] != 1]

if not show_pending:
    df = df[df["actual_winner"].notna() | (df["cancelled"] == 1)]

# ---------------------------------------------------------------------------
# Top metrics
# ---------------------------------------------------------------------------

correct, total, rate = hit_rate(df)

c1, c2, c3 = st.columns(3)
c1.metric("Hit rate", f"{rate:.1%}")
c2.metric("Correct", correct)
c3.metric("Resolved predictions", total)

st.divider()

# ---------------------------------------------------------------------------
# Breakdown table
# ---------------------------------------------------------------------------

col_left, col_right = st.columns(2)

resolved = df[df["actual_winner"].notna() & (df["cancelled"] != 1)]

with col_left:
    st.subheader("By race type")
    type_filters = {
        "GC":      resolved["race_context"] == "gc",
        "Stage":   resolved["race_context"] == "stage",
        "One-day": resolved["race_context"] == "one_day",
        "Road Race": resolved["race_format"] == "rr",
        "ITT":     resolved["race_format"] == "itt",
        "TTT":     resolved["race_format"] == "ttt",
    }
    rows = []
    for label, mask in type_filters.items():
        group = resolved[mask]
        if len(group):
            c, t, r = hit_rate(group)
            rows.append({"Type": label, "Hit rate": round(r * 100, 1), "Predictions": t})
    if rows:
        order = ["GC", "Stage", "One-day", "Road Race", "ITT", "TTT"]
        chart_df = pd.DataFrame(rows)
        chart_df["sort_key"] = chart_df["Type"].map({v: i for i, v in enumerate(order)})
        chart_df = chart_df.sort_values("sort_key")
        base = alt.Chart(chart_df)
        tooltip_spec = [
            alt.Tooltip("Type:O"),
            alt.Tooltip("Hit rate:Q", format=".1f"),
            alt.Tooltip("Predictions:Q"),
        ]
        bars = base.mark_bar().encode(
            x=alt.X("Type:O", sort=order, axis=alt.Axis(labelAngle=0)),
            y=alt.Y("Hit rate:Q", scale=alt.Scale(domain=[0, 100]), title="Hit rate (%)"),
            tooltip=tooltip_spec,
        )
        hover_area = base.mark_rect(opacity=0).encode(
            x=alt.X("Type:O", sort=order),
            tooltip=tooltip_spec,
        )
        st.altair_chart(bars + hover_area, use_container_width=True)

with col_right:
    st.subheader("Hit rate by year")
    rows = []
    for year, group in resolved.groupby("year"):
        c, t, r = hit_rate(group)
        rows.append({"Year": str(int(year)), "Hit rate": round(r * 100, 1), "Predictions": t})
    if rows:
        chart_df = pd.DataFrame(rows).sort_values("Year")
        chart = alt.Chart(chart_df).mark_bar().encode(
            x=alt.X("Year:O", axis=alt.Axis(labelAngle=0)),
            y=alt.Y("Hit rate:Q", scale=alt.Scale(domain=[0, 100]), title="Hit rate (%)"),
            tooltip=[
                alt.Tooltip("Year:O"),
                alt.Tooltip("Hit rate:Q", format=".1f"),
                alt.Tooltip("Predictions:Q"),
            ],
        )
        st.altair_chart(chart, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Predictions table
# ---------------------------------------------------------------------------

st.subheader("Predictions")

search = st.text_input("Search race or rider", "")

display = df.copy()
if search:
    mask = (
        display["race_name"].str.contains(search, case=False, na=False)
        | display["predicted_winner"].str.contains(search, case=False, na=False)
        | display["actual_winner"].str.contains(search, case=False, na=False)
    )
    display = display[mask]

def result_label(row):
    if row["cancelled"] == 1:
        return "🛑 Cancelled"
    if pd.isna(row["actual_winner"]) or row["actual_winner"] == "":
        return "⏳ Pending"
    return "✅ Correct" if row["correct"] == 1 else "❌ Wrong"

display["Result"] = display.apply(result_label, axis=1)
display["Date"] = display["date"].dt.strftime("%Y-%m-%d").fillna("")


def normalize_name(name):
    """Convert PCS format 'LASTNAME Firstname' to 'Firstname Lastname'."""
    if not name or pd.isna(name):
        return ""
    words = name.split()
    # Find where the all-caps lastname words end
    split_idx = 0
    for i, w in enumerate(words):
        if w.isalpha() and w == w.upper():
            split_idx = i + 1
        else:
            break
    if 0 < split_idx < len(words):
        last = " ".join(words[:split_idx]).title()
        first = " ".join(words[split_idx:])
        return f"{first} {last}"
    return name


def cap_first(name: str) -> str:
    """Capitalize the first letter only if the name starts with a letter."""
    if name and name[0].isalpha():
        return name[0].upper() + name[1:]
    return name


def make_link(text, href):
    if not text or pd.isna(text):
        return ""
    if not href or pd.isna(href):
        return str(text)
    return f'<a href="{href}" target="_blank">{text}</a>'


rows_html = ""
for _, r in display.iterrows():
    skip_normalize = r["race_format"] == "ttt" or "holdkonkurrencen" in str(r["race_name"]).lower()
    fmt = (lambda x: x) if skip_normalize else normalize_name
    predicted_cell = make_link(fmt(r["predicted_winner"]), r["url"])
    actual_cell = make_link(fmt(r["actual_winner"]), r["result_source"])
    rows_html += (
        f"<tr>"
        f"<td>{r['Date']}</td>"
        f"<td>{cap_first(r['race_name'])}</td>"
        f"<td>{r['_type_label']}</td>"
        f"<td>{predicted_cell}</td>"
        f"<td>{actual_cell}</td>"
        f"<td>{r['Result']}</td>"
        f"</tr>"
    )

st.markdown(f"""
<style>
  .pred-table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
  .pred-table th {{ text-align: left; padding: 6px 10px; border-bottom: 2px solid #ddd; }}
  .pred-table td {{ padding: 5px 10px; border-bottom: 1px solid #eee; }}
  .pred-table tr:hover td {{ background: inherit; }}
  .pred-table a {{ color: #1a73e8; text-decoration: none; }}
  .pred-table a:hover {{ text-decoration: underline; }}
</style>
<table class="pred-table">
  <thead><tr>
    <th>Date</th><th>Race</th><th>Type</th><th>Predicted</th><th>Actual</th><th>Result</th>
  </tr></thead>
  <tbody>{rows_html}</tbody>
</table>
""", unsafe_allow_html=True)
