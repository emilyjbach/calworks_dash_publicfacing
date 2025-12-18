import re
from datetime import date
from pathlib import Path
from typing import Optional

import altair as alt
import pandas as pd
import streamlit as st

alt.data_transformers.disable_max_rows()

st.set_page_config(
    page_title="CalWORKs (CA 237 CW) Interactive Database",
    layout="wide",
    initial_sidebar_state="expanded",
)

# calworks
CW_FILE_NAMES = [
    "15-16.csv", "16-17.csv", "17-18.csv", "18-19.csv", "19-20.csv",
    "20-21.csv", "21-22.csv", "22-23.csv", "23-24.csv", "24-25.csv",
]

# metrics
METRICS_IN_ORDER = [
    "A. 1. Pending from last month",           # Cell 1
    "A. 1a. Item 5 from last month",           # Cell 2
    "A. 1b. Adjustment",                       # Cell 3
    "A. 2. Applications received",             # Cell 4
    "A. 2a. Applications",                     # Cell 5
    "A. 2b. Restoration",                      # Cell 6
    "A. 3. Total/month",                       # Cell 7
    "A. 4. Disposed of",                       # Cell 8
    "A. 4a. Approved",                         # Cell 9
    "A. 4b. Denied",                           # Cell 10
    "A. 4b1. Denied/Diversion",                # Cell 11
    "A. 4c. Other dispositions",               # Cell 12
    "A. 5. Pending at end of month",           # Cell 13
    "B. 6. Cases brought forward",             # Cell 14
    "B. 6a. Item 10 last month",               # Cell 15
    "B. 6b. Adjustment",                       # Cell 16
    "B. 7. Added during month",                # Cell 17
    "B. 8. Total cases open",                  # Cell 18
    "B. 9. Cases receiving cash grant",        # Cell 19
    "B. 10. Cases carried forward",            # Cell 20
    # ... You can extend this list further based on the full 143+ cells in your dictionary
]

# sidebar setup
with st.sidebar:
    st.header("Filter Options")
    show_debug = st.checkbox("Show debug log", value=False)

# helpers
def base_dir() -> Path:
    try:
        return Path(__file__).resolve().parent
    except Exception:
        return Path.cwd()

BASE_DIR = base_dir()
CANDIDATE_DIRS = [BASE_DIR, BASE_DIR / "data"]

def resolve_path(fname: str) -> Optional[Path]:
    for d in CANDIDATE_DIRS:
        target = d / fname
        if target.exists():
            return target
    return None

def norm_col(val) -> str:
    return str(val).strip().lstrip("\ufeff").strip()

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    to_drop = []
    for col in df.columns:
        clean_name = norm_col(col)
        if not clean_name or clean_name.lower().startswith("unnamed"):
            to_drop.append(col)

    if to_drop:
        df = df.drop(columns=to_drop, errors="ignore")

    renames = {}
    for col in df.columns:
        low_name = norm_col(col).lower()
        if low_name in ("date", "date code", "date_code"):
            renames[col] = "Date_Code"
        elif low_name in ("county name", "county_name", "county"):
            renames[col] = "County_Name"
        elif low_name in ("county code", "county_code"):
            renames[col] = "County_Code"
        elif low_name in ("report month", "report_month"):
            renames[col] = "Report_Month"
        elif low_name == "month":
            renames[col] = "Month"
        elif low_name == "year":
            renames[col] = "Year"

    return df.rename(columns=renames)

def parse_date_series(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    res = pd.Series(pd.NaT, index=s.index).fillna(
        pd.to_datetime(s.str.upper(), format="%b%y", errors="coerce")
    )
    numeric_vals = pd.to_numeric(s, errors="coerce")
    idx = numeric_vals.dropna().index
    if len(idx) > 0:
        yyyymm = numeric_vals.loc[idx].astype(int).astype(str)
        res.loc[idx] = res.loc[idx].fillna(pd.to_datetime(yyyymm, format="%Y%m", errors="coerce"))

    for f in ("%Y-%m", "%Y-%m-%d", "%m/%Y", "%m/%d/%Y", "%b %Y", "%B %Y"):
        res = res.fillna(pd.to_datetime(s, format=f, errors="coerce"))

    return res.fillna(pd.to_datetime(s, errors="coerce"))

def build_date(df: pd.DataFrame) -> pd.Series:
    if "Date_Code" in df.columns:
        parsed_dt = parse_date_series(df["Date_Code"])
        if parsed_dt.notna().any():
            return parsed_dt
    if "Report_Month" in df.columns:
        parsed_dt = parse_date_series(df["Report_Month"])
        if parsed_dt.notna().any():
            return parsed_dt
    return pd.Series(pd.NaT, index=df.index)

def read_cw_csv(path: Path, logs: list[str]) -> Optional[pd.DataFrame]:
    # CalWORKs files often have headers starting at row 4 or 5
    for h_idx in [4, 5, 0]:
        try:
            df = pd.read_csv(path, header=h_idx, engine="python")
            df = normalize_columns(df)
            col_blob = " ".join([norm_col(c).lower() for c in df.columns])
            if "county" in col_blob:
                logs.append(f"{path.name}: read with header={h_idx}")
                return df
        except Exception:
            continue
    return None

def map_metric_columns(df: pd.DataFrame, metrics_in_order: list[str]) -> pd.DataFrame:
    mapping = {}
    for col in df.columns:
        clean_c = norm_col(col)
        # Matches "Cell 1", "Cell1", or just "1"
        match = re.match(r"^(?:Cell\s*)?(\d+)$", clean_c, flags=re.IGNORECASE)
        if not match:
            continue
        cell_num = int(match.group(1))
        if 1 <= cell_num <= len(metrics_in_order):
            mapping[col] = metrics_in_order[cell_num - 1]
    return df.rename(columns=mapping) if mapping else df

@st.cache_data
def load_all(files: list[str], metrics_in_order_key: tuple[str, ...]):
    metrics_list = list(metrics_in_order_key)
    logs, frames = [], []
    has_alpha = re.compile(r"[A-Za-z]")

    for f in files:
        f_path = resolve_path(f)
        if f_path is None:
            logs.append(f"{f}: missing")
            continue

        df = read_cw_csv(f_path, logs)
        if df is None or df.empty:
            continue

        if "County_Name" not in df.columns:
            continue

        df["County_Name"] = df["County_Name"].astype(str).str.strip()
        df = df.loc[df["County_Name"].ne("Statewide")].dropna(subset=["County_Name"])
        df = df.loc[df["County_Name"].apply(lambda x: bool(has_alpha.search(x)))].copy()

        df["Date"] = build_date(df)
        df = df.dropna(subset=["Date"]).copy()

        if "Report_Month" not in df.columns:
            df["Report_Month"] = df["Date"].dt.strftime("%b %Y")

        df = map_metric_columns(df, metrics_list)
        found_metrics = [m for m in metrics_list if m in df.columns]

        for m_col in found_metrics:
            df[m_col] = pd.to_numeric(df[m_col], errors="coerce")

        keys = ["Date", "Report_Month", "County_Name"]
        long_df = pd.melt(
            df, id_vars=keys, value_vars=found_metrics,
            var_name="Metric", value_name="Value"
        ).dropna(subset=["Value"]).copy()

        frames.append(long_df)

    if not frames:
        return pd.DataFrame(), logs

    all_data = pd.concat(frames, ignore_index=True).sort_values("Date").reset_index(drop=True)
    return all_data.drop_duplicates(subset=["Date", "County_Name", "Metric"], keep="first"), logs

# styling
st.markdown("""
    <style>
      .block-container { padding-top: 1.1rem; max-width: 1220px; }
      .cw-hero {
        border-radius: 18px; padding: 18px;
        border: 1px solid rgba(49, 51, 63, 0.14);
        background: radial-gradient(900px 180px at 10% 0%, rgba(0, 100, 255, 0.16), transparent 55%), rgba(255,255,255,0.70);
        box-shadow: 0 10px 30px rgba(0,0,0,0.06);
      }
      .cw-hero-title { font-size: 1.1rem; font-weight: 700; margin-bottom: 0.35rem; }
      .pill { display: inline-flex; align-items: center; gap: 8px; padding: 4px 12px; border-radius: 999px; background: white; font-size: 0.85rem; border: 1px solid #eee; margin-right: 8px;}
    </style>
    """, unsafe_allow_html=True)

st.title("CalWORKs Interactive Database")
st.caption("Updated for CA 237 CW Caseload Movement Report | Last Data Pull: 12/2025")

try:
    data, logs = load_all(CW_FILE_NAMES, tuple(METRICS_IN_ORDER))

    if show_debug:
        with st.expander("Debug log"):
            for l in logs: st.write(l)

    if data.empty:
        st.error("No data loaded. Check filenames and header rows.")
        st.stop()

    min_date, max_date = data["Date"].min().date(), data["Date"].max().date()

    st.markdown(f"""
        <div class="cw-hero">
          <div class="cw-hero-title">CA 237 CW - CalWORKs Cash Grant Caseload Movement Report</div>
          <p style="font-size:0.9rem; opacity:0.8;">words here</p>
          <div class="pill"><b>Rows:</b> {len(data):,}</div>
          <div class="pill"><b>Range:</b> {min_date} to {max_date}</div>
        </div>
        """, unsafe_allow_html=True)

    all_counties = sorted(data["County_Name"].unique().tolist())
    valid_metrics = [m for m in METRICS_IN_ORDER if m in data["Metric"].unique()]

    with st.sidebar:
        date_range = st.slider("Date Range", min_date, max_date, (min_date, max_date))
        selected_counties = st.multiselect("Counties", all_counties, default=all_counties[:2])
        selected_metrics = st.multiselect("Metrics", valid_metrics, default=valid_metrics[:1])

    # Plotting
    plot_df = data[
        (data["Date"].dt.date >= date_range[0]) & 
        (data["Date"].dt.date <= date_range[1]) &
        (data["County_Name"].isin(selected_counties)) & 
        (data["Metric"].isin(selected_metrics))
    ].copy()

    if not plot_df.empty:
        plot_df["Series"] = plot_df["County_Name"] + " - " + plot_df["Metric"]
        chart = alt.Chart(plot_df).mark_line(point=True).encode(
            x=alt.X("Date:T", title="Report Month"),
            y=alt.Y("Value:Q", title="Cases / Applications", scale=alt.Scale(zero=False)),
            color="Series:N",
            tooltip=["Report_Month", "County_Name", "Metric", "Value"]
        ).interactive()
        st.altair_chart(chart, use_container_width=True)
        
        st.markdown("### Data Detail")
        st.dataframe(plot_df.drop(columns=["Series", "Date"]))
    else:
        st.warning("Adjust filters to view data.")

    st.markdown("---")
    st.markdown("### Interpreting CalWORKs Data")
    st.caption("""
'asdfdadfkj.""",)

except Exception as err:
    st.error(f"Error: {err}")
