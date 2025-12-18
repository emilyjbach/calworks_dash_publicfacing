import re
from datetime import date
from pathlib import Path
from typing import Optional

import altair as alt
import pandas as pd
import streamlit as st

alt.data_transformers.disable_max_rows()

st.set_page_config(
    page_title="General Relief (GR) Interactive Database",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- CONFIGURATION ---
GR_FILE_NAMES = [
    "15-16.csv", "16-17.csv", "17-18.csv", "18-19.csv", "19-20.csv",
    "20-21.csv", "21-22.csv", "22-23.csv", "23-24.csv", "24-25.csv",
]

METRICS_IN_ORDER = [
    "A. Adjustment", "A. 1. Cases brought forward", "A. 2. Cases added during month",
    "A. 3. Total cases available", "A. 4. Cases discontinued", "A. 5. Cases carried forward",
    "B. 6. Total General Relief Cases", "B. 6a. Total Family Cases", "B. 6b. Total One-person Cases",
    "B. 6. Total General Relief Persons", "B. 6a. Total Family Case Persons", "B. 6b. Total One-person Case Persons",
    "B. 6. Total GR Expenditure (Dollars)", "B. 6(1). GR Expenditure in Cash", "B. 6(2). GR Expenditure in Kind",
    "B. 6a. Total Family Expenditure (Dollars)", "B. 6b. Total One-person Expenditure (Dollars)",
    "C. 7. Cases added during month", "C. 8. Total SSA checks disposed of", "C. 8a. Total SSA disposed in 1-10 days",
    "C. 9. SSA sent SSI/SSP check directly to recipient", "C. 10. Denial notice received",
    "D. 11. Reimbursements Cases", "D. 11a. SSA check received Cases", "D. 11b. Repaid by recipient Cases",
    "D. 11. Amount reimbursed", "D. 11a. Amount received in SSA check", "D. 11b. Amount repaid by recipient",
    "E. Net General Relief Expenditure",
]

# --- HELPERS ---
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
    to_drop = [col for col in df.columns if not norm_col(col) or norm_col(col).lower().startswith("unnamed")]
    if to_drop:
        df = df.drop(columns=to_drop, errors="ignore")

    renames = {}
    for col in df.columns:
        low_name = norm_col(col).lower()
        if low_name in ("date", "date code", "date_code"): renames[col] = "Date_Code"
        elif low_name in ("county name", "county_name", "county"): renames[col] = "County_Name"
        elif low_name in ("report month", "report_month"): renames[col] = "Report_Month"
        elif low_name == "month": renames[col] = "Month"
        elif low_name == "year": renames[col] = "Year"
    return df.rename(columns=renames)

def parse_date_series(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    res = pd.to_datetime(s.str.upper(), format="%b%y", errors="coerce")
    numeric_vals = pd.to_numeric(s, errors="coerce")
    idx = numeric_vals.dropna().index
    if len(idx) > 0:
        yyyymm = numeric_vals.loc[idx].astype(int).astype(str)
        res.loc[idx] = res.loc[idx].fillna(pd.to_datetime(yyyymm, format="%Y%m", errors="coerce"))
    for f in ("%Y-%m", "%Y-%m-%d", "%m/%Y", "%m/%d/%Y", "%b %Y"):
        res = res.fillna(pd.to_datetime(s, format=f, errors="coerce"))
    return res.fillna(pd.to_datetime(s, errors="coerce"))

def build_date(df: pd.DataFrame) -> pd.Series:
    if "Date_Code" in df.columns:
        parsed = parse_date_series(df["Date_Code"])
        if parsed.notna().any(): return parsed
    if "Report_Month" in df.columns:
        parsed = parse_date_series(df["Report_Month"])
        if parsed.notna().any(): return parsed
    if "Month" in df.columns and "Year" in df.columns:
        mm = pd.to_numeric(df["Month"], errors="coerce").fillna(1).astype(int).astype(str).str.zfill(2)
        yy = pd.to_numeric(df["Year"], errors="coerce").fillna(2000).astype(int).astype(str)
        return pd.to_datetime(yy + "-" + mm + "-01", errors="coerce")
    return pd.Series(pd.NaT, index=df.index)

def read_gr_csv(path: Path, logs: list[str]) -> Optional[pd.DataFrame]:
    for h_idx in [4, 0, 5]:
        try:
            df = pd.read_csv(path, header=h_idx, engine="python")
            df = normalize_columns(df)
            if "County_Name" in df.columns:
                return df
        except: continue
    return None

def map_metric_columns(df: pd.DataFrame, metrics_list: list[str]) -> pd.DataFrame:
    mapping = {}
    for col in df.columns:
        match = re.match(r"^(?:Cell\s*)?(\d+)$", norm_col(col), flags=re.IGNORECASE)
        if match:
            idx = int(match.group(1)) - 1
            if 0 <= idx < len(metrics_list):
                mapping[col] = metrics_list[idx]
    return df.rename(columns=mapping)

@st.cache_data
def load_all_data(files: list[str], metrics_tuple: tuple[str, ...]):
    metrics_list = list(metrics_tuple)
    logs, frames = [], []
    has_alpha = re.compile(r"[A-Za-z]")

    for f in files:
        f_path = resolve_path(f)
        if not f_path: continue
        df = read_gr_csv(f_path, logs)
        if df is None: continue

        df["County_Name"] = df["County_Name"].astype(str).str.strip()
        df = df[~df["County_Name"].str.contains("Statewide", case=False, na=False)]
        df = df[df["County_Name"].apply(lambda x: bool(has_alpha.search(x)))]
        
        df["Date"] = build_date(df)
        df = df.dropna(subset=["Date"])
        df = map_metric_columns(df, metrics_list)

        found_metrics = [m for m in metrics_list if m in df.columns]
        for m_col in found_metrics:
            # Handle '*' de-identification by removing the star before numeric conversion
            df[m_col] = pd.to_numeric(df[m_col].astype(str).str.replace('*', '', regex=False), errors="coerce")

        long_df = pd.melt(df, id_vars=["Date", "County_Name"], value_vars=found_metrics, 
                          var_name="Metric", value_name="Value")
        frames.append(long_df)

    if not frames: return pd.DataFrame(), logs
    
    all_data = pd.concat(frames, ignore_index=True)

    # --- REMOVE EMPTY DATA ---
    # Metrics check
    metric_sums = all_data.groupby("Metric")["Value"].sum(min_count=1)
    empty_metrics = metric_sums[metric_sums.isna() | (metric_sums == 0)].index
    all_data = all_data[~all_data["Metric"].isin(empty_metrics)]

    # County check
    county_sums = all_data.groupby("County_Name")["Value"].sum(min_count=1)
    empty_counties = county_sums[county_sums.isna() | (county_sums == 0)].index
    all_data = all_data[~all_data["County_Name"].isin(empty_counties)]

    all_data = all_data.sort_values("Date").drop_duplicates(subset=["Date", "County_Name", "Metric"])
    return all_data.reset_index(drop=True), logs

# --- APP UI ---
st.markdown("""
    <style>
      .block-container { padding-top: 1.1rem; max-width: 1220px; }
      .gr-hero { border-radius: 18px; padding: 20px; border: 1px solid rgba(49, 51, 63, 0.14); background: #fcfcfc; box-shadow: 0 4px 12px rgba(0,0,0,0.05); }
      .pill { display: inline-flex; padding: 4px 12px; border-radius: 999px; background: white; border: 1px solid #eee; font-size: 0.85rem; margin-right: 8px; font-weight: 500; }
    </style>
""", unsafe_allow_html=True)

st.title("General Relief")
st.caption("Emily Bach (Development) | CDSS Data | Updated: 12/17/2025")

try:
    data, logs = load_all_data(GR_FILE_NAMES, tuple(METRICS_IN_ORDER))

    with st.sidebar:
        st.header("Filter Options")
        if data.empty:
            st.error("No data found. Check file naming or folder paths.")
            st.stop()

        min_d, max_d = data["Date"].min().date(), data["Date"].max().date()
        date_range = st.slider("Select Timeframe", min_d, max_d, (date(2017, 1, 1), max_d))
        
        all_counties = sorted(data["County_Name"].unique())
        selected_counties = st.multiselect("Counties", all_counties, default=all_counties[:2])
        
        all_metrics = sorted(data["Metric"].unique())
        selected_metrics = st.multiselect("Metrics", all_metrics, default=all_metrics[:1])

    # Hero Section
    st.markdown(f"""
        <div class="gr-hero">
            <div style="font-size:1.1rem; font-weight:700;">GR 237 - General Relief and Interim Assistance (SSI/SSP)</div>
            <p style="opacity:0.7; font-size:0.9rem;">Cleaned dataset excluding counties/metrics with no data across the 2015-2025 period.</p>
            <div>
                <span class="pill">Active Counties: {len(all_counties)}</span>
                <span class="pill">Active Metrics: {len(all_metrics)}</span>
            </div>
        </div>
    """, unsafe_allow_html=True)

    # Filtering
    plot_df = data[
        (data["Date"].dt.date >= date_range[0]) & (data["Date"].dt.date <= date_range[1]) &
        (data["County_Name"].isin(selected_counties)) & (data["Metric"].isin(selected_metrics))
    ].copy()

    if not plot_df.empty:
        plot_df["Series"] = plot_df["County_Name"] + " - " + plot_df["Metric"]
        chart = alt.Chart(plot_df).mark_line(point=True).encode(
            x=alt.X("Date:T", title="Report Month", axis=alt.Axis(format="%b %Y")),
            y=alt.Y("Value:Q", scale=alt.Scale(zero=False), title="Numeric Value"),
            color="Series:N",
            tooltip=["Date", "County_Name", "Metric", "Value"]
        ).properties(height=450).interactive()
        st.altair_chart(chart, use_container_width=True)
        
        st.markdown("### Underlying Data")
        st.dataframe(plot_df.pivot_table(index="Date", columns=["County_Name", "Metric"], values="Value"))
    else:
        st.warning("No data matches the selected filters.")

except Exception as err:
    st.error("The application encountered an error.")
    st.exception(err)
