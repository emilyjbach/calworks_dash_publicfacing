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

# files
GR_FILE_NAMES = [
    "15-16.csv", "16-17.csv", "17-18.csv", "18-19.csv", "19-20.csv",
    "20-21.csv", "21-22.csv", "22-23.csv", "23-24.csv", "24-25.csv",
]

# metrics
METRICS_IN_ORDER = [
    "A. Adjustment",
    "A. 1. Cases brought forward",
    "A. 2. Cases added during month",
    "A. 3. Total cases available",
    "A. 4. Cases discontinued",
    "A. 5. Cases carried forward",
    "B. 6. Total General Relief Cases",
    "B. 6a. Total Family Cases",
    "B. 6b. Total One-person Cases",
    "B. 6. Total General Relief Persons",
    "B. 6a. Total Family Case Persons",
    "B. 6b. Total One-person Case Persons",
    "B. 6. Total GR Expenditure (Dollars)",
    "B. 6(1). GR Expenditure in Cash",
    "B. 6(2). GR Expenditure in Kind",
    "B. 6a. Total Family Expenditure (Dollars)",
    "B. 6b. Total One-person Expenditure (Dollars)",
    "C. 7. Cases added during month",
    "C. 8. Total SSA checks disposed of",
    "C. 8a. Total SSA disposed in 1-10 days",
    "C. 9. SSA sent SSI/SSP check directly to recipient",
    "C. 10. Denial notice received",
    "D. 11. Reimbursements Cases",
    "D. 11a. SSA check received Cases",
    "D. 11b. Repaid by recipient Cases",
    "D. 11. Amount reimbursed",
    "D. 11a. Amount received in SSA check",
    "D. 11b. Amount repaid by recipient",
    "E. Net General Relief Expenditure",
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
        elif low_name == "sfy":
            renames[col] = "SFY"
        elif low_name == "ffy":
            renames[col] = "FFY"

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

    if "Month" in df.columns and "Year" in df.columns:
        m = pd.to_numeric(df["Month"], errors="coerce")
        y = pd.to_numeric(df["Year"], errors="coerce")
        valid = m.notna() & y.notna()
        dt_series = pd.Series(pd.NaT, index=df.index)
        if valid.any():
            mm = m[valid].astype(int).astype(str).str.zfill(2)
            yy = y[valid].astype(int).astype(str)
            dt_series.loc[valid] = pd.to_datetime(yy + "-" + mm + "-01", errors="coerce")
        return dt_series

    return pd.Series(pd.NaT, index=df.index)

def read_gr_csv(path: Path, logs: list[str]) -> Optional[pd.DataFrame]:
    try:
        df = pd.read_csv(path, header=4, engine="python")
        df = normalize_columns(df)
        if "County_Name" in df.columns and ("Date_Code" in df.columns or "Report_Month" in df.columns):
            logs.append(f"{path.name}: read with header=4")
            return df
    except Exception as e:
        logs.append(f"{path.name}: header=4 failed ({e})")

    for h_idx in range(0, 51):
        try:
            df = pd.read_csv(path, header=h_idx, engine="python")
            df = normalize_columns(df)
            col_blob = " ".join([norm_col(c).lower() for c in df.columns])
            if "county" in col_blob and (("date" in col_blob) or ("report month" in col_blob) or ("report_month" in col_blob)):
                logs.append(f"{path.name}: read with header={h_idx} (fallback)")
                return df
        except Exception:
            continue

    logs.append(f"{path.name}: could not find usable header row")
    return None

def map_metric_columns(df: pd.DataFrame, metrics_in_order: list[str]) -> pd.DataFrame:
    mapping = {}
    for col in df.columns:
        clean_c = norm_col(col)
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
    logs = []
    frames = []
    has_alpha = re.compile(r"[A-Za-z]")

    for f in files:
        f_path = resolve_path(f)
        if f_path is None:
            logs.append(f"{f}: missing (put next to app.py or in ./data/)")
            continue

        df = read_gr_csv(f_path, logs)
        if df is None or df.empty:
            continue

        if "County_Name" not in df.columns:
            logs.append(f"{f}: missing County_Name after read")
            continue

        df["County_Name"] = df["County_Name"].astype(str).str.strip()
        df = df.loc[df["County_Name"].ne("Statewide")].dropna(subset=["County_Name"])
        df = df.loc[df["County_Name"].apply(lambda x: bool(has_alpha.search(x)))].copy()

        if df.empty:
            logs.append(f"{f}: empty after county filtering")
            continue

        df["Date"] = build_date(df)
        df = df.dropna(subset=["Date"]).copy()
        if df.empty:
            logs.append(f"{f}: no parsable dates")
            continue

        if "Report_Month" not in df.columns:
            df["Report_Month"] = df["Date"].dt.strftime("%b %Y")

        logs.append(f"{f}: Columns before mapping: {df.columns.tolist()}")
        df = map_metric_columns(df, metrics_list)

        found_metrics = [m for m in metrics_list if m in df.columns]
        if not found_metrics:
            logs.append(f"{f}: no metric columns recognized (expected 1..{len(metrics_list)})")
            continue

        for m_col in found_metrics:
            df[m_col] = pd.to_numeric(df[m_col], errors="coerce")

        df = df.dropna(subset=found_metrics, how="all").copy()
        if df.empty:
            logs.append(f"{f}: all metric values empty after numeric coercion")
            continue

        keys = ["Date", "Report_Month", "County_Name"]
        if "County_Code" in df.columns:
            keys.append("County_Code")

        long_df = pd.melt(
            df,
            id_vars=keys,
            value_vars=found_metrics,
            var_name="Metric",
            value_name="Value",
        ).dropna(subset=["Value"]).copy()

        frames.append(long_df)
        logs.append(f"{f}: long_rows={len(long_df):,} | {df['Date'].min().date()} → {df['Date'].max().date()}")

    if not frames:
        return pd.DataFrame(), logs

    all_data = pd.concat(frames, ignore_index=True).sort_values("Date").reset_index(drop=True)
    return all_data.drop_duplicates(subset=["Date", "County_Name", "Metric"], keep="first"), logs

st.markdown(
    """
    <style>
      .block-container { padding-top: 1.1rem; padding-bottom: 1.3rem; max-width: 1220px; }
      h1 { letter-spacing: -0.03em; margin-bottom: 0.15rem; }
      [data-testid="stCaptionContainer"] { margin-top: 0.2rem; opacity: 0.85; }

      section[data-testid="stSidebar"] { border-right: 1px solid rgba(49,51,63,0.12); }
      section[data-testid="stSidebar"] .block-container { padding-top: 1.2rem; }
      section[data-testid="stSidebar"] h2, section[data-testid="stSidebar"] h3 { letter-spacing: -0.02em; }

      .gr-hero {
        position: relative;
        overflow: hidden;
        border-radius: 18px;
        padding: 18px 18px 16px 18px;
        border: 1px solid rgba(49, 51, 63, 0.14);
        background:
          radial-gradient(900px 180px at 10% 0%, rgba(151, 71, 255, 0.16), transparent 55%),
          radial-gradient(900px 180px at 60% 10%, rgba(0, 209, 255, 0.14), transparent 55%),
          radial-gradient(700px 200px at 90% 0%, rgba(255, 97, 165, 0.14), transparent 58%),
          rgba(255,255,255,0.70);
        box-shadow: 0 10px 30px rgba(0,0,0,0.06);
      }
      .gr-hero:after{
        content:"";
        position:absolute;
        inset:-2px;
        border-radius: 18px;
        background: linear-gradient(120deg,
          rgba(151,71,255,0.22),
          rgba(0,209,255,0.18),
          rgba(255,97,165,0.18));
        filter: blur(22px);
        opacity: 0.55;
        z-index: 0;
      }
      .gr-hero * { position: relative; z-index: 1; }
      .gr-hero-title {
        font-size: 1.02rem;
        font-weight: 700;
        letter-spacing: -0.02em;
        margin: 0 0 0.35rem 0;
      }
      .gr-hero-sub {
        font-size: 0.92rem;
        opacity: 0.80;
        margin: 0;
      }
      .pill-row { margin-top: 0.8rem; display: flex; flex-wrap: wrap; gap: 10px; }
      .pill {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 6px 12px;
        border-radius: 999px;
        border: 1px solid rgba(49, 51, 63, 0.16);
        background: rgba(255,255,255,0.72);
        box-shadow: 0 1px 10px rgba(0,0,0,0.04);
        font-size: 0.88rem;
      }
      .dot {
        width: 9px;
        height: 9px;
        border-radius: 999px;
        display:inline-block;
        background: linear-gradient(135deg, rgba(151,71,255,0.95), rgba(0,209,255,0.9));
      }
      .dot2 { background: linear-gradient(135deg, rgba(255,97,165,0.95), rgba(255,184,107,0.9)); }
      .dot3 { background: linear-gradient(135deg, rgba(0,209,255,0.9), rgba(57,255,20,0.55)); }

      .stButton button, .stDownloadButton button {
        border-radius: 12px !important;
        padding: 0.45rem 0.9rem !important;
      }

      h2, h3 { letter-spacing: -0.02em; }

      hr { margin: 1.35rem 0; opacity: 0.45; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("General Relief")
st.caption(
    "Emily Bach (Development, Visualization) | CDSS (Data) | Language: Python | Last Code Update: 12/17/2025 | Last Data Pull: 12/12/2025"
)

try:
    data, logs = load_all(GR_FILE_NAMES, tuple(METRICS_IN_ORDER))

    if show_debug:
        with st.expander("Debug log", expanded=True):
            st.write("Looking in:", [str(d) for d in CANDIDATE_DIRS])
            for l in logs:
                st.write(l)

    if data.empty:
        st.error("No data loaded. Turn on the debug log to see which file(s) failed and why.")
        st.stop()

    min_date = data["Date"].min().date()
    max_date = data["Date"].max().date()

    st.markdown(
        f"""
        <div class="gr-hero">
          <div class="gr-hero-title">GR 237 - General Relief and Interim Assistance to Applicants for SSI/SSP Monthly Caseload and Expenditure Statistical Report</div>
          <p class="gr-hero-sub">Source Data: https://www.cdss.ca.gov/inforesources/research-and-data/disability-adult-programs-data-tables/gr-237</p>
          <div class="pill-row">
            <span class="pill"><span class="dot"></span><b>Rows</b>&nbsp;{len(data):,}</span>
            <span class="pill"><span class="dot dot2"></span><b>Date range</b>&nbsp;{min_date} → {max_date}</span>
            <span class="pill"><span class="dot dot3"></span><b>Files</b>&nbsp;{len(GR_FILE_NAMES)}</span>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("<div style='height: 0.9rem;'></div>", unsafe_allow_html=True)

    all_counties = sorted(data["County_Name"].unique().tolist())
    avail_metrics = data["Metric"].dropna().astype(str).unique().tolist()
    valid_metrics = [m for m in METRICS_IN_ORDER if m in avail_metrics]

    with st.sidebar:
        d_start = max(min_date, date(2017, 1, 1))
        d_end = max_date

        date_range = st.slider(
            "Date Range",
            min_value=min_date,
            max_value=max_date,
            value=(d_start, d_end),
            format="YYYY/MM/DD",
        )

        wanted_counties = ["Contra Costa", "Kern"]
        default_counties = [c for c in wanted_counties if c in all_counties] or all_counties[:2]

        selected_counties = st.multiselect(
            "Counties",
            options=all_counties,
            default=default_counties,
        )

        main_metric = "A. 2. Cases added during month"
        selected_metrics = st.multiselect(
            "Metrics",
            options=valid_metrics,
            default=[main_metric] if main_metric in valid_metrics else (valid_metrics[:1] if valid_metrics else []),
        )

    # Filtering
    subset = data[(data["Date"].dt.date >= date_range[0]) & (data["Date"].dt.date <= date_range[1])].copy()

    plot_df = subset[
        subset["County_Name"].isin(selected_counties) & subset["Metric"].isin(selected_metrics)
    ].dropna(subset=["Value"]).copy()

    if plot_df.empty:
        st.warning("No data for the selected filters.")
        st.stop()

    plot_df["Series"] = plot_df["County_Name"] + " - " + plot_df["Metric"]

    lbl_counties = ", ".join(selected_counties[:4]) + ("…" if len(selected_counties) > 4 else "")
    lbl_metrics = ", ".join(selected_metrics[:4]) + ("…" if len(selected_metrics) > 4 else "")
    lbl_start, lbl_end = date_range[0].strftime("%Y/%m/%d"), date_range[1].strftime("%Y/%m/%d")

    st.markdown(
        f"""
        <h3 style='margin: 0.2rem 0 0.25rem 0;'>{"Counties" if len(selected_counties) > 1 else "County"}: {lbl_counties}</h3>
        <div style="opacity:0.82; font-size:0.95rem; margin-bottom:0.6rem;">
            <b>Metrics:</b> {lbl_metrics} &nbsp; 
            <b>Period:</b> {lbl_start} → {lbl_end}
        </div>
        """,
        unsafe_allow_html=True,
    )

    chart_title = f"{lbl_counties} | {lbl_metrics} | {lbl_start} → {lbl_end}"

    chart = (
        alt.Chart(plot_df)
        .mark_line(point=True)
        .encode(
            x=alt.X("Date:T", axis=alt.Axis(title="Report Month", format="%b %Y")),
            y=alt.Y("Value:Q", scale=alt.Scale(zero=False), title="Value"),
            color=alt.Color("Series:N"),
            tooltip=[
                alt.Tooltip("Report_Month:N"),
                alt.Tooltip("County_Name:N"),
                alt.Tooltip("Metric:N"),
                alt.Tooltip("Value:Q", format=",.0f"),
            ],
        )
        .properties(title=chart_title)
        .interactive()
    )

    st.altair_chart(chart, use_container_width=True)

    st.markdown("---")
    st.markdown("<h3 style='margin-bottom: 0.2rem;'>Underlying Data</h3>", unsafe_allow_html=True)
    st.caption("Tip: Columns are sortable; to multi-sort pin a column. This is likely most helpful for multi-county or multi-metric reports. Copied data exports as .csv.")

    st.dataframe(plot_df.drop(columns=["Series", "County_Code", "Date"], errors="ignore"))

    st.markdown("---")
    st.markdown("<h3 style='margin-bottom: 0.2rem;'>Interpreting Data</h3>", unsafe_allow_html=True)

    data_interpretation_notes = [
        "GR 237 is a monthly report produced by the California Department of Social Services (CDSS) documenting county-level data changes in General Relief and Interim Assistance cases. These programs provide cash benefits to thousands of Californians each month.",
        
        "There are no strict rules for interpreting GR 237 data and local sources with a direct tie to impacted communities should be consulted for a full explanation of trends. The Roots Community Health Center, for example, prepared an **[excellent report](https://rootscommunityhealth.org/wp-content/uploads/2014/07/GA_eval_12.pdf)** utilizing and contextualizing data trends in Alameda County's GA Program (which itself recommends a dashboard like this one.)",
        
        "Still, in general, major shifts in month-to-month data are rare and warrant specific explanation. For investigations into major data changes, viewers should check the y-axis range, where the minimum value is the minimum data point, not zero.",
        
        "Beginning in March 2020, CDSS adopted **[a policy](https://www.cdss.ca.gov/portals/9/Data%20De-Identification%20Guidelines%20DSS%20Reference%20Guide_FINAL.pdf)** that replaced values 1-11 with a * (star) for sensitive caseload metrics, which here, includes all non-dollar metrics. CDSS instituted these changes, known as de-identification, to safeguard privacy rights. De-identification has **[widely-appreciated](https://pmc.ncbi.nlm.nih.gov/articles/PMC8110889/)** **[racial equity](https://healthlaw.org/wp-content/uploads/2023/03/Striking-the-Balance_for-publication.pdf)** **[benefits](https://aisp.upenn.edu/wp-content/uploads/2025/02/Centering-Equity-Toolkit-2.0.pdf)** in the context of public data sets without demographic information, like GR 237.",
        
        "All data prior to 2020 was updated accordingly. Where a star appears in a data set, no value is recorded on the graph or in the underlying data. This has important impacts for individuals analyzing data from small counties, where changes in small month-to-month caseloads (and the data associated with them) can be eschewed or disappear entirely.",
        
        "Where a zero appears in a data set, a zero value is recorded on the graph and underlying data."
    ]

    for txt in data_interpretation_notes:
        st.caption(txt, unsafe_allow_html=True)
        
except Exception as err:
    st.error("The app crashed. Here’s the full error (so it won’t look like a blank page):")
    st.exception(err)
