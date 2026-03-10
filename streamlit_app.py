#!/usr/bin/env python3
"""
CES Dashboard — Streamlit version
Run locally : streamlit run streamlit_app.py
Deploy      : Streamlit Community Cloud (connect GitHub repo)
"""

import base64
import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
from openpyxl import load_workbook

# ─── Page config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="CES Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Constants ────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
EXCEL_FILE = os.path.join(BASE_DIR, "Mater_PythonDataFromSnowflake.xlsx")
COMMENTS_CSV = os.path.join(BASE_DIR, "comments.csv")

FILTER_COLS = [
    "Product", "ANC", "Network", "Case_Size", "Band_Size",
    "Screen_Type", "Screen_Size", "Chip", "CPU", "GPU",
    "Unified_Memory", "SSD", "StockAvailability", "Lists_Colour",
    "Robot_Status", "Band_Colour", "Partner",
]
VALUE_COLS = [
    "PriceMinusPoints", "Selling_Price", "Lists_Price",
    "Pointback", "Hidden_Discount", "Displayed_Discount", "Total_Discount",
]
DISPLAY_COLS = [
    "Date", "Partner", "Product", "StockAvailability",
    "Robot_Status", "Selling_Price", "Lists_Price", "Pointback",
    "Hidden_Discount", "Displayed_Discount", "Total_Discount", "PriceMinusPoints",
]
VALUE_LABELS = {
    "Selling_Price":      "Selling Price",
    "PriceMinusPoints":   "Price - Points",
    "Lists_Price":        "List Price",
    "Pointback":          "Pointback",
    "Hidden_Discount":    "Hidden Discount",
    "Displayed_Discount": "Displayed Discount",
    "Total_Discount":     "Total Discount",
}

# ─── Data loading ─────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Loading Excel data…")
def load_data():
    cols_needed = [
        "TASK_CREATED_AT", "CREATED_TS", "ROBOT_NAME", "MONITOR_NAME",
        "Product", "ANC", "Network", "Case_Size", "Band_Size",
        "Screen_Type", "Screen_Size", "Chip", "CPU", "GPU",
        "Unified_Memory", "SSD", "StockAvailability", "Lists_Colour",
        "Robot_Status", "Band_Colour",
        "PriceMinusPoints", "Selling_Price", "Lists_Price",
        "Pointback", "Hidden_Discount", "Displayed_Discount", "Total_Discount",
    ]
    df = pd.read_excel(EXCEL_FILE, sheet_name="CES",
                       usecols=lambda c: c in cols_needed)
    partner_df = pd.read_excel(EXCEL_FILE, sheet_name="CESPartner")
    partner_map = dict(zip(partner_df["ROBOT_NAME"], partner_df["Partner"]))
    df["Partner"] = df["ROBOT_NAME"].map(partner_map).fillna("-")
    df["Date"] = pd.to_datetime(df["CREATED_TS"], format="mixed").dt.strftime("%Y-%m-%d")
    for col in VALUE_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    for col in FILTER_COLS + ["MONITOR_NAME"]:
        df[col] = df[col].fillna("-").astype(str)
    return df


@st.cache_data(show_spinner=False)
def load_bd_focus():
    try:
        bf = pd.read_excel(os.path.join(BASE_DIR, "20260216_BDFocus.xlsx"))
        bf = bf[bf["BDFocus"] == "Yes"]
        conditions = []
        for _, row in bf.iterrows():
            cond = {"Product": str(row["Product"])}
            if pd.notna(row.get("Chip")) and str(row["Chip"]) not in ("", "nan"):
                cond["Chip"] = str(row["Chip"])
            if pd.notna(row.get("SSD")) and str(row["SSD"]) not in ("", "nan"):
                cond["SSD"] = str(row["SSD"])
            if pd.notna(row.get("ScreenSize")) and str(row["ScreenSize"]) not in ("", "nan"):
                cond["Screen_Size"] = str(row["ScreenSize"])
            conditions.append(cond)
        return conditions
    except Exception:
        return []


def load_comments():
    """Load comments from comments.csv (preferred) or Excel Comments sheet."""
    if os.path.exists(COMMENTS_CSV):
        try:
            df = pd.read_csv(COMMENTS_CSV)
            return df.to_dict(orient="records")
        except Exception:
            pass
    try:
        df = pd.read_excel(EXCEL_FILE, sheet_name="Comments")
        return df.to_dict(orient="records")
    except Exception:
        return []


def save_comments(comments_list):
    """
    Persist comments:
    1. Write comments.csv locally.
    2. Commit it to GitHub via Contents API (requires GITHUB_TOKEN + GITHUB_REPO in secrets).
    """
    df = pd.DataFrame(
        comments_list if comments_list else [],
        columns=["Partner", "Date", "Price", "Comment"],
    )
    df.to_csv(COMMENTS_CSV, index=False)

    try:
        token = st.secrets.get("GITHUB_TOKEN", "")
        repo  = st.secrets.get("GITHUB_REPO", "")   # e.g. "YourName/ces-dashboard"
        if not token or not repo:
            return
        url     = f"https://api.github.com/repos/{repo}/contents/comments.csv"
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
        }
        content_b64 = base64.b64encode(df.to_csv(index=False).encode()).decode()
        r   = requests.get(url, headers=headers, timeout=10)
        sha = r.json().get("sha", "") if r.status_code == 200 else ""
        payload = {"message": "Update comments", "content": content_b64}
        if sha:
            payload["sha"] = sha
        requests.put(url, headers=headers, json=payload, timeout=10)
    except Exception:
        pass


# ─── Session state init ───────────────────────────────────────────────────────
if "comments" not in st.session_state:
    st.session_state.comments = load_comments()

# ─── Load data ────────────────────────────────────────────────────────────────
DF                 = load_data()
ALL_DATES          = sorted(DF["Date"].unique())
DATE_MIN           = ALL_DATES[0]
DATE_MAX           = ALL_DATES[-1]
DATE_15            = ALL_DATES[-15] if len(ALL_DATES) >= 15 else DATE_MIN
BD_FOCUS_CONDITIONS = load_bd_focus()

# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## CES Dashboard")
    st.caption(f"Data through **{DATE_MAX}**")
    st.markdown("---")

    param = st.selectbox(
        "Metric",
        options=VALUE_COLS,
        format_func=lambda x: VALUE_LABELS.get(x, x),
    )

    st.markdown("---")

    date_from = st.date_input(
        "From", value=pd.to_datetime(DATE_15),
        min_value=pd.to_datetime(DATE_MIN), max_value=pd.to_datetime(DATE_MAX),
    )
    date_to = st.date_input(
        "To", value=pd.to_datetime(DATE_MAX),
        min_value=pd.to_datetime(DATE_MIN), max_value=pd.to_datetime(DATE_MAX),
    )
    date_from_str = date_from.strftime("%Y-%m-%d")
    date_to_str   = date_to.strftime("%Y-%m-%d")

    bd_focus = st.toggle("BD Focus Only", value=False)

    st.markdown("---")

    # All filter columns as multiselects
    FILTER_LABELS = {
        "Partner": "Partner", "Product": "Product", "ANC": "ANC",
        "Network": "Network", "Case_Size": "Case Size", "Band_Size": "Band Size",
        "Screen_Type": "Screen Type", "Screen_Size": "Screen Size",
        "Chip": "Chip", "CPU": "CPU", "GPU": "GPU",
        "Unified_Memory": "Unified Memory", "SSD": "SSD",
        "StockAvailability": "Stock", "Lists_Colour": "Color",
        "Robot_Status": "Robot Status", "Band_Colour": "Band Color",
    }
    sel_filters = {}
    for col in FILTER_COLS:
        opts = sorted(v for v in DF[col].unique() if v != "-")
        if opts:
            sel_filters[col] = st.multiselect(
                FILTER_LABELS.get(col, col), options=opts, key=f"f_{col}"
            )

# ─── Filter application ───────────────────────────────────────────────────────
def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    df = df[(df["Date"] >= date_from_str) & (df["Date"] <= date_to_str)]
    for col, vals in sel_filters.items():
        if vals:
            df = df[df[col].isin(vals)]
    if bd_focus and BD_FOCUS_CONDITIONS:
        mask = pd.Series(False, index=df.index)
        for cond in BD_FOCUS_CONDITIONS:
            m = pd.Series(True, index=df.index)
            for col, val in cond.items():
                m = m & (df[col] == val)
            mask = mask | m
        df = df[mask]
    return df


filtered_df = apply_filters(DF)
st.sidebar.caption(f"表示中: {len(filtered_df):,} 件 / 全 {len(DF):,} 件")

# ─── Pivot computation ────────────────────────────────────────────────────────
def compute_pivot(df: pd.DataFrame, param: str):
    all_d    = sorted(df["Date"].unique().tolist())
    dates    = all_d[-15:]
    partners = sorted(p for p in df["Partner"].unique() if p != "-")

    prev_date  = all_d[-16] if len(all_d) > 15 else None
    calc_dates = ([prev_date] if prev_date else []) + dates
    df_calc    = df[df["Date"].isin(calc_dates)]

    pivot_vals: dict = {}
    stock_vals: dict = {}

    for (partner, date), group in df_calc.groupby(["Partner", "Date"]):
        if partner == "-":
            continue
        pivot_vals.setdefault(partner, {})
        stock_vals.setdefault(partner, {})
        valid = group[group[param] > 0][param]
        pivot_vals[partner][date] = int(valid.min()) if len(valid) else None
        out = group["StockAvailability"].fillna("").str.lower()
        stock_vals[partner][date] = bool(out.str.contains(r"out|sold").any())

    changes: dict = {}
    for partner in pivot_vals:
        changes[partner] = {}
        prev = pivot_vals[partner].get(prev_date) if prev_date else None
        for date in dates:
            cur = pivot_vals[partner].get(date)
            changes[partner][date] = prev is not None and cur is not None and cur != prev
            if cur is not None:
                prev = cur

    for partner in list(pivot_vals.keys()):
        pivot_vals[partner] = {d: v for d, v in pivot_vals[partner].items() if d in dates}
        stock_vals[partner] = {d: v for d, v in stock_vals[partner].items() if d in dates}

    return dates, partners, pivot_vals, stock_vals, changes


def compute_best_prices(df: pd.DataFrame) -> dict:
    latest = df["Date"].max()
    if not latest:
        return {}
    df_l = df[df["Date"] == latest]
    best = {}
    for col, label in [
        ("Lists_Price",      "List Price"),
        ("Selling_Price",    "Selling Price"),
        ("Pointback",        "Pointback"),
        ("PriceMinusPoints", "Price - Points"),
    ]:
        valid = df_l[(df_l[col] > 0) & (df_l["Partner"] != "-")]
        if not valid.empty:
            idx = valid[col].idxmin()
            best[label] = {
                "partner": str(valid.loc[idx, "Partner"]),
                "value":   int(valid.loc[idx, col]),
                "date":    latest,
            }
    return best

# ─── Pivot DataFrame builder ──────────────────────────────────────────────────
def build_pivot_df(dates, partners, pivot_vals, stock_vals, changes, comments_map):
    col_names = [d[5:] for d in dates]   # MM-DD 表示
    data = {}
    for date, col in zip(dates, col_names):
        col_data = []
        for partner in partners:
            val      = pivot_vals.get(partner, {}).get(date)
            cart_off = stock_vals.get(partner, {}).get(date, False)
            comment  = comments_map.get(f"{partner}|{date}", "")
            if val is None:
                col_data.append("")
            else:
                cell = f"¥{val:,}"
                if cart_off:
                    cell += " ❌"
                if comment:
                    cell += " 💬"
                col_data.append(cell)
        data[col] = col_data

    df = pd.DataFrame(data, index=partners)
    df.index.name = "Partner"
    return df, col_names


def style_pivot(df, dates, col_names, partners, changes):
    styles = pd.DataFrame("", index=df.index, columns=df.columns)
    for date, col in zip(dates, col_names):
        if col not in styles.columns:
            continue
        for partner in partners:
            if partner not in styles.index:
                continue
            if changes.get(partner, {}).get(date, False):
                styles.loc[partner, col] = "color: #1d4ed8; font-weight: bold"
    return df.style.apply(lambda _: styles, axis=None)


# ═══════════════════════════════════════════════════════════════════════════════
# Main layout
# ═══════════════════════════════════════════════════════════════════════════════

st.markdown(
    f"## CES Dashboard &nbsp;"
    f"<span style='font-size:13px;color:#8a93a8;font-weight:400'>"
    f"{len(filtered_df):,} rows · latest {DATE_MAX}"
    f"</span>",
    unsafe_allow_html=True,
)

# ── Best Prices ───────────────────────────────────────────────────────────────
best_prices = compute_best_prices(filtered_df)
if best_prices:
    bp_cols = st.columns(len(best_prices))
    for i, (label, info) in enumerate(best_prices.items()):
        bp_cols[i].metric(
            label=label,
            value=f"¥{info['value']:,}",
            delta=info["partner"],
            delta_color="off",
        )
    st.markdown("")

# ── Pivot table ───────────────────────────────────────────────────────────────
dates, partners, pivot_vals, stock_vals, changes = compute_pivot(filtered_df, param)

comments_map = {
    f"{c.get('Partner')}|{c.get('Date')}": c.get("Comment", "")
    for c in st.session_state.comments
}

st.markdown(
    f"### Pivot Table — {VALUE_LABELS.get(param, param)} &nbsp;"
    f"<span style='font-size:11px;color:#8a93a8'>(直近15日 / 前日比変化=青太字 / ❌=CART OFF / 💬=コメントあり)</span>",
    unsafe_allow_html=True,
)
pivot_df, col_names = build_pivot_df(dates, partners, pivot_vals, stock_vals, changes, comments_map)
styled_pivot = style_pivot(pivot_df, dates, col_names, partners, changes)
st.dataframe(styled_pivot, use_container_width=True)

# ── Comment editor ────────────────────────────────────────────────────────────
with st.expander("Add / Edit Comment"):
    if not partners:
        st.info("No data to comment on.")
    else:
        cc1, cc2 = st.columns(2)
        cm_partner = cc1.selectbox("Partner", options=partners,    key="cm_partner")
        cm_date    = cc2.selectbox("Date",    options=list(reversed(dates)), key="cm_date")

        cur_price = pivot_vals.get(cm_partner, {}).get(cm_date)
        if cur_price:
            st.caption(f"{VALUE_LABELS.get(param, param)}: ¥{cur_price:,}")

        existing = comments_map.get(f"{cm_partner}|{cm_date}", "")
        cm_text  = st.text_area("Comment", value=existing, height=80, key="cm_text")

        btn1, btn2, _ = st.columns([1, 1, 6])
        if btn1.button("Save", type="primary", key="cm_save"):
            updated = [c for c in st.session_state.comments
                       if not (c.get("Partner") == cm_partner and c.get("Date") == cm_date)]
            if cm_text.strip():
                updated.append({
                    "Partner": cm_partner,
                    "Date":    cm_date,
                    "Price":   cur_price or 0,
                    "Comment": cm_text.strip(),
                })
            st.session_state.comments = updated
            save_comments(updated)
            st.success("Saved!")
            st.rerun()

        if existing and btn2.button("Delete", key="cm_delete"):
            updated = [c for c in st.session_state.comments
                       if not (c.get("Partner") == cm_partner and c.get("Date") == cm_date)]
            st.session_state.comments = updated
            save_comments(updated)
            st.success("Deleted!")
            st.rerun()

st.markdown("---")

# ── Charts ────────────────────────────────────────────────────────────────────
ch1, ch2 = st.columns(2)

with ch1:
    st.markdown(f"### Partner Price Trend")
    trend_df = (
        filtered_df[filtered_df[param] > 0]
        .groupby(["Partner", "Date"])[param]
        .min()
        .reset_index()
    )
    if not trend_df.empty:
        fig = px.line(
            trend_df, x="Date", y=param, color="Partner",
            labels={param: VALUE_LABELS.get(param, param), "Date": ""},
            height=300,
        )
        fig.update_layout(
            margin=dict(l=0, r=0, t=10, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data")

with ch2:
    st.markdown(f"### Avg {VALUE_LABELS.get(param, param)} by Partner")
    bar_df = (
        filtered_df[filtered_df[param] > 0]
        .groupby("Partner")[param]
        .mean()
        .round(0)
        .astype(int)
        .reset_index()
        .sort_values(param, ascending=True)
    )
    if not bar_df.empty:
        fig2 = px.bar(
            bar_df, x=param, y="Partner", orientation="h",
            labels={param: VALUE_LABELS.get(param, param), "Partner": ""},
            height=300,
        )
        fig2.update_layout(margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No data")

st.markdown("---")

# ── Data table ────────────────────────────────────────────────────────────────
st.markdown("### Data Table")
latest_date = filtered_df["Date"].max()
if sel_filters.get("Partner"):
    table_df = (
        filtered_df
        .sort_values("Date", ascending=False)
        .drop_duplicates(subset=["Partner", "Product"])[DISPLAY_COLS]
        .sort_values(["Partner", "Product"])
    )
else:
    table_df = (
        filtered_df[filtered_df["Date"] == latest_date][DISPLAY_COLS]
        .sort_values(["Partner", "Product"])
    )

st.dataframe(table_df, use_container_width=True, hide_index=True)
