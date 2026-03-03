#!/usr/bin/env python3
"""
CES Dashboard — Flask version for Render deployment
Local:  python app.py
Deploy: Render.com (see README)
"""

import json
import os
import secrets

import pandas as pd
from flask import (Flask, Response, jsonify, redirect, render_template_string,
                   request, session, url_for)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", secrets.token_hex(32))

# ── 許可メールアドレス ──────────────────────────────────────────────────────────
# 環境変数 ALLOWED_EMAILS にカンマ区切りで設定
# 例: user1@example.com,user2@company.com
_raw = os.environ.get("ALLOWED_EMAILS", "")
ALLOWED_EMAILS = {e.strip().lower() for e in _raw.split(",") if e.strip()}

# ── データ読み込み ──────────────────────────────────────────────────────────────

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
EXCEL_FILE = os.path.join(BASE_DIR, "Mater_PythonDataFromSnowflake.xlsx")

FILTER_COLS = [
    "Product", "Partner", "ANC", "Network", "Case_Size", "Band_Size",
    "Screen_Type", "Screen_Size", "Chip", "CPU", "GPU",
    "Unified_Memory", "SSD", "StockAvailability", "Lists_Colour",
    "Robot_Status", "Band_Colour",
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


def load_data():
    print("Loading Excel data …")
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

    print(f"Loaded {len(df):,} rows. Date range: {df['Date'].min()} → {df['Date'].max()}")
    return df


DF        = load_data()
ALL_DATES = sorted(DF["Date"].unique())
DATE_MIN  = ALL_DATES[0]
DATE_MAX  = ALL_DATES[-1]
DATE_10   = ALL_DATES[-10] if len(ALL_DATES) >= 10 else DATE_MIN

OPTIONS = {col: sorted(DF[col].unique().tolist()) for col in FILTER_COLS}
OPTIONS["Date"] = ALL_DATES


# ── 認証ヘルパー ────────────────────────────────────────────────────────────────

def is_logged_in():
    return session.get("email") in ALLOWED_EMAILS


def require_login(f):
    from functools import wraps
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not is_logged_in():
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapper


# ── フィルター適用 ──────────────────────────────────────────────────────────────

def apply_filters(params):
    df = DF.copy()
    date_from = params.get("date_from", DATE_10)
    date_to   = params.get("date_to",   DATE_MAX)
    if isinstance(date_from, list): date_from = date_from[0]
    if isinstance(date_to,   list): date_to   = date_to[0]
    df = df[(df["Date"] >= date_from) & (df["Date"] <= date_to)]
    for col in FILTER_COLS:
        vals = params.getlist(col) if hasattr(params, "getlist") else params.get(col, [])
        if vals and vals != [""]:
            df = df[df[col].isin(vals)]
    return df


# ── ログイン画面 ────────────────────────────────────────────────────────────────

LOGIN_HTML = """<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CES Dashboard — Login</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: #f4f6f9; display: flex; align-items: center;
    justify-content: center; min-height: 100vh;
  }
  .card {
    background: #fff; border-radius: 16px; padding: 44px 48px;
    box-shadow: 0 4px 24px rgba(0,0,0,0.10); width: 100%; max-width: 400px;
    text-align: center;
  }
  .logo { font-size: 26px; font-weight: 800; color: #141829; margin-bottom: 6px; }
  .logo span { color: #4f8ef7; }
  .subtitle { font-size: 13px; color: #8a93a8; margin-bottom: 32px; }
  label { display: block; text-align: left; font-size: 12px;
          font-weight: 600; color: #4a5568; margin-bottom: 6px; }
  input[type=email] {
    width: 100%; padding: 11px 14px; border: 1px solid #e0e5ef;
    border-radius: 8px; font-size: 14px; color: #2c3e50;
    outline: none; transition: border-color 0.15s;
  }
  input[type=email]:focus { border-color: #4f8ef7; }
  button {
    width: 100%; margin-top: 20px; padding: 12px;
    background: #4f8ef7; color: #fff; border: none;
    border-radius: 8px; font-size: 15px; font-weight: 600;
    cursor: pointer; transition: background 0.15s;
  }
  button:hover { background: #3a7de0; }
  .error {
    margin-top: 16px; padding: 10px 14px; background: #fde8e8;
    border-radius: 8px; color: #c0392b; font-size: 13px;
  }
  .note { margin-top: 20px; font-size: 11px; color: #aaa; }
</style>
</head>
<body>
<div class="card">
  <div class="logo">CES <span>Dashboard</span></div>
  <div class="subtitle">社内限定 — メールアドレスでアクセス</div>
  <form method="POST" action="/login">
    <label for="email">メールアドレス</label>
    <input type="email" id="email" name="email"
           placeholder="you@company.com" required autofocus>
    {% if error %}
    <div class="error">{{ error }}</div>
    {% endif %}
    <button type="submit">アクセスする</button>
  </form>
  <div class="note">登録済みメールアドレスのみアクセスできます</div>
</div>
</body>
</html>
"""


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        if email in ALLOWED_EMAILS:
            session["email"] = email
            return redirect(url_for("index"))
        error = "このメールアドレスはアクセス権がありません。"
    return render_template_string(LOGIN_HTML, error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ── メインページ ────────────────────────────────────────────────────────────────

@app.route("/")
@require_login
def index():
    html = (HTML
            .replace("DATE_MAX_PLACEHOLDER", DATE_MAX)
            .replace("DATE_10_PLACEHOLDER",  DATE_10)
            .replace("DATE_MAX_JS_PLACEHOLDER", DATE_MAX)
            .replace("DATE_MIN_JS_PLACEHOLDER", DATE_MIN)
            .replace("DATE_10_JS_PLACEHOLDER",  DATE_10)
            .replace("USER_EMAIL_PLACEHOLDER", session.get("email", "")))
    return Response(html, mimetype="text/html; charset=utf-8")


# ── API ─────────────────────────────────────────────────────────────────────────

@app.route("/api/options")
@require_login
def api_options():
    return jsonify(OPTIONS)


@app.route("/api/data")
@require_login
def api_data():
    params  = request.args
    param   = params.get("param", "Selling_Price")
    if param not in VALUE_COLS:
        param = "Selling_Price"

    df = apply_filters(params)

    if df.empty:
        return jsonify({
            "kpis": {"count": 0, "avg": 0, "min": 0, "max": 0, "param": param},
            "by_partner": [], "by_product": [], "by_date": [], "table": [],
            "total_rows": 0, "table_mode": "latest",
            "available_options": {},
        })

    valid = df[df[param] > 0][param]
    kpis = {
        "count": int(len(df)),
        "avg":   int(valid.mean()) if len(valid) else 0,
        "min":   int(valid.min())  if len(valid) else 0,
        "max":   int(valid.max())  if len(valid) else 0,
        "param": param,
    }

    by_partner = (
        df[df[param] > 0].groupby("Partner")[param]
        .agg(avg="mean", count="count").reset_index()
        .sort_values("avg", ascending=False)
    )
    by_partner["avg"] = by_partner["avg"].round(0).astype(int)

    by_product = (
        df[df[param] > 0].groupby("Product")[param]
        .agg(avg="mean", count="count").reset_index()
        .sort_values("avg", ascending=False).head(25)
    )
    by_product["avg"] = by_product["avg"].round(0).astype(int)

    by_date = (
        df[df[param] > 0].groupby("Date")[param].mean()
        .reset_index().rename(columns={param: "avg"})
    )
    by_date["avg"] = by_date["avg"].round(0).astype(int)

    partner_filter = [v for v in params.getlist("Partner") if v]
    if partner_filter:
        table_df = (df.sort_values("Date", ascending=False)
                      .drop_duplicates(subset=["Partner", "Product"])[DISPLAY_COLS]
                      .sort_values(["Partner", "Product"]))
        table_mode = "per_product"
    else:
        table_df = df[DISPLAY_COLS].sort_values("Date", ascending=False).head(200)
        table_mode = "latest"

    avail = {col: sorted(df[col].unique().tolist()) for col in FILTER_COLS}

    return jsonify({
        "kpis":             kpis,
        "by_partner":       by_partner.to_dict(orient="records"),
        "by_product":       by_product.to_dict(orient="records"),
        "by_date":          by_date.to_dict(orient="records"),
        "table":            table_df.to_dict(orient="records"),
        "total_rows":       int(len(df)),
        "table_mode":       table_mode,
        "available_options": avail,
    })


@app.route("/api/pivot")
@require_login
def api_pivot():
    params = request.args
    param  = params.get("param", "Selling_Price")
    if param not in VALUE_COLS:
        param = "Selling_Price"

    df = apply_filters(params)
    if df.empty:
        return jsonify({"dates": [], "partners": [], "values": {},
                        "stock": {}, "changes": {}, "param": param})

    all_dates = sorted(df["Date"].unique().tolist())
    dates     = all_dates[-10:]
    partners  = sorted(p for p in df["Partner"].unique().tolist() if p != "-")
    prev_date = all_dates[-11] if len(all_dates) > 10 else None
    calc_dates = ([prev_date] if prev_date else []) + dates
    df_calc = df[df["Date"].isin(calc_dates)]

    pivot_vals, stock_vals = {}, {}
    for (partner, date), group in df_calc.groupby(["Partner", "Date"]):
        if partner == "-":
            continue
        if partner not in pivot_vals:
            pivot_vals[partner] = {}
            stock_vals[partner] = {}
        valid = group[group[param] > 0][param]
        pivot_vals[partner][date] = int(valid.min()) if len(valid) else None
        out = group["StockAvailability"].fillna("").str.lower()
        stock_vals[partner][date] = bool(out.str.contains(r"out|sold").any())

    changes = {}
    for partner in pivot_vals:
        changes[partner] = {}
        prev = pivot_vals[partner].get(prev_date) if prev_date else None
        for date in dates:
            cur = pivot_vals[partner].get(date)
            changes[partner][date] = (prev is not None and cur is not None and cur != prev)
            if cur is not None:
                prev = cur

    for partner in list(pivot_vals.keys()):
        pivot_vals[partner] = {d: v for d, v in pivot_vals[partner].items() if d in dates}
        stock_vals[partner] = {d: v for d, v in stock_vals[partner].items() if d in dates}

    return jsonify({
        "dates":    dates,
        "partners": partners,
        "values":   pivot_vals,
        "stock":    stock_vals,
        "changes":  changes,
        "param":    param,
    })


# ── HTML テンプレート（dashboard.py と共通）────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CES Dashboard</title>
<style>
  :root {
    --bg: #f4f6f9; --header-bg: #141829; --bar-bg: #1e2336;
    --accent: #4f8ef7; --accent2: #34c994; --text: #2c3e50; --muted: #8a93a8;
    --card: #ffffff; --border: #e0e5ef; --danger: #e74c3c; --warn: #f39c12;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: var(--bg); color: var(--text);
         display: flex; flex-direction: column; height: 100vh; overflow: hidden; }

  /* ── Header ── */
  #header {
    background: var(--header-bg); color: #fff; padding: 11px 20px;
    display: flex; align-items: center; gap: 14px; flex-shrink: 0;
  }
  #header h2 { font-size: 17px; font-weight: 700; flex: 1; }
  #header h2 span { color: var(--accent); }
  #row-count { font-size: 12px; color: var(--muted); white-space: nowrap; }
  .param-tabs { display: flex; gap: 4px; flex-wrap: nowrap; }
  .param-btn {
    padding: 4px 11px; border-radius: 20px; border: 1px solid #3a4468;
    background: #2c3450; color: #c8d0e0; cursor: pointer; font-size: 11px;
    transition: all 0.15s; white-space: nowrap;
  }
  .param-btn.active { background: var(--accent); border-color: var(--accent); color: #fff; }
  .param-btn:hover:not(.active) { background: #3a4468; }
  .logout-btn {
    padding: 4px 12px; border-radius: 20px; border: 1px solid #3a4468;
    background: transparent; color: #8a93a8; cursor: pointer; font-size: 11px;
    transition: all 0.15s; white-space: nowrap; text-decoration: none;
  }
  .logout-btn:hover { border-color: var(--danger); color: var(--danger); }
  .user-email { font-size: 11px; color: #6a7490; white-space: nowrap; }

  /* ── Filter bar ── */
  #filter-bar {
    background: var(--bar-bg); padding: 10px 18px 10px;
    display: flex; flex-direction: column; gap: 8px;
    flex-shrink: 0; border-bottom: 1px solid #0d1020;
  }
  #filter-row-top { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
  .date-group { display: flex; align-items: center; gap: 6px; }
  .date-group span { font-size: 11px; color: #8a93a8; white-space: nowrap; }
  .date-input {
    background: #2c3450; border: 1px solid #3a4468; color: #c8d0e0;
    border-radius: 6px; padding: 5px 9px; font-size: 12px; width: 130px;
  }
  #filter-container { display: flex; flex-wrap: wrap; gap: 6px; align-items: flex-start; }
  .filter-chip { position: relative; }
  .filter-chip.hidden { display: none; }
  .chip-btn {
    display: flex; align-items: center; gap: 5px;
    background: #2c3450; border: 1px solid #3a4468; color: #c8d0e0;
    border-radius: 6px; padding: 6px 12px; font-size: 12px; cursor: pointer;
    white-space: nowrap; transition: border-color 0.15s, background 0.15s; min-height: 32px;
  }
  .chip-btn:hover { border-color: var(--accent); background: #243058; }
  .chip-btn.active { border-color: var(--accent); background: #1a3060; }
  .chip-btn.has-selection { border-color: var(--accent); background: #1a3060; color: #fff; }
  .chip-label { font-weight: 500; letter-spacing: 0.2px; }
  .chip-count { color: #7eb8ff; font-weight: 700; font-size: 11px; }
  .chip-arrow { font-size: 9px; opacity: 0.5; margin-left: 1px; }
  .chip-dropdown {
    display: none; position: absolute; top: calc(100% + 5px); left: 0;
    background: #fff; border: 1px solid var(--border); border-radius: 8px;
    box-shadow: 0 8px 24px rgba(0,0,0,0.18); z-index: 200;
    min-width: 180px; max-height: 260px; overflow-y: auto;
  }
  .chip-dropdown.open { display: block; }
  .chip-option {
    display: flex; align-items: center; gap: 8px;
    padding: 7px 14px; font-size: 13px; cursor: pointer;
    color: var(--text); user-select: none; line-height: 1.3;
  }
  .chip-option:hover { background: #f0f4ff; }
  .chip-option input[type=checkbox] {
    cursor: pointer; accent-color: var(--accent); width: 14px; height: 14px; flex-shrink: 0;
  }
  .chip-option.option-hidden { display: none; }
  .reset-btn {
    padding: 6px 14px; border-radius: 6px; min-height: 32px;
    background: transparent; border: 1px solid #3a4468; color: #8a93a8;
    font-size: 12px; cursor: pointer; transition: all 0.15s; white-space: nowrap;
  }
  .reset-btn:hover { border-color: var(--danger); color: var(--danger); background: #2c1e1e; }

  /* ── Content ── */
  #content { flex: 1; overflow-y: auto; padding: 18px 22px; }

  .kpi-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; margin-bottom: 18px; }
  .kpi-card {
    background: var(--card); border-radius: 10px; padding: 14px 16px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06); border-left: 4px solid var(--accent);
  }
  .kpi-card.k2 { border-left-color: var(--accent2); }
  .kpi-card.k3 { border-left-color: var(--warn); }
  .kpi-card.k4 { border-left-color: var(--danger); }
  .kpi-label { font-size: 11px; color: var(--muted); font-weight: 600;
               text-transform: uppercase; letter-spacing: 0.7px; }
  .kpi-value { font-size: 24px; font-weight: 700; color: var(--text); margin-top: 4px; }
  .kpi-sub   { font-size: 11px; color: var(--muted); margin-top: 2px; }

  .charts-row { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; margin-bottom: 18px; }
  .chart-card {
    background: var(--card); border-radius: 10px; padding: 14px 16px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
  }
  .chart-card h3 { font-size: 12px; font-weight: 600; color: var(--text); margin-bottom: 10px; }
  .bar-row { display: flex; align-items: center; margin-bottom: 6px; }
  .bar-label { font-size: 11px; width: 120px; text-align: right; padding-right: 8px;
               color: var(--text); white-space: nowrap; overflow: hidden;
               text-overflow: ellipsis; flex-shrink: 0; }
  .bar-track { flex: 1; background: #f0f2f7; border-radius: 4px; height: 16px; }
  .bar-fill  { height: 100%; border-radius: 4px; transition: width 0.4s ease; }
  .bar-val   { font-size: 11px; color: var(--muted); margin-left: 6px; width: 70px; flex-shrink: 0; }

  .trend-card {
    background: var(--card); border-radius: 10px; padding: 14px 16px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06); margin-bottom: 18px;
  }
  .trend-card h3 { font-size: 12px; font-weight: 600; margin-bottom: 8px; }

  .table-card {
    background: var(--card); border-radius: 10px; padding: 14px 16px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06); margin-bottom: 18px;
  }
  .table-header { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; margin-bottom: 8px; }
  .table-header h3 { font-size: 12px; font-weight: 600; flex-shrink: 0; }
  .tbl-chip { position: relative; }
  .tbl-chip-btn {
    display: flex; align-items: center; gap: 5px;
    background: #f7f9fc; border: 1px solid var(--border); color: var(--text);
    border-radius: 6px; padding: 5px 10px; font-size: 12px; cursor: pointer;
    white-space: nowrap; transition: border-color 0.15s, background 0.15s; min-height: 30px;
  }
  .tbl-chip-btn:hover { border-color: var(--accent); }
  .tbl-chip-btn.has-sel { border-color: var(--accent); background: #e8f0fe; color: #1d4ed8; font-weight: 600; }
  .tbl-chip-cnt { color: #1d4ed8; font-weight: 700; font-size: 11px; }
  .tbl-chip-arr { font-size: 9px; opacity: 0.45; }
  .tbl-chip-dd {
    display: none; position: absolute; top: calc(100% + 4px); left: 0;
    background: #fff; border: 1px solid var(--border); border-radius: 8px;
    box-shadow: 0 8px 24px rgba(0,0,0,0.14); z-index: 300;
    min-width: 190px; max-height: 240px; overflow-y: auto;
  }
  .tbl-chip-dd.open { display: block; }
  .tbl-chip-opt {
    display: flex; align-items: center; gap: 8px;
    padding: 7px 12px; font-size: 12px; cursor: pointer; color: var(--text); user-select: none;
  }
  .tbl-chip-opt:hover { background: #f0f4ff; }
  .tbl-chip-opt input[type=checkbox] {
    cursor: pointer; accent-color: var(--accent); width: 13px; height: 13px; flex-shrink: 0;
  }
  .table-wrap { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; font-size: 12px; }
  thead th {
    background: #f7f9fc; color: var(--muted); font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.5px; font-size: 10px;
    padding: 7px 10px; border-bottom: 2px solid var(--border);
    text-align: left; white-space: nowrap;
  }
  tbody td { padding: 6px 10px; border-bottom: 1px solid var(--border); white-space: nowrap; }
  tbody tr:hover { background: #f7f9fc; }
  .badge { display: inline-block; padding: 2px 7px; border-radius: 12px; font-size: 10px; font-weight: 600; }
  .badge-ok  { background: #d4f0e8; color: #1a7a54; }
  .badge-err { background: #fde8e8; color: #c0392b; }
  .badge-in  { background: #dbeafe; color: #1d4ed8; }
  .badge-out { background: #fee2e2; color: #b91c1c; }
  .badge-few { background: #fef3c7; color: #92400e; }
  .loading { text-align: center; padding: 40px; color: var(--muted); }
  .spinner { display: inline-block; width: 20px; height: 20px;
             border: 3px solid var(--border); border-top-color: var(--accent);
             border-radius: 50%; animation: spin 0.7s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }

  .pivot-card {
    background: var(--card); border-radius: 10px; padding: 14px 16px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06); margin-bottom: 18px;
  }
  .pivot-card h3 { font-size: 12px; font-weight: 600; margin-bottom: 8px; }
  .pivot-wrap { overflow-x: auto; max-height: 340px; overflow-y: auto; }
  .pivot-table { border-collapse: collapse; font-size: 11px; white-space: nowrap; }
  .pivot-table thead th {
    position: sticky; top: 0; z-index: 2;
    background: #f7f9fc; color: var(--muted); font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.4px; font-size: 10px;
    padding: 6px 10px; border-bottom: 2px solid var(--border); border-right: 1px solid var(--border);
  }
  .pivot-table thead th.partner-head { position: sticky; left: 0; z-index: 3; min-width: 100px; text-align: left; }
  .pivot-table thead th.date-head { text-align: center; min-width: 74px; }
  .pivot-table tbody td {
    padding: 5px 10px; border-bottom: 1px solid var(--border);
    border-right: 1px solid var(--border); text-align: right; background: #fff;
  }
  .pivot-table tbody td.partner-cell {
    position: sticky; left: 0; z-index: 1;
    background: #f7f9fc; font-weight: 600; text-align: left; border-right: 2px solid var(--border);
  }
  .pivot-table tbody tr:hover td { filter: brightness(0.97); }
  .cell-changed { color: #1d4ed8; font-weight: 700; }
  .cell-null { color: #ccc; }
  .cell-cartoff { display: block; font-size: 9px; color: #e74c3c; font-weight: 600;
                  margin-top: 2px; letter-spacing: 0.3px; }

  .partner-chart-card {
    background: var(--card); border-radius: 10px; padding: 14px 16px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06); margin-bottom: 18px;
  }
  .partner-chart-card h3 { font-size: 12px; font-weight: 600; margin-bottom: 6px; }
  #partnerChartCanvas { display: block; width: 100%; }
  .chart-legend { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 8px; }
  .legend-item { display: flex; align-items: center; gap: 5px; font-size: 11px; color: var(--text); }
  .legend-dot { width: 11px; height: 11px; border-radius: 50%; flex-shrink: 0; }
</style>
</head>
<body>

<div id="header">
  <h2>CES <span>Dashboard</span></h2>
  <span id="row-count">Loading…</span>
  <div class="param-tabs" id="param-tabs"></div>
  <span class="user-email" id="user-email">USER_EMAIL_PLACEHOLDER</span>
  <a href="/logout" class="logout-btn">Logout</a>
</div>

<div id="filter-bar">
  <div id="filter-row-top">
    <div class="date-group">
      <span>From</span>
      <input type="date" class="date-input" id="f_date_from" value="DATE_10_PLACEHOLDER">
      <span>To</span>
      <input type="date" class="date-input" id="f_date_to" value="DATE_MAX_PLACEHOLDER">
    </div>
    <button class="reset-btn" onclick="clearFilters()">Reset Filters</button>
  </div>
  <div id="filter-container"></div>
</div>

<div id="content">
  <div class="loading"><div class="spinner"></div></div>
</div>

<script>
const VALUE_COLS = ["PriceMinusPoints","Selling_Price","Lists_Price",
                    "Pointback","Hidden_Discount","Displayed_Discount","Total_Discount"];
const VALUE_LABELS = {
  PriceMinusPoints:"Price - Points", Selling_Price:"Selling Price", Lists_Price:"List Price",
  Pointback:"Pointback", Hidden_Discount:"Hidden Discount",
  Displayed_Discount:"Displayed Discount", Total_Discount:"Total Discount",
};
const CHART_COLORS = ["#4f8ef7","#34c994","#f39c12","#e74c3c","#9b59b6",
                      "#1abc9c","#e67e22","#3498db","#2ecc71","#e91e63"];
const PARTNER_COLORS = ["#4f8ef7","#e74c3c","#34c994","#f39c12","#9b59b6",
                        "#1abc9c","#e67e22","#3498db","#e91e63","#00bcd4"];

let currentParam = "Selling_Price";
let debounceTimer = null;
let options = {};
let _tableStore = { rows: [], paramCol: "Selling_Price", tableMode: "latest" };
const DATE_MAX = "DATE_MAX_JS_PLACEHOLDER";
const DATE_MIN = "DATE_MIN_JS_PLACEHOLDER";
const DATE_10  = "DATE_10_JS_PLACEHOLDER";

async function init() {
  const resp = await fetch("/api/options");
  options = await resp.json();
  buildParamTabs();
  buildFilters();
  refresh();
}

function buildParamTabs() {
  const tabs = document.getElementById("param-tabs");
  VALUE_COLS.forEach(col => {
    const btn = document.createElement("button");
    btn.className = "param-btn" + (col === currentParam ? " active" : "");
    btn.textContent = VALUE_LABELS[col];
    btn.onclick = () => { currentParam = col; updateParamTabs(); refresh(); };
    btn.id = "tab_" + col;
    tabs.appendChild(btn);
  });
}
function updateParamTabs() {
  VALUE_COLS.forEach(col =>
    document.getElementById("tab_" + col).classList.toggle("active", col === currentParam));
}

function buildFilters() {
  const container = document.getElementById("filter-container");
  const FILTER_ORDER = ["Product","Partner","ANC","Network","Case_Size","Band_Size",
    "Screen_Type","Screen_Size","Chip","CPU","GPU","Unified_Memory","SSD",
    "StockAvailability","Lists_Colour","Robot_Status","Band_Colour"];
  const FCOLS = FILTER_ORDER.filter(k => options[k] !== undefined);
  FCOLS.forEach(col => {
    const chip = document.createElement("div");
    chip.className = "filter-chip"; chip.id = "fc_" + col;
    const btn = document.createElement("button");
    btn.className = "chip-btn"; btn.id = "cb_" + col;
    btn.innerHTML =
      `<span class="chip-label">${col.replace(/_/g, " ")}</span>` +
      `<span class="chip-count" id="cc_${col}"></span>` +
      `<span class="chip-arrow">▾</span>`;
    btn.addEventListener("click", e => { e.stopPropagation(); toggleDropdown(col); });
    const dd = document.createElement("div");
    dd.className = "chip-dropdown"; dd.id = "cd_" + col;
    dd.addEventListener("click", e => e.stopPropagation());
    const opts = document.createElement("div");
    opts.id = "co_" + col;
    options[col].forEach(val => {
      const lbl = document.createElement("label");
      lbl.className = "chip-option";
      const cb = document.createElement("input");
      cb.type = "checkbox"; cb.value = val;
      cb.addEventListener("change", () => { updateChipCount(col); scheduleRefresh(); });
      lbl.appendChild(cb);
      lbl.appendChild(document.createTextNode(" " + val));
      opts.appendChild(lbl);
    });
    dd.appendChild(opts); chip.appendChild(btn); chip.appendChild(dd);
    container.appendChild(chip);
  });
  document.addEventListener("click", closeAllDropdowns);
  document.getElementById("f_date_from").addEventListener("change", scheduleRefresh);
  document.getElementById("f_date_to").addEventListener("change", scheduleRefresh);
}

function toggleDropdown(col) {
  const dd = document.getElementById("cd_" + col);
  const wasOpen = dd.classList.contains("open");
  closeAllDropdowns();
  if (!wasOpen) { dd.classList.add("open"); document.getElementById("cb_" + col).classList.add("active"); }
}
function closeAllDropdowns() {
  document.querySelectorAll(".chip-dropdown.open").forEach(d => {
    d.classList.remove("open");
    const col = d.id.replace("cd_", "");
    const btn = document.getElementById("cb_" + col);
    if (btn) btn.classList.remove("active");
  });
}
function updateChipCount(col) {
  const n = document.querySelectorAll(`#co_${col} input:checked`).length;
  const el = document.getElementById("cc_" + col);
  if (el) el.textContent = n > 0 ? ` (${n})` : "";
}
function scheduleRefresh() {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(refresh, 300);
}
function clearFilters() {
  Object.keys(options).filter(k => k !== "Date").forEach(col => {
    document.querySelectorAll(`#co_${col} input`).forEach(cb => cb.checked = false);
    updateChipCount(col);
  });
  document.getElementById("f_date_from").value = DATE_10;
  document.getElementById("f_date_to").value = DATE_MAX;
  refresh();
}
function buildQuery() {
  const parts = ["param=" + currentParam];
  parts.push("date_from=" + document.getElementById("f_date_from").value);
  parts.push("date_to="   + document.getElementById("f_date_to").value);
  Object.keys(options).filter(k => k !== "Date").forEach(col => {
    document.querySelectorAll(`#co_${col} input:checked`).forEach(cb =>
      parts.push(encodeURIComponent(col) + "=" + encodeURIComponent(cb.value)));
  });
  return parts.join("&");
}
async function refresh() {
  const resp = await fetch("/api/data?" + buildQuery());
  const data = await resp.json();
  render(data);
}
function updateFilterOptions(avail) {
  Object.keys(options).filter(k => k !== "Date").forEach(col => {
    const chip = document.getElementById("fc_" + col);
    if (!chip) return;
    const availSet = new Set(avail[col] || []);
    const anySelected = document.querySelectorAll(`#co_${col} input:checked`).length > 0;
    const meaningful  = (avail[col] || []).filter(v => v !== "-");
    if (meaningful.length <= 1 && !anySelected) { chip.classList.add("hidden"); return; }
    chip.classList.remove("hidden");
    document.querySelectorAll(`#co_${col} label.chip-option`).forEach(lbl => {
      const cb = lbl.querySelector("input");
      const inAvail = availSet.has(cb.value);
      if (!inAvail && !cb.checked) { lbl.classList.add("option-hidden"); }
      else { lbl.classList.remove("option-hidden"); lbl.style.opacity = inAvail ? "1" : "0.5"; }
    });
  });
}
function fmt(n) {
  if (n === null || n === undefined) return "-";
  return n.toLocaleString("ja-JP");
}
function render(data) {
  document.getElementById("row-count").textContent = `${data.total_rows.toLocaleString()} rows`;
  const label = VALUE_LABELS[data.kpis.param] || data.kpis.param;
  document.getElementById("content").innerHTML = `
    <div class="pivot-card">
      <h3 id="pivot-title">Partner × Date Pivot Table</h3>
      <div class="pivot-wrap" id="pivot-table-wrap"><div class="loading"><div class="spinner"></div></div></div>
    </div>
    <div class="partner-chart-card">
      <h3 id="partner-chart-title">Partner Price Trend</h3>
      <canvas id="partnerChartCanvas"></canvas>
      <div class="chart-legend" id="partnerLegend"></div>
    </div>
    <div class="table-card">
      <div class="table-header">
        <h3 id="table-title">Data Table (latest 200 rows)</h3>
        <div class="tbl-chip" id="tbl-chip-partner">
          <button class="tbl-chip-btn" id="tbl-btn-partner" onclick="toggleTblDD('partner')">
            Partner <span class="tbl-chip-cnt" id="tbl-cnt-partner"></span><span class="tbl-chip-arr">▾</span>
          </button>
          <div class="tbl-chip-dd" id="tbl-dd-partner"><div id="tbl-opts-partner"></div></div>
        </div>
        <div class="tbl-chip" id="tbl-chip-product">
          <button class="tbl-chip-btn" id="tbl-btn-product" onclick="toggleTblDD('product')">
            Product <span class="tbl-chip-cnt" id="tbl-cnt-product"></span><span class="tbl-chip-arr">▾</span>
          </button>
          <div class="tbl-chip-dd" id="tbl-dd-product"><div id="tbl-opts-product"></div></div>
        </div>
      </div>
      <div class="table-wrap" id="data-table"></div>
    </div>
    <div class="kpi-row">
      <div class="kpi-card"><div class="kpi-label">Rows</div><div class="kpi-value">${fmt(data.kpis.count)}</div><div class="kpi-sub">filtered records</div></div>
      <div class="kpi-card k2"><div class="kpi-label">Avg ${label}</div><div class="kpi-value">¥${fmt(data.kpis.avg)}</div><div class="kpi-sub">excl. zero values</div></div>
      <div class="kpi-card k3"><div class="kpi-label">Min ${label}</div><div class="kpi-value">¥${fmt(data.kpis.min)}</div><div class="kpi-sub">excl. zero values</div></div>
      <div class="kpi-card k4"><div class="kpi-label">Max ${label}</div><div class="kpi-value">¥${fmt(data.kpis.max)}</div><div class="kpi-sub">excl. zero values</div></div>
    </div>
    <div class="charts-row">
      <div class="chart-card"><h3>Avg ${label} by Partner</h3><div id="chart-partner"></div></div>
      <div class="chart-card"><h3>Avg ${label} by Product (Top 25)</h3><div id="chart-product"></div></div>
    </div>
    <div class="trend-card"><h3>Daily Avg ${label} Trend</h3><canvas id="trendCanvas"></canvas></div>
  `;
  renderBarChart("chart-partner", data.by_partner, "Partner", "avg", CHART_COLORS);
  renderBarChart("chart-product", data.by_product, "Product", "avg", CHART_COLORS, true);
  renderTrend(data.by_date);
  renderTable(data.table, data.kpis.param, data.table_mode);
  updateFilterOptions(data.available_options || {});
  loadPivot();
}
function renderBarChart(containerId, rows, labelKey, valueKey, colors) {
  const el = document.getElementById(containerId);
  if (!rows.length) { el.innerHTML = "<p style='color:#aaa;font-size:12px'>No data</p>"; return; }
  const maxVal = Math.max(...rows.map(r => r[valueKey]));
  el.innerHTML = rows.map((r, i) => {
    const pct = maxVal > 0 ? (r[valueKey] / maxVal * 100).toFixed(1) : 0;
    return `<div class="bar-row">
      <div class="bar-label" title="${r[labelKey]}">${r[labelKey]}</div>
      <div class="bar-track"><div class="bar-fill" style="width:${pct}%;background:${colors[i%colors.length]}"></div></div>
      <div class="bar-val">¥${fmt(r[valueKey])}<br><span style="font-size:10px;color:#aaa">${r.count} rows</span></div>
    </div>`;
  }).join("");
}
function renderTrend(byDate) {
  const canvas = document.getElementById("trendCanvas");
  if (!canvas || !byDate.length) return;
  const ctx = canvas.getContext("2d");
  const W = Math.max(canvas.parentElement.getBoundingClientRect().width - 32, 400);
  const H = 150;
  canvas.width = W; canvas.height = H;
  const padL=60, padR=20, padT=10, padB=28, cW=W-padL-padR, cH=H-padT-padB;
  ctx.clearRect(0, 0, W, H);
  const vals = byDate.map(d => d.avg);
  const maxV = Math.max(...vals), minV = Math.min(...vals);
  ctx.strokeStyle="#f0f2f7"; ctx.lineWidth=1;
  for (let i=0;i<=4;i++) {
    const y=padT+(cH/4)*i;
    ctx.beginPath(); ctx.moveTo(padL,y); ctx.lineTo(W-padR,y); ctx.stroke();
    ctx.fillStyle="#aaa"; ctx.font="10px sans-serif"; ctx.textAlign="right";
    ctx.fillText("¥"+Math.round(maxV-((maxV-minV)/4)*i).toLocaleString(), padL-4, y+4);
  }
  const step=Math.max(1,Math.floor(byDate.length/8));
  ctx.fillStyle="#aaa"; ctx.textAlign="center";
  byDate.forEach((d,i) => { if(i%step===0) ctx.fillText(d.Date.slice(5), padL+(i/(byDate.length-1||1))*cW, H-padB+13); });
  ctx.strokeStyle="#4f8ef7"; ctx.lineWidth=2;
  ctx.beginPath();
  byDate.forEach((d,i) => {
    const x=padL+(i/(byDate.length-1||1))*cW, y=padT+(maxV>minV?(1-(d.avg-minV)/(maxV-minV))*cH:cH/2);
    i===0?ctx.moveTo(x,y):ctx.lineTo(x,y);
  });
  ctx.stroke();
  ctx.fillStyle="rgba(79,142,247,0.1)";
  ctx.beginPath();
  byDate.forEach((d,i) => {
    const x=padL+(i/(byDate.length-1||1))*cW, y=padT+(maxV>minV?(1-(d.avg-minV)/(maxV-minV))*cH:cH/2);
    i===0?ctx.moveTo(x,y):ctx.lineTo(x,y);
  });
  ctx.lineTo(padL+cW,padT+cH); ctx.lineTo(padL,padT+cH); ctx.closePath(); ctx.fill();
}
function stockBadge(v) {
  if (!v||v==="-") return `<span class="badge">${v}</span>`;
  const lv=v.toLowerCase();
  if (lv.includes("out")||lv.includes("sold")) return `<span class="badge badge-out">${v}</span>`;
  if (lv.includes("few")) return `<span class="badge badge-few">${v}</span>`;
  if (lv.includes("in stock")) return `<span class="badge badge-in">${v}</span>`;
  return `<span class="badge">${v}</span>`;
}
function robotBadge(v) {
  return v==="OK"?`<span class="badge badge-ok">OK</span>`:`<span class="badge badge-err">${v}</span>`;
}
function populateTblFilter(key, values, keepSel) {
  const container = document.getElementById(`tbl-opts-${key}`);
  if (!container) return;
  container.innerHTML = values.map(v =>
    `<label class="tbl-chip-opt"><input type="checkbox" value="${v}"${keepSel.has(v)?" checked":""} onchange="onTblFilterChange()"> ${v}</label>`
  ).join("");
  updateTblCount(key);
}
function updateTblCount(key) {
  const n = document.querySelectorAll(`#tbl-opts-${key} input:checked`).length;
  const cnt = document.getElementById(`tbl-cnt-${key}`);
  const btn = document.getElementById(`tbl-btn-${key}`);
  if (cnt) cnt.textContent = n > 0 ? `(${n})` : "";
  if (btn) btn.classList.toggle("has-sel", n > 0);
}
function toggleTblDD(key) {
  const dd = document.getElementById(`tbl-dd-${key}`);
  const wasOpen = dd.classList.contains("open");
  closeTblDDs();
  if (!wasOpen) dd.classList.add("open");
}
function closeTblDDs() {
  document.querySelectorAll(".tbl-chip-dd.open").forEach(d => d.classList.remove("open"));
}
document.addEventListener("click", e => { if (!e.target.closest(".tbl-chip")) closeTblDDs(); });
function renderTable(rows, paramCol, tableMode) {
  _tableStore = { rows, paramCol, tableMode };
  const partners = [...new Set(rows.map(r=>r["Partner"]).filter(p=>p&&p!=="-"))].sort();
  const products = [...new Set(rows.map(r=>r["Product"]).filter(p=>p&&p!=="-"))].sort();
  populateTblFilter("partner", partners, new Set());
  populateTblFilter("product", products, new Set());
  _drawTable(rows, paramCol, tableMode);
}
function onTblFilterChange() { updateTblCount("partner"); updateTblCount("product"); filterTable(); }
async function filterTable() {
  const pvals = [...document.querySelectorAll("#tbl-opts-partner input:checked")].map(c=>c.value);
  const vals  = [...document.querySelectorAll("#tbl-opts-product input:checked")].map(c=>c.value);
  const { rows, paramCol, tableMode } = _tableStore;
  if (pvals.length > 0 && tableMode === "latest") {
    const parts = ["param="+currentParam,
      "date_from="+document.getElementById("f_date_from").value,
      "date_to="+document.getElementById("f_date_to").value];
    Object.keys(options).filter(k=>k!=="Date"&&k!=="Partner").forEach(col => {
      document.querySelectorAll(`#co_${col} input:checked`).forEach(cb =>
        parts.push(encodeURIComponent(col)+"="+encodeURIComponent(cb.value)));
    });
    pvals.forEach(p => parts.push("Partner="+encodeURIComponent(p)));
    const data = await (await fetch("/api/data?"+parts.join("&"))).json();
    const prevProdSel = new Set(vals);
    const products = [...new Set(data.table.map(r=>r["Product"]).filter(p=>p&&p!=="-"))].sort();
    populateTblFilter("product", products, prevProdSel);
    let fresh = data.table;
    if (vals.length > 0) fresh = fresh.filter(r=>vals.includes(r["Product"]));
    _drawTable(fresh, paramCol, "per_product");
  } else if (pvals.length === 0 && tableMode === "latest") {
    const prevProdSel = new Set(vals);
    const products = [...new Set(rows.map(r=>r["Product"]).filter(p=>p&&p!=="-"))].sort();
    populateTblFilter("product", products, prevProdSel);
    const filtered = vals.length > 0 ? rows.filter(r=>vals.includes(r["Product"])) : rows;
    _drawTable(filtered, paramCol, tableMode);
  } else {
    let filtered = rows;
    if (pvals.length > 0) filtered = filtered.filter(r=>pvals.includes(r["Partner"]));
    if (vals.length > 0)  filtered = filtered.filter(r=>vals.includes(r["Product"]));
    _drawTable(filtered, paramCol, tableMode);
  }
}
function _drawTable(rows, paramCol, tableMode) {
  const el = document.getElementById("data-table");
  const titleEl = document.getElementById("table-title");
  if (!rows.length) { el.innerHTML="<p style='color:#aaa;padding:16px'>No data</p>"; return; }
  let cols, labels;
  if (tableMode === "per_product") {
    if (titleEl) titleEl.textContent = "Latest Data per Product";
    const uniqPartners = new Set(rows.map(r=>r["Partner"]).filter(p=>p&&p!=="-"));
    const showPartner = uniqPartners.size > 1;
    cols = [...(showPartner?["Partner"]:[]),
            "Product","StockAvailability","Robot_Status",
            "Selling_Price","Lists_Price","Pointback",
            "Hidden_Discount","Displayed_Discount","Total_Discount","PriceMinusPoints"];
    labels = { Partner:"Partner", Product:"Product", StockAvailability:"Stock", Robot_Status:"Status",
               Selling_Price:"Selling Price", Lists_Price:"List Price", Pointback:"Pointback",
               Hidden_Discount:"Hidden Disc", Displayed_Discount:"Disp Disc",
               Total_Discount:"Total Disc", PriceMinusPoints:"Price - Points" };
  } else {
    if (titleEl) titleEl.textContent = "Data Table (latest 200 rows)";
    cols = ["Date","Partner","Product","StockAvailability","Robot_Status",
            "Selling_Price","Lists_Price","Pointback",
            "Hidden_Discount","Displayed_Discount","Total_Discount","PriceMinusPoints"];
    labels = { Date:"Date", Partner:"Partner", Product:"Product",
               StockAvailability:"Stock", Robot_Status:"Status",
               Selling_Price:"Selling Price", Lists_Price:"List Price", Pointback:"Pointback",
               Hidden_Discount:"Hidden Disc", Displayed_Discount:"Disp Disc",
               Total_Discount:"Total Disc", PriceMinusPoints:"Price - Points" };
  }
  const head = cols.map(c => `<th${c===paramCol?" style='background:#e8f0fe;color:#1d4ed8'":""}>${labels[c]||c}</th>`).join("");
  const body = rows.map(r => {
    const cells = cols.map(c => {
      let v = r[c] ?? "–";
      if (c==="StockAvailability") return `<td>${stockBadge(v)}</td>`;
      if (c==="Robot_Status")      return `<td>${robotBadge(v)}</td>`;
      const hl = c===paramCol?" style='background:#f0f4ff;font-weight:600'":"";
      if (typeof v==="number") return `<td${hl}>¥${fmt(v)}</td>`;
      return `<td${hl}>${v}</td>`;
    }).join("");
    return `<tr>${cells}</tr>`;
  }).join("");
  el.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}
async function loadPivot() {
  const wrap = document.getElementById("pivot-table-wrap");
  if (!wrap) return;
  wrap.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
  const data = await (await fetch("/api/pivot?"+buildQuery())).json();
  renderPivotTable(data);
  renderPartnerLineChart(data);
}
function renderPivotTable(data) {
  const wrap = document.getElementById("pivot-table-wrap");
  if (!wrap) return;
  const { dates, partners, values, stock, changes, param } = data;
  document.getElementById("pivot-title").textContent =
    `Partner × Date Pivot Table — ${VALUE_LABELS[param]||param} (Min, latest 10 days)`;
  if (!dates.length||!partners.length) { wrap.innerHTML="<p style='color:#aaa;padding:12px'>No data</p>"; return; }
  const head = `<tr><th class="partner-head">Partner</th>${dates.map(d=>`<th class="date-head">${d.slice(5)}</th>`).join("")}</tr>`;
  const body = partners.map(partner => {
    const cells = dates.map(date => {
      const val=values[partner]&&values[partner][date];
      const chg=changes[partner]&&changes[partner][date];
      const oos=stock[partner]&&stock[partner][date];
      const nullCls=(val===null||val===undefined)?" cell-null":"";
      const chgCls=chg?" cell-changed":"";
      const disp=(val!==null&&val!==undefined)?"¥"+val.toLocaleString():"-";
      const cartTag=oos?`<span class="cell-cartoff">Cart off</span>`:"";
      return `<td class="${nullCls}${chgCls}">${disp}${cartTag}</td>`;
    }).join("");
    return `<tr><td class="partner-cell">${partner}</td>${cells}</tr>`;
  }).join("");
  wrap.innerHTML=`<table class="pivot-table"><thead>${head}</thead><tbody>${body}</tbody></table>`;
}
function fmtShort(v) {
  if (v >= 10000) return "¥"+Math.round(v/1000)+"K";
  return "¥"+Math.round(v).toLocaleString();
}
let _partnerChartState = null;
function renderPartnerLineChart(data) {
  const canvas = document.getElementById("partnerChartCanvas");
  const legend = document.getElementById("partnerLegend");
  if (!canvas||!legend) return;
  const { dates, partners, values, param } = data;
  document.getElementById("partner-chart-title").textContent =
    `Partner Price Trend — ${VALUE_LABELS[param]||param}`;
  legend.innerHTML = "";
  if (!dates.length||!partners.length) { canvas.style.display="none"; return; }
  canvas.style.display="block";
  const W=Math.max(canvas.parentElement.getBoundingClientRect().width-32,400), H=340;
  canvas.width=W; canvas.height=H;
  const padL=72,padR=24,padT=24,padB=36,cW=W-padL-padR,cH=H-padT-padB;
  let allVals=[];
  partners.forEach(p=>dates.forEach(d=>{const v=values[p]&&values[p][d];if(v>0)allVals.push(v);}));
  if (!allVals.length) {
    const ctx=canvas.getContext("2d"); ctx.fillStyle="#aaa"; ctx.font="13px sans-serif"; ctx.textAlign="center";
    ctx.fillText("No data",W/2,H/2); return;
  }
  const maxV=Math.max(...allVals),minV=Math.min(...allVals),rng=maxV-minV||1;
  const vMax=maxV+rng*0.08,vMin=Math.max(0,minV-rng*0.08);
  const allPoints=[];
  partners.forEach((partner,pi)=>{
    const color=PARTNER_COLORS[pi%PARTNER_COLORS.length];
    const pts=dates.map((d,i)=>{
      const v=values[partner]&&values[partner][d];
      if(!v||v<=0) return null;
      const x=padL+(dates.length>1?i/(dates.length-1):0.5)*cW;
      const y=padT+(1-(v-vMin)/(vMax-vMin))*cH;
      return {x,y,v,color,pi,partner,di:i};
    });
    allPoints.push({partner,pi,color,pts});
  });
  const LW=44,LH=10,GAP=2,candidates=[];
  allPoints.forEach(({color,pts,pi})=>{pts.forEach(p=>{if(p)candidates.push({x:p.x,y:p.y,v:p.v,color,pi});});});
  const tryOffsets=[-13,13,-24,24,-35,35,-46,46,-57,57,-68,68],placed=[],byX={};
  candidates.forEach(c=>{const key=Math.round(c.x);if(!byX[key])byX[key]=[];byX[key].push(c);});
  Object.values(byX).forEach(group=>{
    group.sort((a,b)=>a.y-b.y);
    group.forEach(c=>{
      const txt=fmtShort(c.v),tw=Math.min(txt.length*6.5,LW);
      const lx1=c.x-tw/2-1,lx2=c.x+tw/2+1;
      let placedY=c.y-13;
      for(const off of tryOffsets){
        const ty=c.y+off;
        if(ty<padT||ty>padT+cH+LH) continue;
        const ly1=ty-LH,ly2=ty+GAP;
        const collision=placed.some(p=>lx1<p.x2&&lx2>p.x1&&ly1<p.y2&&ly2>p.y1);
        if(!collision){placedY=ty;break;}
      }
      c.placedY=placedY;
      placed.push({x1:lx1,y1:placedY-LH,x2:lx2,y2:placedY+GAP});
    });
  });
  _partnerChartState={canvas,dates,allPoints,candidates,padL,padR,padT,padB,cW,cH,W,H,vMax,vMin};
  drawPartnerChart(_partnerChartState,null);
  canvas.onmousemove=function(e){
    if(!_partnerChartState) return;
    const rect=canvas.getBoundingClientRect();
    const mx=(e.clientX-rect.left)*(canvas.width/rect.width);
    const my=(e.clientY-rect.top)*(canvas.height/rect.height);
    let nearest=null,minDist=28;
    _partnerChartState.allPoints.forEach(({pts,partner,color})=>{
      pts.forEach(p=>{
        if(!p) return;
        const d=Math.hypot(p.x-mx,p.y-my);
        if(d<minDist){minDist=d;nearest={...p,partner,color,date:_partnerChartState.dates[p.di]};}
      });
    });
    drawPartnerChart(_partnerChartState,nearest);
  };
  canvas.onmouseleave=function(){if(_partnerChartState)drawPartnerChart(_partnerChartState,null);};
  allPoints.forEach(({partner,color})=>{
    const item=document.createElement("div"); item.className="legend-item";
    item.innerHTML=`<div class="legend-dot" style="background:${color}"></div>${partner}`;
    legend.appendChild(item);
  });
}
function drawPartnerChart(state,hoverPt){
  const{canvas,dates,allPoints,candidates,padL,padR,padT,padB,cW,cH,W,H,vMax,vMin}=state;
  const ctx=canvas.getContext("2d");
  ctx.clearRect(0,0,W,H);
  ctx.strokeStyle="#f0f2f7"; ctx.lineWidth=1;
  for(let i=0;i<=5;i++){
    const y=padT+(cH/5)*i,val=vMax-((vMax-vMin)/5)*i;
    ctx.beginPath();ctx.moveTo(padL,y);ctx.lineTo(W-padR,y);ctx.stroke();
    ctx.fillStyle="#aaa";ctx.font="10px sans-serif";ctx.textAlign="right";
    ctx.fillText("¥"+Math.round(val).toLocaleString(),padL-4,y+3);
  }
  ctx.fillStyle="#aaa";ctx.textAlign="center";
  dates.forEach((d,i)=>{const x=padL+(dates.length>1?i/(dates.length-1):0.5)*cW;ctx.fillText(d.slice(5),x,H-padB+14);});
  allPoints.forEach(({color,pts})=>{
    ctx.strokeStyle=color;ctx.lineWidth=2;ctx.setLineDash([]);
    ctx.beginPath();let started=false;
    pts.forEach(p=>{if(!p){started=false;return;}if(!started){ctx.moveTo(p.x,p.y);started=true;}else ctx.lineTo(p.x,p.y);});
    ctx.stroke();
  });
  allPoints.forEach(({color,pts})=>{
    pts.forEach(p=>{
      if(!p) return;
      if(hoverPt&&Math.abs(p.x-hoverPt.x)<1&&Math.abs(p.y-hoverPt.y)<1) return;
      ctx.fillStyle=color;ctx.beginPath();ctx.arc(p.x,p.y,3,0,Math.PI*2);ctx.fill();
    });
  });
  candidates.forEach(c=>{
    ctx.font="9px sans-serif";ctx.fillStyle=c.color;ctx.textAlign="center";
    ctx.fillText(fmtShort(c.v),c.x,c.placedY);
  });
  if(hoverPt){
    ctx.fillStyle=hoverPt.color;ctx.beginPath();ctx.arc(hoverPt.x,hoverPt.y,6,0,Math.PI*2);ctx.fill();
    ctx.strokeStyle="#fff";ctx.lineWidth=2.5;ctx.beginPath();ctx.arc(hoverPt.x,hoverPt.y,6,0,Math.PI*2);ctx.stroke();
    const line1=hoverPt.partner,line2=hoverPt.date||"",line3="¥"+Math.round(hoverPt.v).toLocaleString();
    const TW=152,TH=68,TR=7;
    let tx=hoverPt.x+14,ty=hoverPt.y-TH-12;
    if(tx+TW>W-padR+4) tx=hoverPt.x-TW-14;
    if(ty<padT-4) ty=hoverPt.y+14;
    ctx.save();ctx.shadowColor="rgba(0,0,0,0.18)";ctx.shadowBlur=12;ctx.shadowOffsetY=2;
    ctx.fillStyle="#fff";ctx.beginPath();
    if(ctx.roundRect){ctx.roundRect(tx,ty,TW,TH,TR);}else{ctx.rect(tx,ty,TW,TH);}
    ctx.fill();ctx.restore();
    ctx.fillStyle=hoverPt.color;ctx.beginPath();
    if(ctx.roundRect){ctx.roundRect(tx,ty,4,TH,[TR,0,0,TR]);}else{ctx.rect(tx,ty,4,TH);}
    ctx.fill();
    ctx.strokeStyle=hoverPt.color;ctx.lineWidth=1.5;ctx.beginPath();
    if(ctx.roundRect){ctx.roundRect(tx,ty,TW,TH,TR);}else{ctx.rect(tx,ty,TW,TH);}
    ctx.stroke();
    ctx.fillStyle=hoverPt.color;ctx.font="bold 11px sans-serif";ctx.textAlign="left";
    ctx.fillText(line1,tx+12,ty+18);
    ctx.fillStyle="#999";ctx.font="10px sans-serif";ctx.fillText(line2,tx+12,ty+33);
    ctx.fillStyle="#1a1a2e";ctx.font="bold 18px sans-serif";ctx.fillText(line3,tx+12,ty+57);
  }
}
init();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
