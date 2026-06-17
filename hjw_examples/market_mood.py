#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Generate and email an A-share market breadth and short-term sentiment report from Tushare.

Environment:
  TUSHARE_TOKEN=your_tushare_token
  EMAIL_USER=your_sender_email
  EMAIL_PASS=your_sender_password
  RECIPIENT_EMAIL=your_recipient_email

Examples:
  py hjw_examples/market_mood.py
  py hjw_examples/market_mood.py --trade-date 20260617
  py hjw_examples/market_mood.py --trade-date 20260617 --email-subject "A股赚钱效应日报"
"""

from __future__ import print_function

import argparse
import html
import json
import math
import os
import re
import sys
from datetime import datetime, timedelta

import pandas as pd
import tushare as ts


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
DEFAULT_OUTPUT_DIR = os.path.join(ROOT, "statics", "outputs")
DEFAULT_CACHE_DIR = os.path.join(ROOT, "statics", "cache")
EMAIL_ENV_VARS = ["EMAIL_USER", "EMAIL_PASS", "RECIPIENT_EMAIL"]
TOP_CONCEPT_LIMIT = 10
KPL_CONCEPT_PAGE_SIZE = 3000


def parse_args():
    parser = argparse.ArgumentParser(
        description="Use Tushare to calculate A-share money effect and short-term sentiment."
    )
    parser.add_argument(
        "--trade-date",
        help="Trade date in YYYYMMDD. Default: today; if not open, use latest previous open day.",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for raw JSON/CSV side files. Default: ./outputs",
    )
    parser.add_argument(
        "--cache-dir",
        default=DEFAULT_CACHE_DIR,
        help="Directory for cache files. Default: ./cache",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("TUSHARE_TOKEN"),
        help="Tushare token. Prefer using TUSHARE_TOKEN env var instead of passing it here.",
    )
    parser.add_argument(
        "--email-subject",
        help="Email subject. Default: generated from trade date, money effect grade and sentiment stage.",
    )
    parser.add_argument(
        "--refresh-basic",
        action="store_true",
        help="Refresh cached stock_basic data.",
    )
    parser.add_argument(
        "--save-raw",
        action="store_true",
        default=True,
        help="Save raw merged daily and limit list CSV files. Default: true.",
    )
    return parser.parse_args()


def ensure_dir(path):
    if not os.path.isdir(path):
        os.makedirs(path)


def fail(message, code=1):
    print("ERROR: {0}".format(message), file=sys.stderr)
    sys.exit(code)


def ensure_email_config():
    missing = [name for name in EMAIL_ENV_VARS if not os.environ.get(name)]
    if missing:
        fail("Email config is missing: {0}".format(", ".join(missing)))


def send_report_email(html_content, subject):
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    from src.notify import send_email

    send_email(html_content, subject)


def get_pro(token):
    if not token:
        fail("TUSHARE_TOKEN is missing. Set it before running, e.g. $env:TUSHARE_TOKEN='your_token'")
    ts.set_token(token)
    return ts.pro_api(token)


def call_api(pro, api_name, **kwargs):
    if hasattr(pro, api_name):
        return getattr(pro, api_name)(**kwargs)
    return pro.query(api_name, **kwargs)


def today_yyyymmdd():
    return datetime.now().strftime("%Y%m%d")


def yyyymmdd_to_display(date_text):
    return "{0}-{1}-{2}".format(date_text[:4], date_text[4:6], date_text[6:8])


def normalize_date(date_text):
    if not date_text:
        return today_yyyymmdd()
    text = re.sub(r"\D", "", date_text)
    if len(text) != 8:
        fail("--trade-date must be YYYYMMDD, got: {0}".format(date_text))
    return text


def get_latest_trade_date(pro, target_date):
    start_date = (datetime.strptime(target_date, "%Y%m%d") - timedelta(days=20)).strftime("%Y%m%d")
    cal = call_api(
        pro,
        "trade_cal",
        exchange="SSE",
        start_date=start_date,
        end_date=target_date,
        fields="cal_date,is_open,pretrade_date",
    )
    if cal.empty:
        fail("trade_cal returned empty data for {0} to {1}".format(start_date, target_date))
    cal = cal.sort_values("cal_date")
    open_days = cal[cal["is_open"].astype(int) == 1]["cal_date"].tolist()
    if not open_days:
        fail("No open trading day found before {0}".format(target_date))
    return open_days[-1]


def get_previous_trade_date(pro, trade_date):
    start_date = (datetime.strptime(trade_date, "%Y%m%d") - timedelta(days=20)).strftime("%Y%m%d")
    cal = call_api(
        pro,
        "trade_cal",
        exchange="SSE",
        start_date=start_date,
        end_date=trade_date,
        fields="cal_date,is_open",
    )
    cal = cal.sort_values("cal_date")
    open_days = cal[cal["is_open"].astype(int) == 1]["cal_date"].tolist()
    open_days = [d for d in open_days if d < trade_date]
    if not open_days:
        fail("No previous open trading day found before {0}".format(trade_date))
    return open_days[-1]


def load_stock_basic(pro, cache_dir, refresh=False):
    ensure_dir(cache_dir)
    cache_path = os.path.join(cache_dir, "stock_basic_L.csv")
    if os.path.exists(cache_path) and not refresh:
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(cache_path))
            if datetime.now() - mtime < timedelta(days=7):
                return pd.read_csv(cache_path, dtype={"symbol": str, "list_date": str})
        except Exception:
            pass

    fields = "ts_code,symbol,name,area,market,list_date"
    df = call_api(pro, "stock_basic", exchange="", list_status="L", fields=fields)
    if df.empty:
        fail("stock_basic returned empty data")
    df.to_csv(cache_path, index=False, encoding="utf-8-sig")
    return df


def to_numeric(df, cols):
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def fetch_daily(pro, trade_date):
    fields = "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount"
    df = call_api(pro, "daily", trade_date=trade_date, fields=fields)
    if df.empty:
        fail("daily returned empty data for {0}. Data may not be updated yet.".format(trade_date))
    return to_numeric(df, ["open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"])


def fetch_daily_basic(pro, trade_date):
    fields = "ts_code,trade_date,turnover_rate,volume_ratio,total_mv,circ_mv"
    try:
        df = call_api(pro, "daily_basic", trade_date=trade_date, fields=fields)
    except Exception as exc:
        print("WARN: daily_basic failed: {0}".format(exc), file=sys.stderr)
        return pd.DataFrame()
    return to_numeric(df, ["turnover_rate", "volume_ratio", "total_mv", "circ_mv"])


def fetch_limit_list(pro, trade_date):
    try:
        df = call_api(pro, "limit_list_d", trade_date=trade_date)
    except Exception as exc:
        print("WARN: limit_list_d failed for {0}: {1}".format(trade_date, exc), file=sys.stderr)
        return pd.DataFrame()
    if df.empty:
        return df
    numeric_cols = [
        "close",
        "pct_chg",
        "amount",
        "limit_amount",
        "float_mv",
        "total_mv",
        "turnover_ratio",
        "fd_amount",
        "open_times",
        "limit_times",
    ]
    return to_numeric(df, numeric_cols)


def fetch_kpl_concept_cons(pro, trade_date):
    fields = "ts_code,name,con_code,con_name"
    chunks = []
    offset = 0
    try:
        while True:
            df = call_api(
                pro,
                "kpl_concept_cons",
                trade_date=trade_date,
                fields=fields,
                limit=KPL_CONCEPT_PAGE_SIZE,
                offset=offset,
            )
            if df is None or df.empty:
                break
            chunks.append(df)
            if len(df) < KPL_CONCEPT_PAGE_SIZE:
                break
            offset += KPL_CONCEPT_PAGE_SIZE
    except Exception as exc:
        if chunks:
            print(
                "WARN: kpl_concept_cons partial data for {0} at offset {1}: {2}".format(trade_date, offset, exc),
                file=sys.stderr,
            )
        else:
            try:
                return call_api(pro, "kpl_concept_cons", trade_date=trade_date, fields=fields)
            except Exception as fallback_exc:
                print(
                    "WARN: kpl_concept_cons failed for {0}: {1}; fallback failed: {2}".format(
                        trade_date, exc, fallback_exc
                    ),
                    file=sys.stderr,
                )
                return pd.DataFrame()
    if not chunks:
        return pd.DataFrame()
    return pd.concat(chunks, ignore_index=True).drop_duplicates()


def is_common_a_share(ts_code):
    return isinstance(ts_code, str) and (
        ts_code.endswith(".SH") or ts_code.endswith(".SZ") or ts_code.endswith(".BJ")
    )


def filter_a_share_traded(daily, basic):
    daily = daily[daily["ts_code"].map(is_common_a_share)].copy()
    if basic is None or basic.empty:
        return daily
    keep_cols = [c for c in ["ts_code", "name", "market", "list_date"] if c in basic.columns]
    merged = daily.merge(basic[keep_cols], on="ts_code", how="left")
    return merged


def classify_limit_row(row):
    text_parts = []
    for col in ["limit", "limit_type", "status", "limit_status"]:
        if col in row and pd.notnull(row[col]):
            text_parts.append(str(row[col]).strip().upper())
    text = "|".join(text_parts)

    pct = row.get("pct_chg", float("nan"))
    if any(x in text for x in ["D", "跌", "DOWN"]):
        return "down"
    if any(x in text for x in ["Z", "炸", "OPEN"]):
        return "broken"
    if any(x in text for x in ["U", "涨", "UP"]):
        return "up"
    if pd.notnull(pct):
        if pct > 0:
            return "up"
        if pct < 0:
            return "down"
    return "unknown"


def enrich_limit_list(limit_df):
    if limit_df.empty:
        return limit_df
    df = limit_df.copy()
    df["limit_side"] = df.apply(classify_limit_row, axis=1)
    if "limit_times" in df.columns:
        df["limit_times_num"] = pd.to_numeric(df["limit_times"], errors="coerce").fillna(1)
    else:
        df["limit_times_num"] = 1

    # Some Tushare versions expose up_stat as text like "3天2板"; parse the last number as a fallback.
    if "up_stat" in df.columns:
        parsed = df["up_stat"].astype(str).str.extract(r"(\d+)\D*$")[0]
        parsed = pd.to_numeric(parsed, errors="coerce")
        df["limit_times_num"] = df["limit_times_num"].where(df["limit_times_num"].notnull(), parsed)
        df["limit_times_num"] = df["limit_times_num"].combine_first(parsed).fillna(1)
    return df


def safe_div(a, b):
    if b is None or b == 0 or pd.isnull(b):
        return 0.0
    return float(a) / float(b)


def pct_text(x, digits=1):
    if x is None or pd.isnull(x):
        return "-"
    return "{0:.{1}f}%".format(float(x), digits)


def num_text(x, digits=2):
    if x is None or pd.isnull(x):
        return "-"
    if isinstance(x, float):
        return "{0:.{1}f}".format(x, digits)
    return str(x)


def calc_money_effect(daily, limit_df, prev_limit_up, trade_date):
    pct = daily["pct_chg"].dropna()
    total = int(len(pct))
    up = int((pct > 0).sum())
    down = int((pct < 0).sum())
    flat = int((pct == 0).sum())
    up3 = int((pct >= 3).sum())
    down3 = int((pct <= -3).sum())
    up5 = int((pct >= 5).sum())
    down5 = int((pct <= -5).sum())

    limit_up = 0
    limit_down = 0
    limit_broken = 0
    opened_after_limit = 0
    if not limit_df.empty:
        limit_up = int((limit_df["limit_side"] == "up").sum())
        limit_down = int((limit_df["limit_side"] == "down").sum())
        limit_broken = int((limit_df["limit_side"] == "broken").sum())
        if "open_times" in limit_df.columns:
            opened_after_limit = int(
                ((limit_df["limit_side"] == "up") & (pd.to_numeric(limit_df["open_times"], errors="coerce") > 0)).sum()
            )

    touched_up = limit_up + limit_broken
    true_broken_rate = safe_div(limit_broken, touched_up) if touched_up else None
    opened_rate_proxy = safe_div(opened_after_limit, limit_up) if limit_up else None

    prev_feedback = calc_previous_limit_feedback(daily, prev_limit_up)

    adv_ratio = safe_div(up, total)
    median_pct = float(pct.median()) if total else float("nan")
    avg_pct = float(pct.mean()) if total else float("nan")
    strong_weak_ratio = safe_div(up3 + 1, down3 + 1)

    score = 0.0
    score += min(35.0, max(0.0, adv_ratio * 35.0))
    if median_pct >= 1.5:
        score += 20
    elif median_pct >= 0.5:
        score += 16
    elif median_pct >= 0:
        score += 12
    elif median_pct >= -0.5:
        score += 8
    elif median_pct >= -1.5:
        score += 4

    limit_net = limit_up - limit_down
    if limit_net >= 60:
        score += 20
    elif limit_net >= 30:
        score += 16
    elif limit_net >= 10:
        score += 12
    elif limit_net >= 0:
        score += 8
    elif limit_net >= -20:
        score += 4

    if strong_weak_ratio >= 3:
        score += 15
    elif strong_weak_ratio >= 1.5:
        score += 11
    elif strong_weak_ratio >= 0.8:
        score += 7
    elif strong_weak_ratio >= 0.4:
        score += 3

    yavg = prev_feedback["avg_pct"]
    if yavg is not None and not pd.isnull(yavg):
        if yavg >= 2:
            score += 10
        elif yavg >= 0:
            score += 7
        elif yavg >= -2:
            score += 3

    rate_for_penalty = true_broken_rate if true_broken_rate is not None else opened_rate_proxy
    if rate_for_penalty is not None:
        if rate_for_penalty >= 0.50:
            score -= 10
        elif rate_for_penalty >= 0.35:
            score -= 6
        elif rate_for_penalty >= 0.25:
            score -= 3
    score = max(0.0, min(100.0, score))

    if score >= 70:
        grade = "强"
    elif score >= 35:
        grade = "中"
    else:
        grade = "弱"

    return {
        "trade_date": trade_date,
        "total": total,
        "up": up,
        "down": down,
        "flat": flat,
        "up_ratio": adv_ratio,
        "avg_pct": avg_pct,
        "median_pct": median_pct,
        "up3": up3,
        "down3": down3,
        "up5": up5,
        "down5": down5,
        "limit_up": limit_up,
        "limit_down": limit_down,
        "limit_broken": limit_broken,
        "opened_after_limit": opened_after_limit,
        "true_broken_rate": true_broken_rate,
        "opened_rate_proxy": opened_rate_proxy,
        "prev_limit_feedback": prev_feedback,
        "score": score,
        "grade": grade,
    }


def calc_previous_limit_feedback(daily, prev_limit_up):
    if prev_limit_up is None or prev_limit_up.empty:
        return {
            "count": 0,
            "avg_pct": None,
            "median_pct": None,
            "positive_ratio": None,
            "continue_limit_count": 0,
            "continue_limit_ratio": None,
        }

    codes = set(prev_limit_up["ts_code"].dropna().tolist())
    today = daily[daily["ts_code"].isin(codes)].copy()
    if today.empty:
        return {
            "count": 0,
            "avg_pct": None,
            "median_pct": None,
            "positive_ratio": None,
            "continue_limit_count": 0,
            "continue_limit_ratio": None,
        }
    pct = today["pct_chg"].dropna()
    continue_limit_count = int((pct >= 9.5).sum())
    return {
        "count": int(len(today)),
        "avg_pct": float(pct.mean()) if len(pct) else None,
        "median_pct": float(pct.median()) if len(pct) else None,
        "positive_ratio": safe_div(int((pct > 0).sum()), len(pct)) if len(pct) else None,
        "continue_limit_count": continue_limit_count,
        "continue_limit_ratio": safe_div(continue_limit_count, len(pct)) if len(pct) else None,
    }


def calc_sentiment(today_limit, prev_limit, money):
    limit_up = money["limit_up"]
    limit_down = money["limit_down"]
    yavg = money["prev_limit_feedback"]["avg_pct"]
    y_continue = money["prev_limit_feedback"]["continue_limit_ratio"]
    broken_rate = money["true_broken_rate"]
    opened_proxy = money["opened_rate_proxy"]
    rate_for_risk = broken_rate if broken_rate is not None else opened_proxy

    prev_limit_up_count = int((prev_limit["limit_side"] == "up").sum()) if prev_limit is not None and not prev_limit.empty else 0
    prev_limit_down_count = int((prev_limit["limit_side"] == "down").sum()) if prev_limit is not None and not prev_limit.empty else 0

    limit_height = 0
    second_board_count = 0
    if today_limit is not None and not today_limit.empty:
        up_df = today_limit[today_limit["limit_side"] == "up"].copy()
        if not up_df.empty:
            limit_height = int(pd.to_numeric(up_df["limit_times_num"], errors="coerce").fillna(1).max())
            second_board_count = int((pd.to_numeric(up_df["limit_times_num"], errors="coerce").fillna(1) >= 2).sum())

    reasons = []
    if limit_up > prev_limit_up_count:
        reasons.append("涨停数较上一交易日增加")
    elif limit_up < prev_limit_up_count:
        reasons.append("涨停数较上一交易日减少")
    if limit_down > prev_limit_down_count:
        reasons.append("跌停数增加")
    if yavg is not None and not pd.isnull(yavg):
        reasons.append("昨日涨停今日平均收益 {0}".format(pct_text(yavg)))
    if rate_for_risk is not None:
        reasons.append("炸板/开板风险指标 {0}".format(pct_text(rate_for_risk * 100)))
    if limit_height:
        reasons.append("连板高度 {0} 板".format(limit_height))

    heat_score = float(money["score"])
    if limit_up >= 120:
        heat_score += 12
    elif limit_up >= 80:
        heat_score += 8
    elif limit_up >= 50:
        heat_score += 4
    elif limit_up <= 15:
        heat_score -= 8
    elif limit_up <= 25:
        heat_score -= 4

    if prev_limit_up_count > 0:
        if limit_up >= prev_limit_up_count * 1.25:
            heat_score += 6
        elif limit_up <= prev_limit_up_count * 0.65:
            heat_score -= 8

    if limit_height >= 7:
        heat_score += 10
    elif limit_height >= 5:
        heat_score += 7
    elif limit_height >= 3:
        heat_score += 3

    if limit_down >= max(15, limit_up * 0.5):
        heat_score -= 15
    elif limit_down >= max(10, limit_up * 0.3):
        heat_score -= 8
    elif limit_down <= max(3, limit_up * 0.1):
        heat_score += 3

    if yavg is not None and not pd.isnull(yavg):
        if yavg >= 3:
            heat_score += 8
        elif yavg >= 1:
            heat_score += 5
        elif yavg >= 0:
            heat_score += 2
        elif yavg <= -3:
            heat_score -= 8
        elif yavg <= -1:
            heat_score -= 5
        else:
            heat_score -= 2

    if y_continue is not None and not pd.isnull(y_continue):
        if y_continue >= 0.35:
            heat_score += 5
        elif y_continue >= 0.20:
            heat_score += 3
        elif y_continue <= 0.05:
            heat_score -= 4

    if rate_for_risk is not None:
        if rate_for_risk >= 0.50:
            heat_score -= 12
        elif rate_for_risk >= 0.35:
            heat_score -= 8
        elif rate_for_risk >= 0.25:
            heat_score -= 4
        elif rate_for_risk <= 0.15:
            heat_score += 3

    ice_condition = (
        (limit_up <= 20 and limit_down >= 10)
        or (limit_up <= 15 and yavg is not None and yavg < -1)
        or (money["median_pct"] <= -1.5 and money["up_ratio"] < 0.30)
    )
    cold_condition = (
        (yavg is not None and yavg < 0 and (rate_for_risk is not None and rate_for_risk >= 0.35))
        or (limit_down >= max(10, limit_up * 0.4))
        or (prev_limit_up_count > 0 and limit_up < prev_limit_up_count * 0.65 and yavg is not None and yavg < 0)
    )
    boiling_condition = (
        (limit_up >= 120 or limit_height >= 7)
        and (yavg is None or yavg >= 1.5)
        and (rate_for_risk is None or rate_for_risk < 0.25)
        and limit_down <= max(6, limit_up * 0.10)
    )
    overheat_condition = (
        (limit_up >= 80 or limit_height >= 5)
        and (yavg is None or yavg >= 1)
        and (rate_for_risk is None or rate_for_risk < 0.35)
        and limit_down <= max(8, limit_up * 0.15)
    )
    micro_hot_condition = (
        (
            limit_up > prev_limit_up_count
            and limit_down <= prev_limit_down_count + 3
            and (yavg is None or yavg >= -1)
        )
        or (yavg is not None and yavg >= 0 and money["up_ratio"] >= 0.45)
    )

    if ice_condition:
        heat_score = min(heat_score, 20)
    elif cold_condition:
        heat_score = min(heat_score, 34)
    elif boiling_condition:
        heat_score = max(heat_score, 85)
    elif overheat_condition:
        heat_score = max(heat_score, 70)
    elif micro_hot_condition:
        heat_score = max(heat_score, 55)

    heat_score = max(0.0, min(100.0, heat_score))
    if heat_score >= 85:
        stage = "沸点"
    elif heat_score >= 70:
        stage = "过热"
    elif heat_score >= 55:
        stage = "微热"
    elif heat_score >= 40:
        stage = "微冷"
    elif heat_score >= 25:
        stage = "过冷"
    else:
        stage = "冰点"

    return {
        "stage": stage,
        "heat_score": heat_score,
        "limit_height": limit_height,
        "second_board_count": second_board_count,
        "prev_limit_up_count": prev_limit_up_count,
        "prev_limit_down_count": prev_limit_down_count,
        "yesterday_continue_ratio": y_continue,
        "reasons": reasons,
    }


def normalize_stock_code(code):
    if code is None or pd.isnull(code):
        return None
    text = str(code).strip().upper()
    if not text:
        return None
    if text.endswith(".KP"):
        return None
    match = re.match(r"^(\d{6})\.(SH|SZ|BJ)$", text)
    if match:
        return text
    match = re.match(r"^(SH|SZ|BJ)(\d{6})$", text)
    if match:
        return "{0}.{1}".format(match.group(2), match.group(1))
    match = re.search(r"(\d{6})", text)
    if not match:
        return None
    digits = match.group(1)
    if digits.startswith(("60", "68", "90")):
        return "{0}.SH".format(digits)
    if digits.startswith(("00", "30", "20")):
        return "{0}.SZ".format(digits)
    if digits.startswith(("43", "83", "87", "88", "92")):
        return "{0}.BJ".format(digits)
    return None


def pick_stock_code_col(df):
    candidates = ["con_code", "stock_code", "stock_ts_code", "member_code", "symbol", "code", "ts_code"]
    best_col = None
    best_score = 0
    sample_size = min(len(df), 200)
    for col in candidates:
        if col not in df.columns:
            continue
        score = int(df[col].head(sample_size).map(normalize_stock_code).notnull().sum())
        if score > best_score:
            best_col = col
            best_score = score
    return best_col if best_score > 0 else None


def pick_concept_name_col(df, stock_col):
    # kpl_concept_cons fields: ts_code/name are concept id/name; con_code/con_name are stock code/name.
    if "name" in df.columns and "con_code" in df.columns:
        return "name"

    candidates = [
        "concept_name",
        "c_name",
        "topic_name",
        "theme_name",
        "theme",
        "concept",
        "plate_name",
        "bk_name",
        "题材名称",
        "概念名称",
        "题材",
        "概念",
    ]
    stock_name_cols = {"con_name", "ts_name", "stock_name", "sec_name"}
    for col in candidates:
        if col in df.columns and col != stock_col and col not in stock_name_cols:
            return col
    return None


def split_concept_names(value):
    if value is None or pd.isnull(value):
        return []
    text = str(value).strip()
    if not text:
        return []
    return [part.strip() for part in re.split(r"[,，;；|、/]+", text) if part.strip()]


def prepare_kpl_concepts(kpl_concept_cons):
    empty = pd.DataFrame(columns=["ts_code", "concept_name"])
    if kpl_concept_cons is None or kpl_concept_cons.empty:
        return empty

    stock_col = pick_stock_code_col(kpl_concept_cons)
    concept_col = pick_concept_name_col(kpl_concept_cons, stock_col)
    if not stock_col or not concept_col:
        print(
            "WARN: kpl_concept_cons missing stock/concept columns. columns={0}".format(
                ",".join(kpl_concept_cons.columns.astype(str))
            ),
            file=sys.stderr,
        )
        return empty

    records = []
    for _, row in kpl_concept_cons[[stock_col, concept_col]].iterrows():
        ts_code = normalize_stock_code(row[stock_col])
        if not ts_code:
            continue
        for concept_name in split_concept_names(row[concept_col]):
            records.append({"ts_code": ts_code, "concept_name": concept_name})

    if not records:
        return empty
    df = pd.DataFrame(records)
    return df.drop_duplicates(["ts_code", "concept_name"]).reset_index(drop=True)


def aggregate_limit_concepts(limit_df, kpl_concepts):
    if limit_df is None or limit_df.empty or kpl_concepts is None or kpl_concepts.empty:
        return []
    up_df = limit_df[limit_df["limit_side"] == "up"].copy()
    if up_df.empty:
        return []
    up_codes = set(up_df["ts_code"].dropna().map(normalize_stock_code).dropna().tolist())
    df = kpl_concepts[kpl_concepts["ts_code"].isin(up_codes)].drop_duplicates(["concept_name", "ts_code"])
    if df.empty:
        return []
    grouped = (
        df.groupby("concept_name")
        .agg(limit_up_count=("ts_code", "nunique"))
        .sort_values("limit_up_count", ascending=False)
        .head(TOP_CONCEPT_LIMIT)
        .reset_index()
    )
    return grouped.to_dict(orient="records")


def top_movers_by_concept(daily, kpl_concepts):
    if kpl_concepts is None or kpl_concepts.empty:
        return []
    df = daily[["ts_code", "pct_chg"]].merge(kpl_concepts, on="ts_code", how="inner")
    df = df.dropna(subset=["concept_name", "pct_chg"]).drop_duplicates(["concept_name", "ts_code"])
    if df.empty:
        return []
    g = df.groupby("concept_name").agg(
        stock_count=("ts_code", "nunique"),
        avg_pct=("pct_chg", "mean"),
        median_pct=("pct_chg", "median"),
        up_ratio=("pct_chg", lambda s: float((s > 0).sum()) / float(len(s)) if len(s) else 0),
        up3_count=("pct_chg", lambda s: int((s >= 3).sum())),
    )
    g = g.sort_values(["avg_pct", "up3_count"], ascending=False)
    qualified = g[g["stock_count"] >= 5]
    if len(qualified) >= TOP_CONCEPT_LIMIT:
        g = qualified.head(TOP_CONCEPT_LIMIT)
    else:
        rest = g[~g.index.isin(qualified.index)]
        g = pd.concat([qualified, rest]).head(TOP_CONCEPT_LIMIT)
    g = g.reset_index()
    return g.to_dict(orient="records")


def html_text(value):
    if value is None:
        return "-"
    try:
        if pd.isnull(value):
            return "-"
    except (TypeError, ValueError):
        pass
    return html.escape(str(value), quote=False)


def html_table(headers, rows, numeric_from=1):
    parts = ["<table>", "<thead><tr>"]
    for header in headers:
        parts.append("<th>{0}</th>".format(html_text(header)))
    parts.append("</tr></thead>")
    parts.append("<tbody>")
    for row in rows:
        parts.append("<tr>")
        for idx, cell in enumerate(row):
            cls = ' class="num"' if idx >= numeric_from else ""
            parts.append("<td{0}>{1}</td>".format(cls, html_text(cell)))
        parts.append("</tr>")
    parts.append("</tbody></table>")
    return "\n".join(parts)


def html_list(items):
    lines = ["<ul>"]
    for item in items:
        lines.append("<li>{0}</li>".format(html_text(item)))
    lines.append("</ul>")
    return "\n".join(lines)


def make_email_subject(trade_date, money, sentiment):
    return "A股赚钱效应与情绪日报 {0} {1}/{2}".format(
        yyyymmdd_to_display(trade_date),
        money["grade"],
        sentiment["stage"],
    )


def make_email_html(trade_date, prev_trade_date, money, sentiment, top_concepts, limit_concepts, raw_paths):
    yfb = money["prev_limit_feedback"]

    money_rows = [
        ("可交易样本数", money["total"]),
        ("上涨 / 下跌 / 平盘", "{0} / {1} / {2}".format(money["up"], money["down"], money["flat"])),
        ("上涨家数占比", pct_text(money["up_ratio"] * 100)),
        ("平均涨跌幅", pct_text(money["avg_pct"])),
        ("涨跌幅中位数", pct_text(money["median_pct"])),
        ("涨幅 >= 3% / 跌幅 <= -3%", "{0} / {1}".format(money["up3"], money["down3"])),
        ("涨幅 >= 5% / 跌幅 <= -5%", "{0} / {1}".format(money["up5"], money["down5"])),
        ("涨停 / 跌停", "{0} / {1}".format(money["limit_up"], money["limit_down"])),
    ]
    if money["true_broken_rate"] is not None:
        money_rows.append(("炸板率", pct_text(money["true_broken_rate"] * 100)))
    elif money["opened_rate_proxy"] is not None:
        money_rows.append(("开板率近似值", pct_text(money["opened_rate_proxy"] * 100)))
    else:
        money_rows.append(("炸板率", "-"))
    money_rows.append(("昨日涨停今日平均收益", pct_text(yfb["avg_pct"])))
    money_rows.append(
        (
            "昨日涨停今日红盘率",
            pct_text(yfb["positive_ratio"] * 100) if yfb["positive_ratio"] is not None else "-",
        )
    )
    money_rows.append(
        (
            "昨日涨停晋级率",
            pct_text(yfb["continue_limit_ratio"] * 100) if yfb["continue_limit_ratio"] is not None else "-",
        )
    )

    sentiment_rows = [
        ("情绪阶段", sentiment["stage"]),
        ("热度评分", "{0:.1f}/100".format(sentiment["heat_score"])),
        ("连板高度", sentiment["limit_height"]),
        ("二板及以上家数", sentiment["second_board_count"]),
        ("今日涨停数 vs 昨日涨停数", "{0} / {1}".format(money["limit_up"], sentiment["prev_limit_up_count"])),
        ("今日跌停数 vs 昨日跌停数", "{0} / {1}".format(money["limit_down"], sentiment["prev_limit_down_count"])),
    ]

    sections = [
        "<!doctype html>",
        '<html><head><meta charset="utf-8">',
        "<style>",
        "body{font-family:Arial,'Microsoft YaHei',sans-serif;color:#1f2933;line-height:1.6;margin:0;padding:24px;background:#f6f8fa;}",
        ".wrap{max-width:920px;margin:0 auto;background:#fff;border:1px solid #d8dee4;padding:24px;}",
        "h1{font-size:24px;margin:0 0 12px;}h2{font-size:18px;margin:24px 0 10px;border-bottom:1px solid #d8dee4;padding-bottom:6px;}",
        ".meta{color:#57606a;margin:0 0 18px;}.summary{font-size:16px;background:#f1f8ff;border-left:4px solid #0969da;padding:12px;margin:0;}",
        "table{border-collapse:collapse;width:100%;margin:8px 0 16px;font-size:14px;}th,td{border:1px solid #d8dee4;padding:8px 10px;text-align:left;}",
        "th{background:#f6f8fa;}td.num{text-align:right;}ul{margin-top:6px;padding-left:22px;}",
        "</style></head><body><div class=\"wrap\">",
        "<h1>A股赚钱效应与情绪日报</h1>",
        "<p class=\"meta\">交易日：{0}<br>上一交易日：{1}<br>数据源：Tushare daily / daily_basic / limit_list_d / kpl_concept_cons / trade_cal</p>".format(
            html_text(yyyymmdd_to_display(trade_date)),
            html_text(yyyymmdd_to_display(prev_trade_date)),
        ),
        "<h2>一句话结论</h2>",
        "<p class=\"summary\">赚钱效应：<strong>{0}</strong>（评分 {1:.1f}/100）；短线情绪：<strong>{2}</strong>。</p>".format(
            html_text(money["grade"]),
            money["score"],
            html_text(sentiment["stage"]),
        ),
        "<h2>赚钱效应</h2>",
        html_table(["指标", "数值"], money_rows),
        "<h2>短线情绪</h2>",
        html_table(["指标", "数值"], sentiment_rows),
    ]

    if sentiment["reasons"]:
        sections.append("<p>判断依据：</p>")
        sections.append(html_list(sentiment["reasons"]))

    if limit_concepts:
        rows = []
        for item in limit_concepts:
            rows.append((item.get("concept_name"), item.get("limit_up_count")))
        sections.append("<h2>涨停题材粗略统计</h2>")
        sections.append(html_table(["题材", "涨停数"], rows))

    if top_concepts:
        rows = []
        for item in top_concepts:
            rows.append(
                (
                    item.get("concept_name"),
                    int(item.get("stock_count", 0)),
                    pct_text(item.get("avg_pct")),
                    pct_text(item.get("up_ratio", 0) * 100),
                    int(item.get("up3_count", 0)),
                )
            )
        sections.append("<h2>题材强弱粗略统计</h2>")
        sections.append(html_table(["题材", "样本数", "平均涨跌幅", "上涨占比", ">=3%家数"], rows))

    sections.append("<h2>口径说明</h2>")
    sections.append(
        html_list(
            [
                "样本：Tushare stock_basic 中已上市 A 股，合并当日 daily 有交易数据的股票。",
                "题材统计使用 Tushare kpl_concept_cons 开盘啦题材成分，同一股票可归属多个题材。",
                "赚钱效应评分是本脚本的经验规则，主要由上涨占比、中位涨跌幅、涨停跌停差、强弱股数量、昨日涨停反馈和炸板/开板风险构成。",
                "若 limit_list_d 未提供真实炸板行，脚本会使用涨停股 open_times > 0 的比例作为开板风险近似值。",
                "赚钱效应等级分为强 / 中 / 弱；情绪阶段分为沸点 / 过热 / 微热 / 微冷 / 过冷 / 冰点，用于日报辅助判断，不等同于交易建议。",
            ]
        )
    )
    if raw_paths:
        sections.append("<h2>输出文件</h2>")
        sections.append(html_table(["名称", "路径"], list(raw_paths.items()), numeric_from=99))

    sections.append("</div></body></html>")
    return "\n".join(sections)


def main():
    args = parse_args()
    ensure_email_config()
    ensure_dir(args.output_dir)
    ensure_dir(args.cache_dir)

    pro = get_pro(args.token)
    target_date = normalize_date(args.trade_date)
    trade_date = get_latest_trade_date(pro, target_date)
    prev_trade_date = get_previous_trade_date(pro, trade_date)

    stock_basic = load_stock_basic(pro, args.cache_dir, args.refresh_basic)
    daily = fetch_daily(pro, trade_date)
    daily_basic = fetch_daily_basic(pro, trade_date)
    if daily_basic is not None and not daily_basic.empty:
        daily = daily.merge(
            daily_basic.drop(columns=["trade_date"], errors="ignore"),
            on="ts_code",
            how="left",
        )
    daily = filter_a_share_traded(daily, stock_basic)

    today_limit = enrich_limit_list(fetch_limit_list(pro, trade_date))
    prev_limit = enrich_limit_list(fetch_limit_list(pro, prev_trade_date))
    kpl_concept_cons = fetch_kpl_concept_cons(pro, trade_date)
    kpl_concepts = prepare_kpl_concepts(kpl_concept_cons)
    prev_limit_up = (
        prev_limit[prev_limit["limit_side"] == "up"].copy()
        if prev_limit is not None and not prev_limit.empty
        else pd.DataFrame()
    )

    money = calc_money_effect(daily, today_limit, prev_limit_up, trade_date)
    sentiment = calc_sentiment(today_limit, prev_limit, money)
    limit_concepts = aggregate_limit_concepts(today_limit, kpl_concepts)
    top_concepts = top_movers_by_concept(daily, kpl_concepts)

    stamp = trade_date
    raw_paths = {}
    if args.save_raw:
        daily_path = os.path.join(args.output_dir, "daily_merged_{0}.csv".format(stamp))
        limit_path = os.path.join(args.output_dir, "limit_list_d_{0}.csv".format(stamp))
        kpl_concept_path = os.path.join(args.output_dir, "kpl_concept_cons_{0}.csv".format(stamp))
        daily.to_csv(daily_path, index=False, encoding="utf-8-sig")
        if today_limit is not None and not today_limit.empty:
            today_limit.to_csv(limit_path, index=False, encoding="utf-8-sig")
            raw_paths["涨跌停原始表"] = limit_path
        if kpl_concept_cons is not None and not kpl_concept_cons.empty:
            kpl_concept_cons.to_csv(kpl_concept_path, index=False, encoding="utf-8-sig")
            raw_paths["开盘啦题材成分表"] = kpl_concept_path
        raw_paths["日行情合并表"] = daily_path

    result = {
        "trade_date": trade_date,
        "prev_trade_date": prev_trade_date,
        "money_effect": money,
        "sentiment": sentiment,
        "limit_concepts": limit_concepts,
        "top_concepts": top_concepts,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    json_path = os.path.join(args.output_dir, "market_mood_{0}.json".format(stamp))
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    raw_paths["JSON"] = json_path

    email_subject = args.email_subject or make_email_subject(trade_date, money, sentiment)
    email_html = make_email_html(trade_date, prev_trade_date, money, sentiment, top_concepts, limit_concepts, raw_paths)
    try:
        send_report_email(email_html, email_subject)
    except Exception as exc:
        fail("Send email failed: {0}".format(exc))
    print("Sent report email: {0}".format(email_subject))
    print("Saved JSON: {0}".format(json_path))


if __name__ == "__main__":
    main()
