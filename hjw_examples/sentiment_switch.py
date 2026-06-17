# -*- coding: utf-8 -*-
"""
情绪仓位开关（Tushare 可跑模板）
功能：
1) 读取指定交易日涨停/跌停/炸板数据（limit_list_d）
2) 计算情绪指标 -> 输出“仓位建议”（0/30/60/100）
3) 生成次日观察池（按连板强度/成交额/炸板过滤等简单规则）

依赖：
pip install tushare pandas numpy

使用：
export TUSHARE_TOKEN="你的token"
python sentiment_switch.py --date 20251223
"""

import os
import sys
import argparse
import datetime as dt

import numpy as np
import pandas as pd
import tushare as ts


# ----------------------------
# 基础：交易日工具
# ----------------------------
def _to_yyyymmdd(d) -> str:
    if isinstance(d, str):
        return d.replace("-", "")[:8]
    if isinstance(d, (dt.date, dt.datetime)):
        return d.strftime("%Y%m%d")
    raise ValueError("date must be str or datetime/date")


def get_recent_trade_dates(pro, end_date: str, n: int = 15, exchange: str = "SSE"):
    """
    获取 end_date 往前 n 个交易日（含 end_date 若为交易日）。
    """
    end_date = _to_yyyymmdd(end_date)
    # 预取足够多的日历天，防止节假日不够
    start_date = (dt.datetime.strptime(end_date, "%Y%m%d") - dt.timedelta(days=45)).strftime("%Y%m%d")

    cal = pro.trade_cal(exchange=exchange, start_date=start_date, end_date=end_date, is_open="1")
    if cal is None or cal.empty:
        raise RuntimeError("trade_cal 返回为空，检查 token 或接口权限/网络")

    dates = cal.sort_values("cal_date")["cal_date"].tolist()
    if len(dates) < n:
        raise RuntimeError(f"可用交易日不足：{len(dates)} < {n}，请扩大回溯范围")
    return dates[-n:]


# ----------------------------
# 数据：涨停/跌停/炸板
# ----------------------------
def fetch_limit_data(pro, trade_date: str):
    """
    拉取涨跌停&炸板数据
    Tushare 接口：limit_list_d
    说明：不同账户权限字段可能略有差异，下面用“安全取列”的方式兼容。
    """
    trade_date = _to_yyyymmdd(trade_date)
    df = pro.limit_list_d(trade_date=trade_date)
    if df is None:
        df = pd.DataFrame()
    return df


def safe_col(df: pd.DataFrame, col: str, default=None):
    return df[col] if col in df.columns else default


def split_limit_types(limit_df: pd.DataFrame):
    """
    经验做法：limit_list_d 通常包含：
    - U：涨停
    - D：跌停
    - Z：炸板/打开涨停（不同版本字段可能不同，可能通过 open_times>0 + 涨停类型推断）
    为了兼容：优先用 limit_type，如果没有就用 open_times 推断。
    """
    if limit_df is None or limit_df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df = limit_df.copy()

    if "limit_type" in df.columns:
        up = df[df["limit_type"].astype(str).str.upper().isin(["U", "UP"])].copy()
        down = df[df["limit_type"].astype(str).str.upper().isin(["D", "DOWN"])].copy()
        # 炸板：Z 或者（涨停池里 open_times>0）
        z = df[df["limit_type"].astype(str).str.upper().isin(["Z"])].copy()
        if "open_times" in df.columns and not up.empty:
            z2 = up[pd.to_numeric(up["open_times"], errors="coerce").fillna(0) > 0].copy()
            z = pd.concat([z, z2], ignore_index=True).drop_duplicates(subset=["ts_code"], keep="first")
        return up, down, z

    # 没有 limit_type：尽量推断
    # 假设：pct_chg>0 且出现过封板记录为涨停候选；pct_chg<0 为跌停候选
    pct = pd.to_numeric(safe_col(df, "pct_chg", pd.Series([np.nan]*len(df))), errors="coerce")
    open_times = pd.to_numeric(safe_col(df, "open_times", pd.Series([0]*len(df))), errors="coerce").fillna(0)

    up = df[pct > 0].copy()
    down = df[pct < 0].copy()
    z = up[open_times > 0].copy()
    return up, down, z


# ----------------------------
# 风控：ST过滤（可选）
# ----------------------------
def fetch_st_set(pro, trade_date: str):
    """
    用 stock_st 做ST过滤（如果你权限不足/接口失败，自动降级为空集合）。
    """
    try:
        st_df = pro.stock_st(trade_date=_to_yyyymmdd(trade_date))
        if st_df is None or st_df.empty:
            return set()
        col = "ts_code" if "ts_code" in st_df.columns else None
        return set(st_df[col].astype(str).tolist()) if col else set()
    except Exception:
        return set()


# ----------------------------
# 连板强度（简易版）
# ----------------------------
def compute_streak(pro, codes, end_date: str, lookback_trade_days: int = 8):
    """
    简易连板：在最近 lookback_trade_days 个交易日内，连续出现在涨停列表中的天数。
    注意：这不是严格“封板连板”，但足够做观察池排序。
    """
    end_date = _to_yyyymmdd(end_date)
    dates = get_recent_trade_dates(pro, end_date=end_date, n=lookback_trade_days)
    codes = list(set([str(x) for x in codes]))

    # 初始化 streak
    streak = {c: 0 for c in codes}

    # 从 end_date 向前数：连续出现就加1，否则中断
    for d in reversed(dates):
        day_df = fetch_limit_data(pro, d)
        up, _, _ = split_limit_types(day_df)
        up_codes = set(up["ts_code"].astype(str).tolist()) if (up is not None and not up.empty and "ts_code" in up.columns) else set()
        for c in codes:
            if streak[c] >= 0:
                if c in up_codes:
                    streak[c] += 1
                else:
                    # 一旦断掉，标记为 -1 表示后面不再累计
                    streak[c] = -1

    # 把 -1 置为 0
    for c in list(streak.keys()):
        if streak[c] < 0:
            streak[c] = 0
    return streak


# ----------------------------
# 情绪指标 & 仓位开关
# ----------------------------
def calc_sentiment_metrics(limit_df: pd.DataFrame):
    up, down, z = split_limit_types(limit_df)

    up_cnt = len(up)
    down_cnt = len(down)
    z_cnt = len(z)

    # 炸板率：炸板 / (涨停 + 炸板) —— 防止除0
    denom = max(up_cnt + z_cnt, 1)
    z_rate = z_cnt / denom

    # 成交额（如果有）
    amount = safe_col(limit_df, "amount", None)
    if isinstance(amount, pd.Series):
        amount_sum = pd.to_numeric(amount, errors="coerce").fillna(0).sum()
    else:
        amount_sum = np.nan

    return {
        "up_cnt": int(up_cnt),
        "down_cnt": int(down_cnt),
        "z_cnt": int(z_cnt),
        "z_rate": float(z_rate),
        "amount_sum": float(amount_sum) if amount_sum == amount_sum else np.nan,
    }


def decide_position(metrics: dict):
    """
    纯规则：你可以替换为自己更成熟的“周期模型”。
    逻辑：
    - 核心：涨停多、跌停少、炸板率低 => 加仓
    - 一票否决：跌停爆炸 + 炸板高 => 直接 0
    输出：position(0/30/60/100) + 文案原因
    """
    up_cnt = metrics["up_cnt"]
    down_cnt = metrics["down_cnt"]
    z_rate = metrics["z_rate"]

    # 一票否决（可以按你的交易经验调）
    if down_cnt >= 40 and z_rate >= 0.45:
        return 0, "跌停多且炸板率高：情绪崩，禁止进攻"
    if down_cnt >= 60:
        return 0, "跌停极端：只防守或空仓"

    # 情绪分（0~100）
    # 你要“做大收益”：分数不是最关键，关键是阈值要让你敢满仓、也敢空仓
    score = 50.0
    score += min(up_cnt, 120) * 0.25          # 涨停越多越好
    score -= min(down_cnt, 120) * 0.55        # 跌停惩罚更重
    score -= z_rate * 40.0                    # 炸板率惩罚
    score = float(np.clip(score, 0, 100))

    # 分档仓位
    if score >= 70:
        return 100, f"情绪强（score={score:.1f}）：可主攻"
    if score >= 55:
        return 60, f"情绪偏强（score={score:.1f}）：进攻但控回撤"
    if score >= 42:
        return 30, f"情绪中性/偏弱（score={score:.1f}）：小仓试错"
    return 0, f"情绪弱（score={score:.1f}）：空仓/只做低风险套利"


# ----------------------------
# 观察池生成
# ----------------------------
def build_watchlist(pro, trade_date: str, limit_df: pd.DataFrame, topn: int = 30, exclude_st: bool = True):
    """
    观察池：从涨停/炸板里筛出“可能有次日博弈价值”的票
    默认规则（你后面可以按你的打法改）：
    - 只看涨停池（也可把炸板加入“备选”）
    - 排序：连板(简易) desc -> 成交额 desc
    - 过滤：ST
    """
    up, _, z = split_limit_types(limit_df)
    if up is None or up.empty:
        return pd.DataFrame()

    df = up.copy()
    # 基础字段
    for col in ["ts_code", "name", "reason_type", "amount", "pct_chg", "open_times", "first_time", "last_time"]:
        if col not in df.columns:
            df[col] = np.nan

    # ST过滤
    if exclude_st and "ts_code" in df.columns:
        st_set = fetch_st_set(pro, trade_date)
        if st_set:
            df = df[~df["ts_code"].astype(str).isin(st_set)].copy()

    # 成交额数值化
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

    # 连板（简易）
    codes = df["ts_code"].astype(str).tolist()
    streak = compute_streak(pro, codes=codes, end_date=trade_date, lookback_trade_days=8)
    df["streak"] = df["ts_code"].astype(str).map(streak).fillna(0).astype(int)

    # 炸板备选（可选）：把炸板强势票加入尾部
    # 这里留钩子：你想把“炸板回封”玩法加进来，就扩展这段
    # if z is not None and not z.empty: ...

    # 排序&截断
    df = df.sort_values(["streak", "amount"], ascending=[False, False]).head(topn)

    # 精简展示列
    out = df[["ts_code", "name", "streak", "amount", "reason_type", "open_times", "first_time", "last_time"]].copy()
    return out.reset_index(drop=True)


# ----------------------------
# 主程序
# ----------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="交易日 YYYYMMDD，如 20251223")
    parser.add_argument("--topn", type=int, default=30, help="观察池数量")
    parser.add_argument("--no_st_filter", action="store_true", help="不做ST过滤")
    args = parser.parse_args()

    token = os.getenv("TUSHARE_TOKEN", "").strip()
    if not token:
        print("ERROR: 未设置 TUSHARE_TOKEN 环境变量", file=sys.stderr)
        sys.exit(1)

    ts.set_token(token)
    pro = ts.pro_api()

    trade_date = _to_yyyymmdd(args.date)

    # 取数据
    limit_df = fetch_limit_data(pro, trade_date)
    if limit_df.empty:
        print(f"WARNING: {trade_date} limit_list_d 为空（可能不是交易日/接口权限不足/网络问题）")
        sys.exit(0)

    # 情绪指标
    metrics = calc_sentiment_metrics(limit_df)
    pos, reason = decide_position(metrics)

    # 输出情绪概览
    print("=" * 70)
    print(f"交易日：{trade_date}")
    print(f"涨停：{metrics['up_cnt']} | 跌停：{metrics['down_cnt']} | 炸板：{metrics['z_cnt']} | 炸板率：{metrics['z_rate']:.2%}")
    print(f"仓位建议：{pos}%  —— {reason}")
    print("=" * 70)

    # 观察池
    watch = build_watchlist(
        pro, trade_date, limit_df, topn=args.topn, exclude_st=(not args.no_st_filter)
    )

    if watch.empty:
        print("观察池：空（当天无涨停或被过滤）")
        return

    # 格式化成交额（万/亿）
    def fmt_amount(x):
        # Tushare amount 通常单位：千元(或万元)取决于接口；这里不强行假设单位，只做缩放展示
        # 你自己确认后可改成固定“亿元”
        if x >= 1e8:
            return f"{x/1e8:.2f}e8"
        if x >= 1e4:
            return f"{x/1e4:.2f}e4"
        return f"{x:.0f}"

    watch_show = watch.copy()
    watch_show["amount"] = watch_show["amount"].apply(fmt_amount)

    print("次日观察池（按连板强度/成交额排序）：")
    print(watch_show.to_string(index=False))

    print("\n提示：想真正“做大收益”，把你的交易规则固化成两段：")
    print("1) 情绪->仓位（今天能不能重仓）")
    print("2) 观察池->执行（哪些票允许打，触发条件是什么）")


if __name__ == "__main__":
    main()
