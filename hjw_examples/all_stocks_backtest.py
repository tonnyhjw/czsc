# all_stocks_backtest.py

import os
import traceback
import concurrent
from pprint import pprint
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

from czsc import home_path
from czsc.data import TsDataCache
from src.notify import notify_buy_backtrader
from src.backtrade.utils import *
from src.backtrade.run import run_single_stock_backtest


def run_all_stocks_backtest(stock, edt: str = datetime.now().strftime('%Y%m%d'),
                            fx_pwr="弱", signals="二买", freq="D", db="BI"):
    all_trade_analyzers = []
    all_sharpe_ratios = []
    all_trade_details = []

    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = {}

        for index, row in stock.iterrows():
            ts_code = row.get('ts_code')
            print(f'Running backtest for {ts_code}')
            future = executor.submit(run_single_stock_backtest, ts_code, edt, fx_pwr, signals, freq, db)
            futures[future] = ts_code  # 保存future和ts_code的映射

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            trade_analyzer = result.get("trade_analyzer")
            sharpe_ratio = result.get("sharpe_ratio")
            trade_detail = result.get("trade_detail")
            if trade_analyzer and sharpe_ratio:
                all_trade_analyzers.append(trade_analyzer)
                all_sharpe_ratios.append(sharpe_ratio)
                all_trade_details.append(trade_detail)

    # 汇总分析结果
    combined_trade_analyzer = combine_trade_analyzers(all_trade_analyzers)
    combined_sharpe_ratio = combine_sharpe_ratios(all_sharpe_ratios)

    email_subject = f"[测试][回测]{fx_pwr}{signals}测试结果"
    notify_buy_backtrader(combined_trade_analyzer, combined_sharpe_ratio, all_trade_details, email_subject)


def generate_email_body(analysis):
    # 从分析结果中提取数据
    gross_pnl = analysis['pnl']['gross']['total']
    net_pnl = analysis['pnl']['net']['total']
    win_ratio = analysis['won']['total']
    loss_ratio = analysis['lost']['total']

    # 格式化电子邮件正文
    email_body = f"""                        
        总体盈亏金额:
        总盈利金额: ¥{gross_pnl:.2f}
        总净利金额: ¥{net_pnl:.2f}
        
        总体盈亏比例:
        盈利笔数占比: {win_ratio}%
        亏损笔数占比: {loss_ratio}%
        
        """
    return email_body


if __name__ == '__main__':
    stock_basic = TsDataCache(home_path).stock_basic()  # 只用于读取股票基础信息
    # stock_basic = stock_basic.head(100)
    FX_PWR = ["强", "中", "弱"]
    SIGNALS = ["一买", "二买", "三买"]
    for _signals in SIGNALS:
        for _fx_pwr in FX_PWR:
            run_all_stocks_backtest(stock_basic, fx_pwr=_fx_pwr, signals=_signals, db="BI")

