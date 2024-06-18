# all_stocks_backtest.py

import os
from datetime import datetime
from src.backtrade import run_single_stock_backtest
from czsc.data import TsDataCache
from pprint import pprint


def run_all_stocks_backtest(stocks, edt: str = datetime.now().strftime('%Y%m%d'), freq="D"):
    all_trade_analyzers = []
    all_sharpe_ratios = []

    for ts_code in stocks:
        print(f'Running backtest for {ts_code}')
        trade_analyzer, sharpe_ratio = run_single_stock_backtest(ts_code, edt, freq)
        if trade_analyzer and sharpe_ratio:
            all_trade_analyzers.append(trade_analyzer)
            all_sharpe_ratios.append(sharpe_ratio)

    # 汇总分析结果
    combined_trade_analyzer = combine_trade_analyzers(all_trade_analyzers)
    combined_sharpe_ratio = combine_sharpe_ratios(all_sharpe_ratios)

    # 打印汇总分析结果
    print('Combined Trade Analysis Results:')
    pprint(combined_trade_analyzer)

    print('Combined Sharpe Ratio Analysis:')
    pprint(combined_sharpe_ratio)


def combine_trade_analyzers(analyzers):
    combined = {}
    for analyzer in analyzers:
        for key, value in analyzer.items():
            if key not in combined:
                combined[key] = value
            else:
                combined[key] += value
    return combined


def combine_sharpe_ratios(ratios):
    combined = {}
    for ratio in ratios:
        for key, value in ratio.items():
            if key not in combined:
                combined[key] = value
            else:
                combined[key] = (combined[key] + value) / 2  # 平均值
    return combined


if __name__ == '__main__':
    stocks = ['000001.SZ', '000002.SZ', '000003.SZ']  # 示例个股列表
    run_all_stocks_backtest(stocks)
