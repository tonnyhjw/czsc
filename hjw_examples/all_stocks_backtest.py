# all_stocks_backtest.py

import os
import traceback
import concurrent
import backtrader as bt
from pprint import pprint
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

from czsc import home_path
from czsc.data import TsDataCache
from src.notify import notify_buy_backtrader
from src.backtrade import run_single_stock_backtest


def run_all_stocks_backtest(stock, edt: str = datetime.now().strftime('%Y%m%d'), freq="D"):
    all_trade_analyzers = []
    all_sharpe_ratios = []

    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = {}

        for index, row in stock.iterrows():
            ts_code = row.get('ts_code')
            print(f'Running backtest for {ts_code}')
            future = executor.submit(run_single_stock_backtest, ts_code, edt, freq)
            futures[future] = ts_code  # 保存future和ts_code的映射

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            trade_analyzer, sharpe_ratio = result.get("trade_analyzer"), result.get("sharpe_ratio")
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
    email_subject = f"[测试][回测]强、中、弱三买测试结果"
    notify_buy_backtrader(combined_trade_analyzer, combined_sharpe_ratio, email_subject)


def combine_trade_analyzers(analyzers):
    combined = bt.AutoOrderedDict()
    for analyzer_data in analyzers:
        if not analyzer_data:
            continue
        for key, value in analyzer_data.items():
            if key in combined:
                combined[key] = combine_dicts(combined[key], value)
            else:
                combined[key] = value
    return combined


def combine_dicts(dict1, dict2):
    combined = dict1.copy()
    for key, value in dict2.items():
        if key in combined:
            if isinstance(value, bt.AutoOrderedDict):
                combined[key] = combine_dicts(combined[key], value)
            else:
                if isinstance(combined[key], bt.AutoOrderedDict):
                    combined[key] = add_dicts(combined[key], {key: value})
                else:
                    combined[key] += value
        else:
            combined[key] = value
    return combined


def add_dicts(dict1, dict2):
    combined = dict1.copy()
    for key, value in dict2.items():
        if key in combined:
            combined[key] += value
        else:
            combined[key] = value
    return combined


def combine_sharpe_ratios(ratios):
    combined = bt.AutoOrderedDict()
    for ratio in ratios:
        for key, value in ratio.items():
            if key not in combined:
                if value is not None:
                    combined[key] = value
            else:
                if value is not None and combined[key] is not None:
                    combined[key] = (combined[key] + value) / 2  # 计算平均值
    return combined


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
    run_all_stocks_backtest(stock_basic)

