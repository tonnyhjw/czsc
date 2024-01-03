import os
import sys
import datetime
import pandas as pd
import concurrent
from concurrent.futures import ProcessPoolExecutor


sys.path.insert(0, '.')
sys.path.insert(0, '..')
from flask import Flask, render_template
from czsc import CZSC, home_path, empty_cache_path, RawBar
from czsc.data import TsDataCache
from hjw_examples.sig import trend_reverse_ubi
from hjw_examples.notify import send_email
from hjw_examples.templates.email_templates import daily_email_style

idx = 1000

# ts_code      000001.SZ
# symbol          000001
# name              平安银行
# area                深圳
# industry            银行
# list_date     19910403
# Name: 0, dtype: object


def read_history(history_file):
    try:
        one_month_ago = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
        history = pd.read_csv(history_file, parse_dates=['date'])
        # 仅保留最近一个月的记录
        history = history[history['date'] >= one_month_ago]
    except FileNotFoundError:
        history = pd.DataFrame(columns=['ts_code', 'date'])
    return history


def update_history(history, ts_code, history_file):
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    new_record = pd.DataFrame({'ts_code': [ts_code], 'date': [today]})

    # 使用 concat 替代 append
    history = pd.concat([history, new_record], ignore_index=True)

    # 保留最近一个月的记录
    one_month_ago = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
    history = history[history['date'] >= one_month_ago]

    history.to_csv(history_file, index=False)
    return history


def process_stock(row, sdt, edt):
    dc = TsDataCache(home_path)  # 在每个进程中创建独立的实例
    _ts_code = row.get('ts_code')
    _symbol = row.get('symbol')
    _name = row.get('name')
    _hs = _ts_code.split(".")[-1]

    output = {}
    try:
        bars = dc.pro_bar(_ts_code, start_date=sdt, freq='D', asset="E", adj='qfq', raw_bar=True)
        assert "ST" not in _name, "排除ST股票"
        c = CZSC(bars)
        _signals = trend_reverse_ubi(c)

        for s_value in _signals.values():
            if "多头" in s_value:
                symbol_link = f'<a href="https://xueqiu.com/S/{_hs}{_symbol}">{_symbol}</a>'
                output = {
                    'name': _name,
                    'symbol': symbol_link,
                    'ts_code': _ts_code,
                    'signals': s_value.split("_")[1]
                }
    except Exception as e_msg:
        print(f"{_ts_code} {_name}出现报错，{e_msg}")
    finally:
        return output


def check(history_file: str):
    stock_basic = TsDataCache(home_path).stock_basic()  # 只用于读取股票基础信息
    history = read_history(history_file)
    results = []  # 用于存储所有股票的结果

    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = {}
        for index, row in stock_basic.iterrows():
            _ts_code = row.get('ts_code')
            if not history[
                (history['ts_code'] == _ts_code) & (
                        history['date'] > (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d'))
            ].empty:
                continue
            future = executor.submit(process_stock, row, "20200101", datetime.datetime.now().strftime('%Y%m%d'))
            futures[future] = _ts_code  # 保存future和ts_code的映射

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                results.append(result)
                history = update_history(history, result['ts_code'], history_file)

    # 将结果转换为 DataFrame
    df_results = pd.DataFrame(results)
    # 生成 HTML 表格
    html_table = df_results.to_html(classes='table table-striped table-hover', border=0, index=False, escape=False)
    styled_table = daily_email_style(html_table)

    # 发送电子邮件
    send_email(styled_table, "[自动盯盘]发现新个股买点")


if __name__ == '__main__':
    script_name = os.path.basename(__file__)
    output_name = f"statics/{script_name}_{datetime.datetime.today().strftime('%Y-%m-%d')}.txt"
    history_csv = f"statics/history/{script_name}.csv"
    check(history_csv)
