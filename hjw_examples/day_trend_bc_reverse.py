import os
import sys
import datetime
import traceback
import pandas as pd


sys.path.insert(0, '.')
sys.path.insert(0, '..')
from flask import Flask, render_template
from czsc import CZSC, home_path, empty_cache_path, RawBar
from czsc.data import TsDataCache
from hjw_examples.sig import trend_reverse_ubi

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
    new_record = {'ts_code': ts_code, 'date': today}
    history = history.append(new_record, ignore_index=True)
    history.to_csv(history_file)
    return history


def check(write_file: str, history_file: str):
    global idx
    idx += 1
    dc = TsDataCache(home_path)
    stock_basic = dc.stock_basic()
    history = read_history(history_file)

    with open(write_file, 'w') as f:
        for index, row in stock_basic.iterrows():
            _ts_code = row.get('ts_code')
            _symbol = row.get('symbol')
            _name = row.get('name')
            _hs = _ts_code.split(".")[-1]

            # 检查股票是否在最近一个月选中过
            if not history[
                (history['ts_code'] == _ts_code) &
                (history['date'] > (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d'))
            ].empty:
                continue

            try:
                bars = dc.pro_bar(_ts_code, start_date="20180101", freq='D', asset="E", adj='qfq', raw_bar=True)
                c = CZSC(bars)
                _signals = trend_reverse_ubi(c)

                for s_value in _signals.values():
                    if "多头" in s_value:
                        _stock_output = f"{_symbol} {_name} {_signals}, https://xueqiu.com/S/{_hs}{_symbol}"
                        f.write(f"{_stock_output}\n")
                        history = update_history(history, _ts_code, history_file)
            except Exception as e_msg:
                print(f"{_ts_code} {_name}出现报错，{e_msg}")
                traceback.print_exc()


if __name__ == '__main__':
    script_name = os.path.basename(__file__)
    output_name = f"statics/{script_name}_{datetime.datetime.today().strftime('%Y-%m-%d')}.txt"
    history_file = f"statics/{script_name}.csv"
    check(output_name, history_file)
