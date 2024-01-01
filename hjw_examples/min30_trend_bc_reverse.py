import os
import sys
import datetime
import traceback


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


def check(write_file: str):
    global idx
    idx += 1
    dc = TsDataCache(home_path)
    stock_basic = dc.stock_basic()

    with open(write_file, 'w') as f:

        for index, row in stock_basic.iterrows():
            _symbol = row.get('symbol')
            _ts_code = row.get('ts_code')
            _name = row.get('name')
            _hs = _ts_code.split(".")[-1]
            try:
                bars = dc.pro_bar(_ts_code, start_date="20231001", freq='30min', asset="E", adj='qfq', raw_bar=True)
                # print(f"{len(_bars)} {_bars[-1]} {dir(_bars[-1])}")
                c = CZSC(bars)
                _signals = trend_reverse_ubi(c)
                for s_value in _signals.values():
                    if "多头" in s_value:
                        _stock_output = f"{_symbol} {_name} {_signals}, https://xueqiu.com/S/{_hs}{_symbol}"
                        # print(_stock_output)
                        f.write(f"{_stock_output}\n")
            except Exception as e_msg:
                print(f"{_ts_code} {_name}出现报错，{e_msg}")
                traceback.print_exc()


if __name__ == '__main__':
    script_name = os.path.basename(__file__)
    output_name = f"statics/{script_name}_{datetime.datetime.today().strftime('%Y-%m-%d')}.txt"
    # if not os.path.exists(output_name):
    #     empty_cache_path()
    check(output_name)
