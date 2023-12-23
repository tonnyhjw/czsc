# -*- coding: utf-8 -*-
"""
author: zengbin93
email: zeng_bin8888@163.com
create_dt: 2022/7/12 14:22
describe: CZSC 逐K线播放
https://pyecharts.org/#/zh-cn/web_flask
"""
import pprint
import sys

sys.path.insert(0, '.')
sys.path.insert(0, '..')
from czsc import CZSC, home_path
from czsc.data import TsDataCache
from czsc.utils import sig
from czsc.signals.hjw_cxt import pzbc_macd_bc_V231221


dc = TsDataCache(home_path)
bars = dc.pro_bar('000001.SH', start_date="20220101", end_date="20231230", freq='W', asset="I", adj='qfq', raw_bar=True)
idx = 1000


def demo():
    global idx
    print(bars[-1].dt)

    bi_list = CZSC(bars).bi_list
    zs_seq = sig.get_zs_seq(bi_list)
    pprint.pp(bi_list)
    pprint.pp(zs_seq)


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
                bars = dc.pro_bar(_ts_code, start_date="20220401", freq='D', asset="E", adj='qfq', raw_bar=True)
                _bars = bars[:idx]

                # print(f"{len(_bars)} {_bars[-1]} {dir(_bars[-1])}")
                c = CZSC(_bars)
                _signals = cxt_third_bs_V230319(c, ma_type="SMA", timeperiod=5)
                for s_value in _signals.values():
                    if "三买" in s_value:
                        _stock_output = f"{_symbol} {_name} {_signals}, https://xueqiu.com/S/{_hs}{_symbol}"
                        # print(_stock_output)
                        if _break_out_threshold(_bars, 5, 0.07):
                            print(f"{_symbol} {_name} 出现三买但已突破, https://xueqiu.com/S/{_hs}{_symbol}")
                            continue
                        else:
                            f.write(f"{_stock_output}\n")
            except Exception as e_msg:
                print(f"{_ts_code} {_name}出现报错，{e_msg}")


if __name__ == "__main__":
    demo()
