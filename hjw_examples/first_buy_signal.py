import os
import sys
import datetime

sys.path.insert(0, '.')
sys.path.insert(0, '..')
from flask import Flask, render_template
from czsc import CZSC, home_path, empty_cache_path, RawBar
from czsc.data import TsDataCache
from czsc.utils import sig as sig_utils
from tas_macd_bc_V230804 import tas_macd_bc_V230804
from czsc.signals import cxt_third_bs_V230319

idx = 1000

# ts_code      000001.SZ
# symbol          000001
# name              平安银行
# area                深圳
# industry            银行
# list_date     19910403
# Name: 0, dtype: object


def check_3buy_day_1buy_week(write_file: str):
    global idx
    idx += 1
    dc = TsDataCache(home_path)
    stock_basic = dc.stock_basic()

    with open(write_file, 'w') as f:

        for index, row in stock_basic.iterrows():
            try:
                _bc_output = is_valid_entry(row, dc, "20220401")
                if _bc_output is not None:
                    f.write(f"{_bc_output}\n")
            except Exception as e_msg:
                print(f"{row.get('ts_code')} {row.get('name')}出现报错，{e_msg}")


def is_valid_entry(row: dict, dc: TsDataCache, start_date: str):
    _symbol = row.get('symbol')
    _ts_code = row.get('ts_code')
    _name = row.get('name')
    _hs = _ts_code.split(".")[-1]

    freq_d_bars = dc.pro_bar(_ts_code, start_date=start_date, freq='D', asset="E", adj='qfq', raw_bar=True)
    freq_w_bars = dc.pro_bar(_ts_code, start_date=start_date, freq='W', asset="E", adj='qfq', raw_bar=True)

    c_day = CZSC(freq_d_bars)
    c_week = CZSC(freq_w_bars)

    week_signals = tas_macd_bc_V230804(c_week)
    day_signals = cxt_third_bs_V230319(c_day)

    for week_value in week_signals.values():
        if "多头" in week_value:
            for day_value in day_signals.values():
                if "三买" in day_value:
                    return f"{_symbol} {_name} , https://xueqiu.com/S/{_hs}{_symbol}"
    return


def demo_one(_ts_code):
    dc = TsDataCache(home_path)
    row = dict(ts_code=_ts_code)
    print(is_valid_entry(row, dc, "20220401"))


def _break_out_threshold(stock_data, last_n_days: int, threshold: float):
    # 计算最后N个K线数据的价格变化百分比，与阈值比较
    # 使用示例
    # stock_data 是一个包含有序字典的列表，每个字典包含某天的收盘价格(close)
    # N 是你想检查的最后N个交易日的数量
    # threshold 是你设定的阈值，如0.05代表5%

    # result = check_threshold(stock_data, N, threshold)
    # print(result)
    for i in range(-last_n_days, 0):
        if i == -len(stock_data):  # 若检查的是第一天的数据，则没有前一天的数据，跳过
            continue
        change = (stock_data[i].close - stock_data[i-1].close) / stock_data[i-1].close
        if abs(change) > threshold:
            return True
    return False


def stock_amount_below_limit(bars: RawBar, days, threshold=100000000):
    """
    分析股票最近N个交易日成交额大于m的股票
    bars: 股票K线数据
    days: 最近交易日数量
    threshold: 成交额阈值
    """

    # 获取最近N个交易日
    recent_bars = bars[-days:]

    # 过滤成交额
    for bar in recent_bars:
        if bar.amount < threshold:
            return True
    return False


if __name__ == '__main__':
    output_name = f"statics/first_buy_result_{datetime.datetime.today().strftime('%Y-%m-%d')}.txt"
    if not os.path.exists(output_name):
        empty_cache_path()
    check_3buy_day_1buy_week(output_name)
