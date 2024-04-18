import datetime

from czsc.data import TsDataCache
from czsc import home_path
from czsc.signals.tas import update_macd_cache
from czsc.analyze import CZSC
from hjw_examples.stock_process import *


def play():
    row = dict(ts_code="300122.sz", symbol="300122", name="智飞生物")
    sdt, edt = "20200101", "20240416"
    result = trend_reverse_ubi_entry(row=row, sdt=sdt, edt=edt)
    print(result)


def fx_reliability_exam():
    stock_basic = TsDataCache(home_path).stock_basic()  # 只用于读取股票基础信息
    for index, row in stock_basic.iterrows():
        _ts_code = row.get('ts_code')
        bot_fx_detect(row, "20210501", "20240501")


if __name__ == '__main__':
    fx_reliability_exam()
