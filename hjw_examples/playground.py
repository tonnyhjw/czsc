import datetime

from czsc.data import TsDataCache
from czsc import home_path
from czsc.signals.tas import update_macd_cache
from czsc.analyze import CZSC
from hjw_examples.day_trend_bc_reverse import process_stock


def play():
    row = dict(ts_code="300012.sz", symbol="300012", name="华测检测")
    sdt, edt = "20200101", "20231214"
    result = process_stock(row=row, sdt=sdt, edt=edt)
    print(result)


if __name__ == '__main__':
    play()
