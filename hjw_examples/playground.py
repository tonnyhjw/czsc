import datetime

from czsc.data import TsDataCache
from czsc import home_path
from czsc.signals.tas import update_macd_cache
from czsc.analyze import CZSC
from hjw_examples.stock_process import trend_reverse_ubi_entry


def play():
    row = dict(ts_code="300122.sz", symbol="300122", name="智飞生物")
    sdt, edt = "20200101", "20240416"
    result = trend_reverse_ubi_entry(row=row, sdt=sdt, edt=edt)
    print(result)


if __name__ == '__main__':
    play()
