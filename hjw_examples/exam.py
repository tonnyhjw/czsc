import pprint

from src.stock_process import *


def play_day_trend_reverse():
    row = dict(ts_code="603713.sh", symbol="603713", name="密尔克卫", industry="仓储物流")
    sdt, edt = "20180101", "20240429"
    result = trend_reverse_ubi_entry(row=row, sdt=sdt, edt=edt, freq="D", fx_dt_limit=5)
    pprint.pprint(result)


def play_pzbc():
    row = dict(ts_code="300956.sz", symbol="300956", name="英力股份", industry="光伏")
    sdt, edt = "20180101", "20240223"
    result = bottom_pzbc(row, sdt, edt)
    pprint.pprint(result)


if __name__ == '__main__':
    play_day_trend_reverse()
    # play_pzbc()
