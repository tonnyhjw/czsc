import pprint

from src.stock_process import *


def play_day_trend_reverse():
    row = dict(ts_code="002455.sz", symbol="002455", name="百川股份", industry="化工原料")
    sdt, edt = "20180101", "20240219"
    result = trend_reverse_ubi_entry(row=row, sdt=sdt, edt=edt, freq="D", fx_dt_limit=5)
    pprint.pprint(result)


def play_pzbc():
    row = dict(ts_code="300069.sz", symbol="300069", name="金利华电", industry="金利华电")
    sdt, edt = "20180101", "20240301"
    result = bottom_pzbc(row, sdt, edt)
    pprint.pprint(result)


if __name__ == '__main__':
    # fx_reliability_exam("20180501", "20240329")
    play_pzbc()
