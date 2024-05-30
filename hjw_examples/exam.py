import pprint

from src.stock_process import *


def play_day_trend_reverse():
    row = dict(ts_code="601009.sh", symbol="601009", name="南京银行", industry="银行")
    sdt, edt = "20180101", "20240322"
    result = trend_reverse_ubi_entry(row=row, sdt=sdt, edt=edt, freq="D", fx_dt_limit=5)
    pprint.pprint(result)


def play_pzbc():
    row = dict(ts_code="300069.sz", symbol="300069", name="金利华电", industry="金利华电")
    sdt, edt = "20180101", "20240301"
    result = bottom_pzbc(row, sdt, edt)
    pprint.pprint(result)


if __name__ == '__main__':
    play_day_trend_reverse()
    # play_pzbc()
