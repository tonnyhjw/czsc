import pprint

from src.stock_process import *
from src.backtrade import run_single_stock_backtest


def play_day_trend_reverse():
    row = dict(ts_code="600171.sh", symbol="600171", name="上海贝岭", industry="半导体")
    sdt, edt = "20200101", "20240424"
    result = trend_reverse_ubi_entry(row=row, sdt=sdt, edt=edt, freq="D", fx_dt_limit=5)
    pprint.pprint(result)


def play_pzbc():
    row = dict(ts_code="600171.sh", symbol="600171", name="上海贝岭", industry="半导体")
    sdt, edt = "20200101", "20240223"
    result = bottom_pzbc(row, sdt, edt, "W", fx_dt_limit=30)
    pprint.pprint(result)


def view_fxs():
    row = dict(ts_code="002180.sz", symbol="002180", name="纳思达", industry="IT设备")
    sdt, edt = "20240101", "20240429"
    c = row_2_czsc(row, sdt, edt, "D")
    pprint.pprint(c.bi_list)
    last_bi = c.bi_list[-1]
    pprint.pp(last_bi.fxs)
    print(last_bi.fx_b.power_str)


if __name__ == '__main__':
    play_day_trend_reverse()
    # play_pzbc()
    # result = run_single_stock_backtest(ts_code='000415.SZ', edt='20240614', freq="D")
    # pprint.pprint(result.get("sharpe_ratio"))
    # view_fxs()
