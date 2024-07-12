import pprint

from src.stock_process import *
from src.decorate import *


def play_day_trend_reverse():
    row = dict(ts_code="600076.sh", symbol="600076", name="康欣新材", industry="广告包装")
    sdt, edt = "20200101", "20240626"
    result = trend_reverse_ubi_entry(row=row, sdt=sdt, edt=edt, freq="D", fx_dt_limit=5)
    pprint.pprint(result)


def play_pzbc():
    row = dict(ts_code="600171.sh", symbol="600171", name="上海贝岭", industry="半导体")
    sdt, edt = "20200101", "20240223"
    result = bottom_pzbc(row, sdt, edt, "W", fx_dt_limit=30)
    pprint.pprint(result)


@timer
def xd_dev():
    from src.xd.analyze import create_xd
    row = dict(ts_code="002180.sz", symbol="002180", name="纳思达", industry="IT设备")
    sdt, edt = "20200101", "20240429"
    c = row_2_czsc(row, sdt, edt, "D")
    xd_fxs = create_xd(c.bi_list)
    for fx in xd_fxs:
        print(fx.mark)


if __name__ == '__main__':
    # play_day_trend_reverse()
    # play_pzbc()
    # result = run_single_stock_backtest(ts_code='000415.SZ', edt='20240614', freq="D")
    # pprint.pprint(result.get("sharpe_ratio"))
    xd_dev()
