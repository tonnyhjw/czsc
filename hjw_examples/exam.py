import pprint

from src.stock_process import *
from src.decorate import *


def play_day_trend_reverse():
    row = dict(ts_code="600171.sh", symbol="600171", name="上海贝岭", industry="半导体")
    sdt, edt = "20180501", "20240708"
    result = trend_reverse_ubi_entry(row=row, sdt=sdt, edt=edt, freq="D", fx_dt_limit=5)
    pprint.pprint(result)


def play_pzbc():
    row = dict(ts_code="600171.sh", symbol="600171", name="上海贝岭", industry="半导体")
    sdt, edt = "20200101", "20240223"
    result = bottom_pzbc(row, sdt, edt, "W", fx_dt_limit=30)
    pprint.pprint(result)


@timer
def xd_dev():
    from src.xd.analyze_by_break import analyze_xd
    from src.sig_xd import get_xd_zs_seq
    row = dict(ts_code="300510.SZ", symbol="300510", name="金冠股份", industry="电气设备")
    sdt, edt = "20180501", "20240712"
    c = row_2_czsc(row, sdt, edt, "D")
    xds = analyze_xd(c.bi_list)
    zs_seq = get_xd_zs_seq(xds)
    for zs in zs_seq[-3:]:
        print(len(zs.xds))
        pprint.pprint(zs)
        pprint.pp(zs.xds)
    print(zs_seq[-1].is_valid)


@timer
def ma_pzbc_dev():
    row = dict(ts_code="836504.BJ", symbol="836504", name="博迅生物", industry="医疗保健")
    sdt, edt = "20180501", "20240524"
    ma_pzbc(row, sdt, edt, "D", 5)
    return


if __name__ == '__main__':
    # play_day_trend_reverse()
    # play_pzbc()
    # result = run_single_stock_backtest(ts_code='000415.SZ', edt='20240614', freq="D")
    # pprint.pprint(result.get("sharpe_ratio"))
    # xd_dev()
    ma_pzbc_dev()
