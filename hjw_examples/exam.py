import datetime
import pprint

from czsc.data import TsDataCache
from czsc import home_path
from czsc.signals.tas import update_macd_cache
from czsc.analyze import CZSC
from hjw_examples.stock_process import *


def play_day_trend_reverse():
    row = dict(ts_code="002455.sz", symbol="002455", name="百川股份", industry="化工原料")
    sdt, edt = "20180101", "20240219"
    result = trend_reverse_ubi_entry(row=row, sdt=sdt, edt=edt, freq="D", fx_dt_limit=5)
    pprint.pprint(result)


def fx_reliability_exam(sdt, edt):
    import concurrent
    from concurrent.futures import ProcessPoolExecutor
    from hjw_examples.history import read_history

    history_csv = f"statics/history/day_trend_bc_reverse.csv"
    history = read_history(history_csv)
    stock_basic = TsDataCache(home_path).stock_basic()  # 只用于读取股票基础信息
    results = []  # 用于存储所有股票的结果

    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = {}
        for index, row in stock_basic.iterrows():
            _ts_code = row.get('ts_code')
            future = executor.submit(trend_reverse_ubi_entry, row, sdt, edt, 'D', 5)
            futures[future] = _ts_code  # 保存future和ts_code的映射

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

    for _stock in results:
        if not history[(history['ts_code'] == _stock.get('ts_code'))].empty:
            print(f"{row.get('name')} {_ts_code}，出现过日线买点")
        else:
            print("没有出现买点")


def play_pzbc():
    row = dict(ts_code="600101.sh", symbol="600101", name="明星电力", industry="电力")
    sdt, edt = "20180101", "20240223"
    result = bottom_pzbc(row, sdt, edt)
    pprint.pprint(result)


if __name__ == '__main__':
    # fx_reliability_exam("20180501", "20240329")
    play_pzbc()
