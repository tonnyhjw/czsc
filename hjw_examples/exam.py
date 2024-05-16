import datetime
import pprint

from czsc.data import TsDataCache
from czsc import home_path
from czsc.signals.tas import update_macd_cache
from czsc.analyze import CZSC
from hjw_examples.stock_process import *


def play():
    row = dict(ts_code="603960.sh", symbol="603960", name="克来机电")
    sdt, edt = "20200501", "20240202"
    result = bot_fx_detect(row=row, sdt=sdt, edt=edt)
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


if __name__ == '__main__':
    fx_reliability_exam()
