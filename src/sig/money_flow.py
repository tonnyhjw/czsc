import datetime
import os

from src.sig.utils import *
from czsc.data import TsDataCache
from czsc import DataClient, home_path
from database import history

cache_path = os.getenv("TS_CACHE_PATH", os.path.expanduser("~/.ts_data_cache"))
dc = DataClient(url="http://api.tushare.pro", cache_path=cache_path)


def money_flow_global(target_day, n_days, head_n: int = 50):
    results = []
    flow_types = dict()
    start_date, end_date = get_relative_str_date(target_day, n_days), target_day
    flow_data = dc.moneyflow(start_date=start_date, end_date=end_date)

    for sort_key in MONEY_FLOW_SORT_KEYS_AMOUNT:
        _flow_data = flow_data.sort_values(sort_key, ascending=False, ignore_index=True).head(head_n)
        for i, row in _flow_data.iterrows():
            symbol, exchange = row.get("ts_code").split(".")
            _target_day = datetime.datetime.strptime(target_day, "%Y%m%d")
            if history.check_duplicate(symbol, check_date=_target_day, days=5, db="BI"):
                buy_points = history.query_all_buy_point(symbol, edt=_target_day)
                buy_point = buy_points[-1]
                # if buy_point.signals != "一买":
                sort_type = MONEY_FLOW_SORT_KEYS_AMOUNT.get(sort_key)

                logger.info(f"{symbol} {buy_point.name}: {sort_type}_{i}"
                            f" {target_day=} {buy_point.date} "
                            f"{buy_point.freq=} {buy_point.signals=} {buy_point.fx_pwr=}")
                flow_type = f"{sort_type}_{i+1}"
                if flow_types.get(buy_point.name):
                    flow_types[buy_point.name].append(flow_type)
                else:
                    flow_types[buy_point.name] = [flow_type]
                    bp_freq = TsDataCache(home_path).freq_map.get(buy_point.freq)
                    symbol_link = f'<a href="https://xueqiu.com/S/{exchange}{symbol}">{symbol}</a>'
                    result = dict()
                    result["name"] = buy_point.name
                    result["symbol"] = symbol_link
                    result["fx_pwr"] = buy_point.fx_pwr
                    result["signals"] = buy_point.signals
                    result["bp_freq"] = bp_freq
                    result["industry"] = buy_point.industry
                    results.append(result)

    if flow_types and results:
        # 指定要添加的flow_types
        results = [{**r, "flow_types": "，".join(flow_types.get(r["name"]))} for r in results]

    return results


def money_flow_individual(ts_code, target_day, n_days):
    results = dict()
    flow_types = []
    buy_point = None
    start_date = get_relative_str_date(target_day, n_days)
    end_date = target_day
    flow_data = dc.moneyflow(ts_code=ts_code, start_date=start_date, end_date=end_date)
    for sort_key in MONEY_FLOW_SORT_KEYS_VOL:
        _flow_data = flow_data.sort_values(sort_key, ascending=False, ignore_index=True).head(30)

        for i, row in _flow_data.iterrows():
            symbol, exchange = row.get("ts_code").split(".")
            _target_date = datetime.datetime.strptime(end_date, "%Y%m%d")
            if history.check_duplicate(symbol, check_date=_target_date, days=5, db="BI"):
                buy_points = history.query_all_buy_point(symbol, edt=_target_date)
                buy_point = buy_points[-1]
                # if buy_point.signals != "一买":
                sort_type = MONEY_FLOW_SORT_KEYS_VOL.get(sort_key)
                logger.info(f"{symbol} {buy_point.name}: {sort_type}_{i}"
                            f" {_target_date=} {buy_point.date} "
                            f"{buy_point.freq=} {buy_point.signals=} {buy_point.fx_pwr=}")
                flow_types.append(f"{sort_type}_{i+1}")
                break

    if buy_point and flow_types:
        bp_freq = TsDataCache(home_path).freq_map.get(buy_point.freq)
        _hs = buy_point.ts_code.split(".")[-1]
        symbol_link = f'<a href="https://xueqiu.com/S/{_hs}{buy_point.symbol}">{buy_point.symbol}</a>'
        results["name"] = buy_point.name
        results["symbol"] = symbol_link
        results["fx_pwr"] = buy_point.fx_pwr
        results["signals"] = buy_point.signals
        results["bp_freq"] = bp_freq
        results["flow_types"] = "，".join(flow_types)
        results["industry"] = buy_point.industry
    return results

