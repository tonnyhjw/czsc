import datetime
import os

from src.sig.utils import *
from czsc.data import TsDataCache
from czsc import DataClient, home_path
from database import history

cache_path = os.getenv("TS_CACHE_PATH", os.path.expanduser("~/.ts_data_cache"))
dc = DataClient(url="http://api.tushare.pro", cache_path=cache_path)


def money_flow_global(ana_sd="20240201", ana_ed="20240808"):
    # flow_data = dc.moneyflow(ts_code='301548.SZ')
    # flow_data = dc.moneyflow(trade_date='20240607')
    # flow_data = dc.moneyflow(start_date='20240501', end_date='20240701')

    trade_dates = TsDataCache(home_path).get_dates_span(ana_sd, ana_ed, is_open=True)
    # 将日期格式化为'%Y%m%d'
    for business_date in trade_dates:
        start_date, end_date = get_relative_str_date(business_date), business_date
        flow_data = dc.moneyflow(start_date=start_date, end_date=end_date)
        for sort_key in MONEY_FLOW_SORT_KEYS_AMOUNT:
            _flow_data = flow_data.sort_values(sort_key, ascending=False, ignore_index=True).head(50)
            for i, row in _flow_data.iterrows():
                symbol, exchange = row.get("ts_code").split(".")
                _business_date = datetime.datetime.strptime(business_date, "%Y%m%d")
                if history.check_duplicate(symbol, check_date=_business_date, days=5, db="BI"):
                    buy_points = history.query_all_buy_point(symbol, edt=_business_date)
                    buy_point = buy_points[-1]
                    # if buy_point.signals != "一买":
                    logger.info(f"{symbol} {buy_point.name}: {MONEY_FLOW_SORT_KEYS_AMOUNT.get(sort_key)}_{i}"
                                f" {business_date=} {buy_point.date} "
                                f"{buy_point.freq=} {buy_point.signals=} {buy_point.fx_pwr=}")


def money_flow_individual(ts_code, start_date, end_date):
    flow_data = dc.moneyflow(ts_code=ts_code, start_date=start_date, end_date=end_date)
    for sort_key in MONEY_FLOW_SORT_KEYS_VOL:
        _flow_data = flow_data.sort_values(sort_key, ascending=False, ignore_index=True).head(10)

        for i, row in _flow_data.iterrows():
            symbol, exchange = row.get("ts_code").split(".")
            _target_date = datetime.datetime.strptime(end_date, "%Y%m%d")
            if history.check_duplicate(symbol, check_date=_target_date, days=5, db="BI"):
                buy_points = history.query_all_buy_point(symbol, edt=_target_date)
                buy_point = buy_points[-1]
                # if buy_point.signals != "一买":
                logger.info(f"{symbol} {buy_point.name}: {MONEY_FLOW_SORT_KEYS_VOL.get(sort_key)}_{i}"
                            f" {_target_date=} {buy_point.date} "
                            f"{buy_point.freq=} {buy_point.signals=} {buy_point.fx_pwr=}")
                break
