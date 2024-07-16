import os
import sys
import datetime
import concurrent
from loguru import logger
from concurrent.futures import ProcessPoolExecutor


sys.path.insert(0, '.')
sys.path.insert(0, '..')
from czsc import home_path
from czsc.data import TsDataCache
from src.notify import notify_buy_points
from src.stock_process import ma_pzbc

idx = 1000
script_name = os.path.basename(__file__)
logger.add("statics/logs/day_ma_support.log", rotation="10MB", encoding="utf-8", enqueue=True, retention="10 days")


def check(sdt: str = "20180101", edt: str = datetime.datetime.now().strftime('%Y%m%d')):
    stock_basic = TsDataCache(home_path).stock_basic()  # 只用于读取股票基础信息
    total_stocks = len(stock_basic)
    results = []  # 用于存储所有股票的结果

    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = {}
        for index, row in stock_basic.iterrows():
            _ts_code = row.get('ts_code')
            _today = datetime.datetime.today()
            logger.info(f"共{total_stocks}个股票，正在分析第{index}只个股{_ts_code}在{edt}的走势，"
                        f"进度{round(float(index/total_stocks)*100)}%")
            future = executor.submit(ma_pzbc, row, sdt, edt, 'D', 2)
            futures[future] = _ts_code  # 保存future和ts_code的映射

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

    email_subject = f"[测试][日线ma支撑][A股]{edt}发现{len(results)}个买点"
    notify_buy_points(results=results, email_subject=email_subject, notify_empty=False)


if __name__ == '__main__':
    first_day = '2024-03-22'
    last_day = '2024-05-12'
    # last_day = datetime.datetime.now().strftime("%Y-%m-%d")
    trade_dates = TsDataCache(home_path).get_dates_span(first_day, last_day, is_open=True)

    # 将日期格式化为'%Y%m%d'
    for business_date in trade_dates:
        logger.info(f"测试日期:{business_date}")
        check(sdt="20200101", edt=business_date)
