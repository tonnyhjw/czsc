import os
import sys
import datetime
import concurrent
import pandas as pd
from loguru import logger
from concurrent.futures import ProcessPoolExecutor


sys.path.insert(0, '.')
sys.path.insert(0, '..')
from czsc import home_path
from czsc.data import TsDataCache
from src.notify import notify_buy_points
from src.stock_process import bottom_pzbc


idx = 1000
script_name = os.path.basename(__file__)
logger.add("statics/logs/week_trend_bc_reverse.log", rotation="10MB", encoding="utf-8", enqueue=True, retention="10 days")


def check(sdt: str = "20180501", edt: str = datetime.datetime.now().strftime('%Y%m%d')):
    stock_basic = TsDataCache(home_path).stock_basic()  # 只用于读取股票基础信息
    results = []  # 用于存储所有股票的结果

    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = {}
        for index, row in stock_basic.iterrows():
            _ts_code = row.get('ts_code')
            _today = datetime.datetime.today()
            logger.info(f"正在分析{_ts_code}在{edt}的走势")
            future = executor.submit(bottom_pzbc, row, sdt, edt, 'W', 30)
            futures[future] = _ts_code  # 保存future和ts_code的映射

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

    notify_buy_points(results=results, edt=edt, notify_empty=False)


if __name__ == '__main__':
    # 获取当前日期
    today = datetime.datetime.now()

    # 生成日期范围，从2024年1月1日到今天的每周五
    date_range = pd.date_range(start='2024-01-01', end=today, freq='W-FRI')

    # 将日期格式化为'%Y%m%d'
    formatted_dates = date_range.strftime('%Y%m%d').tolist()
    for business_date in formatted_dates:
        logger.info(f"测试日期: {business_date}")
        check(edt=business_date)
