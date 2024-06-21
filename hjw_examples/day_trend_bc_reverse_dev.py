import os
import pprint
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
from src.stock_process import trend_reverse_ubi_entry

idx = 1000
script_name = os.path.basename(__file__)
logger.add("statics/logs/day_trend_bc_reverse.log", rotation="50MB", encoding="utf-8", enqueue=True, retention="10 days")


# ts_code      000001.SZ
# symbol          000001
# name              平安银行
# area                深圳
# industry            银行
# list_date     19910403
# Name: 0, dtype: object


def check(sdt: str = "20180501", edt: str = datetime.datetime.now().strftime('%Y%m%d')):
    stock_basic = TsDataCache(home_path).stock_basic()  # 只用于读取股票基础信息
    results = []  # 用于存储所有股票的结果

    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = {}
        for index, row in stock_basic.iterrows():
            _ts_code = row.get('ts_code')
            _today = datetime.datetime.today()
            logger.info(f"正在分析{_ts_code}在{edt}的走势")
            future = executor.submit(trend_reverse_ubi_entry, row, sdt, edt, 'D', 5)
            futures[future] = _ts_code  # 保存future和ts_code的映射

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

    email_subject = f"[测试][日线买点][A股]{edt}发现{len(results)}个买点"
    notify_buy_points(results=results, email_subject=email_subject, notify_empty=False)


if __name__ == '__main__':
    # # 获取当前日期
    # today = datetime.datetime.now()
    #
    # # 生成日期范围，从2024年1月1日到今天
    # date_range = pd.date_range(start='2024-01-17', end=today, freq='B')
    # formatted_dates = date_range.strftime('%Y%m%d').tolist()

    today = datetime.datetime.now().strftime("%Y%m%d")
    trade_dates = TsDataCache(home_path).get_dates_span('2024-03-30', today, is_open=True)

    # 将日期格式化为'%Y%m%d'
    for business_date in trade_dates:
        logger.info(f"测试日期:{business_date}")
        check(edt=business_date)
