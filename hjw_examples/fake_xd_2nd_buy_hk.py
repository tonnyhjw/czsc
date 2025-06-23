import os
import sys
import argparse
import datetime
import concurrent
import time

import pandas as pd
from loguru import logger
from concurrent.futures import ProcessPoolExecutor


sys.path.insert(0, '.')
sys.path.insert(0, '..')
from czsc import home_path
from src.connectors.hk_cache import HKDataCache
from src.notify import notify_buy_points
from src.stock_process import fake_xd_2nd_buy_hk
from src import is_friday
from src.decorate import timer

idx = 1000
script_name = os.path.basename(__file__)
logger.add("statics/logs/fake_xd.log", rotation="10MB", encoding="utf-8", enqueue=True, retention="10 days")


@timer
def check(sdt: str = "20240101", edt: str = datetime.datetime.now().strftime('%Y%m%d'),
          freq: str = 'D', subj_lv1="自动盯盘", notify_empty=True, slow=False):
    os.environ['czsc_min_bi_len'] = '7'
    hkdc = HKDataCache(home_path)

    ggt_components = hkdc.ggt_components()  # 获取港股通成分股名单
    total_stocks = len(ggt_components)
    results = []  # 用于存储所有股票的结果
    logger.info(f"共{total_stocks}个股票待分析")
    for index, row in ggt_components.iterrows():
        _ts_code = row.get('ts_code')
        _today = datetime.datetime.today()
        result = fake_xd_2nd_buy_hk(row, sdt, edt, freq, 3)
        if result:
            results.append(result)
        if slow:
            time.sleep(3)

    email_subject = f"[{subj_lv1}][{hkdc.freq_map.get(freq)}模拟线段二买][港股]{edt}发现{len(results)}个买点"
    notify_buy_points(results=results, email_subject=email_subject, notify_empty=notify_empty)


if __name__ == '__main__':
    ana_sdt = '20240901'
    slow = False
    today = datetime.datetime.now().strftime("%Y%m%d")

    parser = argparse.ArgumentParser(description="模拟线段二买")
    # 添加参数
    parser.add_argument("--f", type=str, default="D", help="K线级别")
    parser.add_argument("--sd", default=ana_sdt, help="分析开始日期")
    parser.add_argument("--ed", default=today, help="分析结束日期")
    parser.add_argument("-d", "--dev", action="store_true", help="运行开发模式")
    parser.add_argument("-r", "--refresh", action="store_true", help="更新缓存")

    # 解析参数
    args = parser.parse_args()

    # 判断是否更新缓存
    if args.refresh:
        HKDataCache(home_path).clear()
        slow = True

    # 根据参数决定运行模式
    if args.dev:
        logger.info("正在运行开发模式")
        logger.info(f"使用日期范围：{args.sd} 到 {args.ed}")
        date_range = pd.date_range(start=args.sd, end=args.ed, freq="B")
        # 将日期格式化为'%Y%m%d'
        formatted_dates = date_range.strftime('%Y%m%d').tolist()

        for business_date in formatted_dates:
            if args.f == "W" and not is_friday(business_date):
                continue
            logger.info(f"测试日期:{business_date}")
            check(edt=business_date, freq=args.f, subj_lv1="测试", notify_empty=False, slow=slow)
    else:
        logger.info("正在运行默认模式")
        check(freq=args.f, slow=slow)

