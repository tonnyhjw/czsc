import os
import pprint
import sys
import argparse
import datetime
import concurrent
import pandas as pd
from loguru import logger
from concurrent.futures import ProcessPoolExecutor


sys.path.insert(0, '.')
sys.path.insert(0, '..')
from czsc import home_path
from src.connectors.yf_cache import YfDataCache
from src.notify import notify_buy_points
from src.stock_process import trend_reverse_ubi_entry_us
from src import is_friday

idx = 1000
script_name = os.path.basename(__file__)
logger.add("statics/logs/day_trend_bc_reverse.log", rotation="50MB", encoding="utf-8", enqueue=True, retention="10 days")


# Symbol                                        MMM
# Security                                       3M
# GICS Sector                           Industrials
# GICS Sub-Industry        Industrial Conglomerates
# Headquarters Location       Saint Paul, Minnesota
# Date added                             1957-03-04
# CIK                                         66740
# Founded                                      1902


def check(sdt: str = "20180101", edt: str = datetime.datetime.now().strftime('%Y%m%d'), freq: str = 'D',
          subj_lv1="自动盯盘"):
    os.environ['czsc_min_bi_len'] = '7'
    ydc = YfDataCache(home_path)
    snp500 = ydc.wiki_snp500_member()  # 只用于读取股票基础信息
    total_stocks = len(snp500)
    results = []  # 用于存储所有股票的结果

    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = {}
        for index, row in snp500.iterrows():
            _symbol = row.get('Symbol')
            _today = datetime.datetime.today()
            logger.info(f"共{total_stocks}个股票，正在分析第{index}只个股[{_symbol}]在{edt}的走势，"
                        f"进度{round(float(index/total_stocks)*100)}%")

            future = executor.submit(trend_reverse_ubi_entry_us, row, sdt, edt, freq, 5)
            futures[future] = _symbol  # 保存future和_symbol的映射

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

    email_subject = f"[{subj_lv1}][{ydc.freq_map.get(freq)}买点][美股]{edt}发现{len(results)}个买点"
    notify_buy_points(results=results, email_subject=email_subject, notify_empty=False)


if __name__ == '__main__':
    ana_sdt = '20240322'
    today = datetime.datetime.now().strftime("%Y%m%d")

    parser = argparse.ArgumentParser(description="这是一个示例程序")
    # 添加参数
    parser.add_argument("--f", type=str, default="D", help="K线级别")
    parser.add_argument("--sd", default=ana_sdt, help="分析开始日期")
    parser.add_argument("--ed", default=today, help="分析结束日期")
    parser.add_argument("-d", "--dev", action="store_true", help="运行开发模式")

    # 解析参数
    args = parser.parse_args()

    # 根据参数决定运行模式
    if args.dev:
        logger.info("正在运行开发模式")
        logger.info(f"使用日期范围：{args.sd} 到 {args.ed}")
        date_range = pd.date_range(start='2024-01-01', end=today, freq="B")
        # 将日期格式化为'%Y%m%d'
        formatted_dates = date_range.strftime('%Y%m%d').tolist()

        for business_date in formatted_dates:
            if args.f == "W" and not is_friday(business_date):
                continue
            logger.info(f"测试日期:{business_date}")
            check(edt=business_date, freq=args.f, subj_lv1="测试")
    else:
        logger.info("正在运行默认模式")
        check(freq=args.f)
