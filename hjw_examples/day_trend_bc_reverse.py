import os
import sys
import datetime
import argparse
import concurrent
from loguru import logger
from concurrent.futures import ProcessPoolExecutor


sys.path.insert(0, '.')
sys.path.insert(0, '..')
from czsc import home_path
from czsc.data import TsDataCache
from src.notify import notify_buy_points
from src.stock_process import trend_reverse_ubi_entry
from src import is_friday
from src.decorate import timer

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


@timer
def check(sdt: str = "20170101", edt: str = datetime.datetime.now().strftime('%Y%m%d'), freq: str = 'D',
          subj_lv1="自动盯盘", notify_empty=True):
    os.environ['czsc_min_bi_len'] = '7'
    tdc = TsDataCache(home_path)

    stock_basic = tdc.stock_basic()  # 只用于读取股票基础信息
    total_stocks = len(stock_basic)
    results = []  # 用于存储所有股票的结果

    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = {}
        for index, row in stock_basic.iterrows():
            _ts_code = row.get('ts_code')
            _today = datetime.datetime.today()
            logger.info(f"共{total_stocks}个股票，正在分析第{index}只个股{_ts_code}在{edt}的走势，"
                        f"进度{round(float(index/total_stocks)*100)}%")
            future = executor.submit(trend_reverse_ubi_entry, row, sdt, edt, freq, 5)
            futures[future] = _ts_code  # 保存future和ts_code的映射

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

    email_subject = f"[{subj_lv1}][{tdc.freq_map.get(freq)}买点][A股]{edt}发现{len(results)}个买点"

    notify_buy_points(results=results, email_subject=email_subject, notify_empty=notify_empty)


if __name__ == '__main__':
    ana_sdt = '20240322'
    today = datetime.datetime.now().strftime("%Y%m%d")

    parser = argparse.ArgumentParser(description="这是一个示例程序")
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
        TsDataCache(home_path).clear()

    # 根据参数决定运行模式
    if args.dev:
        logger.info("正在运行开发模式")
        logger.info(f"使用日期范围：{args.sd} 到 {args.ed}")
        trade_dates = TsDataCache(home_path).get_dates_span(args.sd, args.ed, is_open=True)
        # 将日期格式化为'%Y%m%d'
        for business_date in trade_dates:
            if args.f == "W" and not is_friday(business_date):
                continue
            logger.info(f"测试日期:{business_date}")
            check(edt=business_date, freq=args.f, subj_lv1="测试", notify_empty=False)
    else:
        logger.info("正在运行默认模式")
        check(freq=args.f)
