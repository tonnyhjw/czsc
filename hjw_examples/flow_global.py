import os
import sys
import argparse
import datetime
import concurrent
from loguru import logger
from concurrent.futures import ProcessPoolExecutor


sys.path.insert(0, '.')
sys.path.insert(0, '..')
from czsc import home_path, DataClient
from czsc.data import TsDataCache
from src.notify import notify_money_flow
from src.sig.money_flow import money_flow_global

script_name = os.path.basename(__file__)
cache_path = os.getenv("TS_CACHE_PATH", os.path.expanduser("~/.ts_data_cache"))
# dc = DataClient(url="http://api.tushare.pro", cache_path=cache_path)
logger.add("statics/logs/flow.log", rotation="10MB", encoding="utf-8", enqueue=True, retention="10 days")


def check(target_day: str = datetime.datetime.now().strftime('%Y%m%d'), n_days: int = 365, head_n: int = 50,
          subj_lv1="自动盯盘", notify_empty=True):
    results = money_flow_global(target_day, n_days, head_n)

    email_subject = f"[{subj_lv1}][头部资金流][A股]{target_day}发现{len(results)}个{n_days}天资金流处于头部的买点"
    notify_money_flow(results=results, email_subject=email_subject, notify_empty=notify_empty)


if __name__ == '__main__':
    ana_sdt = '20240322'
    _sdt = '20230101'
    today = datetime.datetime.now().strftime("%Y%m%d")

    parser = argparse.ArgumentParser(description="这是一个示例程序")
    # 添加参数
    parser.add_argument("--n", default=1, type=int, help="选取n天作为分析区间")
    parser.add_argument("--h", default=10, type=int, help="选取成交额的头部区间")
    parser.add_argument("--tgd", default=today, type=str, help="目标日期")
    parser.add_argument("--sd", default=ana_sdt, help="分析开始日期")
    parser.add_argument("--ed", default=today, help="分析结束日期")
    parser.add_argument("-d", "--dev", action="store_true", help="运行开发模式")
    # parser.add_argument("-r", "--refresh", action="store_true", help="更新缓存")

    # 解析参数
    args = parser.parse_args()

    # # 判断是否更新缓存
    # if args.refresh:
    #     TsDataCache(home_path).clear()

    # 根据参数决定运行模式
    if args.dev:
        logger.info("正在运行开发模式")
        logger.info(f"使用日期范围：{args.sd} 到 {args.ed}")
        trade_dates = TsDataCache(home_path).get_dates_span(args.sd, args.ed, is_open=True)
        # 将日期格式化为'%Y%m%d'
        for business_date in trade_dates:
            logger.info(f"测试日期:{business_date}")
            check(business_date, args.n, args.h, subj_lv1="测试", notify_empty=False)
    else:
        logger.info(f"正在运行默认模式，目标日期：{args.tgd}，共{args.n}天")
        check(args.tgd, args.n, args.h)