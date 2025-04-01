import argparse
import datetime
from loguru import logger

from czsc import home_path
from czsc.data import TsDataCache
from hjw_examples.concept_radar import hot_rank_notify

if __name__ == '__main__':
    today = datetime.datetime.now().strftime('%Y%m%d')

    parser = argparse.ArgumentParser(description="热门题材监控")
    parser.add_argument("-d", "--dev", action="store_true", help="运行开发模式")
    parser.add_argument("--sd", default="20250301", help="开发模式调试起始日")
    parser.add_argument("--ed", default=today, help="开发模式调试结束日")

    # 解析参数
    args = parser.parse_args()

    # 根据参数决定运行模式
    if args.dev:
        logger.info("正在运行开发模式")
        logger.info(f"使用日期范围：{args.sd} 到 {args.ed}")
        trade_dates = TsDataCache(home_path).get_dates_span(args.sd, args.ed, is_open=True)
        # 将日期格式化为'%Y%m%d'
        for business_date in trade_dates:
            logger.info(f"调试日期:{business_date}")

            hot_rank_notify(latest_timestamp=business_date, subj_lv1="测试")
    else:
        logger.info("正在运行默认模式")
        hot_rank_notify()