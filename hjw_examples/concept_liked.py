import argparse
import datetime

from hjw_examples.concept_radar import liked

if __name__ == '__main__':
    today = datetime.datetime.now().strftime('%Y%m%d')

    parser = argparse.ArgumentParser(description="关注题材监控")
    parser.add_argument("--dt", default=today, help="目标日期")

    # 解析参数
    args = parser.parse_args()

    liked(bp_days_limit=3, latest_timestamp=args.dt)
