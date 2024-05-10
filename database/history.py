from datetime import timedelta, date
from database.models import BuyPoint


def check_duplicate(symbol, check_date, days=30):
    """
    检查给定股票代码和日期是否在最近N天内已存在买点记录
    :param stock_code: 股票代码
    :param check_date: 需要检查的日期
    :param days: 检查的天数范围,默认30天
    :return: True表示重复,False表示未重复
    """
    start_date = check_date - timedelta(days=days)
    exists = BuyPoint.select().where(
        BuyPoint.symbol == symbol,
        BuyPoint.date >= start_date,
        BuyPoint.date <= check_date
    ).exists()
    return exists


def insert_buy_point(stock_code, date, reason):
    """
    插入新的买点记录
    :param stock_code: 股票代码
    :param date: 买点日期
    :param reason: 买点原因
    """
    if not check_duplicate(stock_code, date):
        buy_point = BuyPoint.create(
            stock_code=stock_code,
            date=date,
            reason=reason
        )
        buy_point.save()
        print(f"插入新买点: {stock_code} {date} {reason}")
    else:
        print(f"买点已存在: {stock_code} {date}")


def demo():
    # 假设你已经分析出一个买点
    stock_code = '600036'
    buy_date = date(2023, 4, 28)
    reason = 'MACD金叉'

    insert_buy_point(stock_code, buy_date, reason)