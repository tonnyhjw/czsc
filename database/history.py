from datetime import timedelta, datetime
from database.models import BuyPoint


def check_duplicate(ts_code, check_date, days=30):
    """
    检查给定股票代码和日期是否在最近N天内已存在买点记录
    :param ts_code: 股票代码
    :param check_date: 需要检查的日期
    :param days: 检查的天数范围,默认30天
    :return: True表示重复,False表示未重复
    """
    start_date = check_date - timedelta(days=days)
    exists = BuyPoint.select().where(
        BuyPoint.ts_code == ts_code,
        BuyPoint.date >= start_date,
        BuyPoint.date <= check_date
    ).exists()
    return exists


def insert_buy_point(name: str, symbol: str, ts_code: str, freq: str, signals: str,
                     fx_pwr: str, expect_profit: float, industry: str, date: datetime, reason=None):
    """
    插入新的买点记录
    :param name: 股票名称
    :param symbol: 股票代码
    :param ts_code: tushare代码
    :param freq: K线级别
    :param signals: 买点类型
    :param fx_pwr: 分型强度
    :param expect_profit: 预期收益
    :param industry: 板块
    :param date: 买点日期
    :param reason: 买点原因
    """
    if not check_duplicate(ts_code, date):
        buy_point = BuyPoint.create(
            name=name,
            symbol=symbol,
            ts_code=ts_code,
            freq=freq,
            signals=signals,
            fx_pwr=fx_pwr,
            expect_profit=expect_profit,
            industry=industry,
            date=date,
            reason=reason
        )
        buy_point.save()
        print(f"插入新买点: {ts_code} {date} {reason}")
    else:
        print(f"买点已存在: {ts_code} {date}")


def demo():
    # 假设你已经分析出一个买点
    name = '某股票'
    symbol = '600036'
    ts_code = '600036.SH'
    freq = 'D'
    signals = '一买'
    fx_pwr = '强'
    expect_profit = 16.2
    industry = '半导体'
    date = datetime.today()
    reason = '背驰强一买'

    insert_buy_point(name, symbol, ts_code, freq, signals, fx_pwr, expect_profit, industry, date, reason)
