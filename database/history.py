from loguru import logger

from datetime import timedelta, datetime
from database.models import BuyPoint

logger.add("statics/logs/database.log", rotation="10MB", encoding="utf-8", enqueue=True, retention="10 days")


def check_duplicate(symbol, check_date, days=30, fx_pwr=None, signals=None):
    """
    检查给定股票代码和日期是否在最近N天内已存在买点记录，并可以根据分型强度过滤。
    :param symbol: 股票代码
    :param check_date: 需要检查的日期
    :param days: 检查的天数范围，默认30天
    :param fx_pwr: 可选，分型强度过滤
    :param signals: 可选，买点类型过滤
    :return: True表示重复, False表示未重复
    """
    start_date = check_date - timedelta(days=days)

    # 构建基本查询条件
    query = BuyPoint.select().where(
        (BuyPoint.symbol == symbol) &
        (BuyPoint.date >= start_date) &
        (BuyPoint.date <= check_date)
    )

    # 如果提供了fx_pwr，则添加到查询条件中
    if fx_pwr is not None:
        query = query.where(BuyPoint.fx_pwr == fx_pwr)
    # 如果提供了fx_pwr，则添加到查询条件中
    if signals is not None:
        query = query.where(BuyPoint.signals == signals)

    exists = query.exists()
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
        logger.info(f"插入新买点: {ts_code} {date} {reason}")
    else:
        logger.debug(f"买点已存在: {ts_code} {date}")


def query_latest_buy_point(symbol, fx_pwr=None, signal=None):
    """
    根据股票代码查询最近的买点信息，可根据分型强度和信号过滤。
    :param symbol: 股票代码
    :param fx_pwr: 可选，分型强度过滤
    :param signal: 可选，买入信号过滤
    :return: 配置好的查询对象
    """
    # 构建基本查询
    query = BuyPoint.select().where(BuyPoint.symbol == symbol)

    # 根据 fx_pwr 添加过滤条件
    if fx_pwr is not None:
        query = query.where(BuyPoint.fx_pwr == fx_pwr)

    # 根据 signal 添加过滤条件
    if signal is not None:
        query = query.where(BuyPoint.signal == signal)

    # 返回按日期降序排序的查询对象
    return query.order_by(BuyPoint.date.desc()).first()


def buy_point_exists(symbol, check_date, signal):
    """
    检查给定股票代码、日期和信号的买点是否已存在。

    :param symbol: 股票代码
    :param check_date: 检查的日期
    :param signal: 买入信号
    :return: 如果存在返回 True，否则返回 False
    """
    # 查询条件：股票代码、日期和买入信号都匹配
    exists = BuyPoint.select().where(
        (BuyPoint.symbol == symbol) &
        (BuyPoint.date == check_date) &
        (BuyPoint.signal == signal)
    ).exists()
    return exists


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
