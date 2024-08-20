import pprint

from loguru import logger
import pandas as pd

from datetime import timedelta, datetime
from database.models import *

logger.add("statics/logs/database.log", rotation="10MB", encoding="utf-8", enqueue=True, retention="10 days")


def check_duplicate(symbol, check_date, days=30, fx_pwr=None, signals=None, db="BI"):
    """
    检查给定股票代码和日期是否在最近N天内已存在买点记录，并可以根据分型强度过滤。
    :param symbol: 股票代码
    :param check_date: 需要检查的日期
    :param days: 检查的天数范围，默认30天
    :param fx_pwr: 可选，分型强度过滤
    :param signals: 可选，买点类型过滤
    :param db: 数据库选择，可选BI或XD，默认BI
    :return: True表示重复, False表示未重复
    """
    switch_database(db)
    # 将 pandas.Timestamp 转换为 datetime 对象
    if isinstance(check_date, pd.Timestamp):
        check_date = check_date.to_pydatetime()

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
                     fx_pwr: str, expect_profit: float, industry: str, date: datetime,
                     high=None, low=None, reason=None, db="BI"):
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
    :param high: 分型高点
    :param low: 分型低点
    :param reason: 买点原因
    :param db: 数据库选择，可选BI或XD，默认BI
    """
    # logger.info(f"Inserting buy point with database choice: {db}")
    switch_database(db)

    if isinstance(date, pd.Timestamp):
        date = date.to_pydatetime()

    if not buy_point_exists(symbol, date, freq, db=db):
        try:
            with db_proxy.atomic():
                buy_point = BuyPoint.create(
                    name=name,
                    symbol=symbol,
                    ts_code=ts_code,
                    freq=freq,
                    signals=signals,
                    fx_pwr=fx_pwr,
                    expect_profit=expect_profit,
                    industry=industry,
                    high=high,
                    low=low,
                    date=date,
                    reason=reason
                )
            logger.info(f"成功插入新买点: {ts_code} {date} {signals} {reason} {buy_point}")
        except Exception as e:
            logger.error(f"插入买点时发生错误: {e}")
    else:
        logger.debug(f"买点已存在: {ts_code} {date}")


def query_latest_buy_point(symbol, fx_pwr=None, signals=None, freq=None, db="BI"):
    """
    根据股票代码查询最近的买点信息，可根据分型强度和信号过滤。
    :param symbol: 股票代码
    :param fx_pwr: 可选，分型强度过滤
    :param signals: 可选，买入信号过滤
    :param freq: 可选，交易级别过滤
    :param db: 数据库选择，可选BI或XD，默认BI
    :return: 配置好的查询对象
    """
    switch_database(db)

    # 构建基本查询
    query = BuyPoint.select().where(BuyPoint.symbol == symbol)

    # 根据 fx_pwr 添加过滤条件
    if fx_pwr is not None:
        query = query.where(BuyPoint.fx_pwr == fx_pwr)

    # 根据 signals 添加过滤条件
    if signals is not None:
        query = query.where(BuyPoint.signals == signals)

    # 根据 signals 添加过滤条件
    if freq is not None:
        query = query.where(BuyPoint.freq == freq)

    # 返回按日期降序排序的查询对象
    return query.order_by(BuyPoint.date.desc()).first()


def query_all_buy_point(symbol, fx_pwr=None, signals=None, freq=None, sdt=None, edt=None, db="BI"):
    """
    根据股票代码查询所有买点信息，可根据分型强度和信号过滤。
    :param symbol: 股票代码
    :param fx_pwr: 可选，分型强度过滤
    :param signals: 可选，买入信号过滤
    :param freq: 可选，交易级别过滤
    :param sdt: 可选，起始时间过滤
    :param edt: 可选，结束时间过滤
    :param db: 数据库选择，可选BI或XD，默认BI
    :return: 配置好的查询对象
    """
    switch_database(db)

    # 构建基本查询
    query = BuyPoint.select().where(BuyPoint.symbol == symbol)

    # 根据 fx_pwr 添加过滤条件
    if fx_pwr is not None:
        if isinstance(fx_pwr, list):
            query = query.where(BuyPoint.fx_pwr.in_(fx_pwr))
        else:
            query = query.where(BuyPoint.fx_pwr == fx_pwr)

    # 根据 signals 添加过滤条件
    if signals is not None:
        if isinstance(signals, list):
            query = query.where(BuyPoint.signals.in_(signals))
        else:
            query = query.where(BuyPoint.signals == signals)

    # 根据 起始时间 添加过滤条件
    if freq is not None:
        if isinstance(freq, list):
            query = query.where(BuyPoint.freq.in_(freq))
        else:
            query = query.where(BuyPoint.freq == freq)

    # 根据 起始时间 添加过滤条件
    if sdt is not None:
        query = query.where(BuyPoint.date >= sdt)

    # 根据 结束时间 添加过滤条件
    if edt is not None:
        query = query.where(BuyPoint.date <= edt)

    # 返回按日期降序排序的查询对象
    return query.order_by(BuyPoint.date.asc())


def buy_point_exists(symbol, check_date, freq, signals=None, db="BI"):
    """
    检查给定股票代码、日期和信号的买点是否已存在。

    :param symbol: 股票代码
    :param check_date: 检查的日期
    :param freq: K线图级别
    :param signals: 可选，买入信号
    :param db: 数据库选择，可选BI或XD，默认BI
    :return: 如果存在返回 True，否则返回 False
    """
    switch_database(db)

    # 将 pandas.Timestamp 转换为 datetime 对象
    if isinstance(check_date, pd.Timestamp):
        check_date = check_date.to_pydatetime()

    # 查询条件：股票代码、日期和买入信号都匹配
    query = BuyPoint.select().where(
        (BuyPoint.symbol == symbol) &
        (BuyPoint.date == check_date) &
        (BuyPoint.freq == freq)
    )
    # 根据 signals 添加过滤条件
    if signals is not None:
        query = query.where(BuyPoint.signals == signals)
    return query.exists()


def get_consecutive_symbols(start_date, end_date, min_occurrences: int, freq=None, signals=None, db="BI"):
    switch_database(db)

    query = BuyPoint.select().where((BuyPoint.date.between(start_date, end_date)))

    # 根据 signals 添加过滤条件
    if signals is not None:
        query = query.where(BuyPoint.signals == signals)
    # 根据 freq 添加过滤条件
    if freq is not None:
        query = query.where(BuyPoint.freq == freq)

    # Peewee查询语句
    query = (query
             .select(BuyPoint.name, BuyPoint.symbol, fn.COUNT(BuyPoint.symbol).alias('count'))
             .group_by(BuyPoint.symbol)
             .having(fn.COUNT(BuyPoint.symbol) > min_occurrences)
             .order_by(fn.COUNT(BuyPoint.symbol).desc()))
    results = [(entry.name, entry.symbol, entry.count) for entry in query]

    return results


def remove_duplicate_buy_points():
    switch_database("BI")
    query = BuyPoint.select()
    for e in query:
        print(f"{e.name} {e.symbol} {e.date}")
        insert_buy_point(e.name, e.symbol, e.ts_code, e.freq, e.signals, e.fx_pwr, e.expect_profit,
                         e.industry, e.date, db="TEMP")


def find_symbols_with_both_freqs(start_date, end_date, db="BI"):
    switch_database(db)

    # 子查询，找出在日期范围内，频率为'w'的symbol
    subquery_w = (BuyPoint
                  .select(BuyPoint.symbol)
                  .where((BuyPoint.date.between(start_date, end_date)) & (BuyPoint.freq == 'W'))
                  .group_by(BuyPoint.symbol))

    # 子查询，找出在日期范围内，频率为'D'的symbol
    subquery_d = (BuyPoint
                  .select(BuyPoint.symbol)
                  .where((BuyPoint.date.between(start_date, end_date)) & (BuyPoint.freq == 'D'))
                  .group_by(BuyPoint.symbol))

    # 找出既在subquery_w又在subquery_d中的symbol
    query = (BuyPoint
             .select(BuyPoint.symbol)
             .where(BuyPoint.symbol.in_(subquery_w) & BuyPoint.symbol.in_(subquery_d))
             .group_by(BuyPoint.symbol))

    # 执行查询并返回结果
    results = [entry.symbol for entry in query]
    return results


def demo(db="BI"):
    # 使用示例
    start_date = "2024-07-01"
    end_date = "2024-08-01"
    consecutive_symbols = get_consecutive_symbols(start_date, end_date, 2, db=db)
    print(f"Symbols appearing consecutively between {start_date} and {end_date}:")
    pprint.pp(list(consecutive_symbols))
    both_freqs_symbols = find_symbols_with_both_freqs(start_date, end_date, db=db)
    for symbol in both_freqs_symbols:
        bps = query_all_buy_point(symbol, sdt=start_date, edt=end_date, db=db)
        print(symbol, bps[0].name, '='*30)
        pprint.pp([(bp.freq, bp.fx_pwr, bp.date) for bp in bps])


if __name__ == '__main__':
    demo()
