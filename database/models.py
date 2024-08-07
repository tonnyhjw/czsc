import datetime
from loguru import logger
from peewee import *

from database import configs

logger.add("statics/logs/database.log", rotation="10MB", encoding="utf-8", enqueue=True, retention="10 days")


# 连接SQLite数据库
db_buy_point_bi = SqliteDatabase(configs.BUY_POINT_BI_PATH)
db_buy_point_xd = SqliteDatabase(configs.BUY_POINT_XD_PATH)
db_buy_point_ma250 = SqliteDatabase(configs.BUY_POINT_MA250_PATH)
db_buy_point_bi_us = SqliteDatabase(configs.BUY_POINT_BI_US_PATH)
db_buy_point_maus = SqliteDatabase(configs.BUY_POINT_MAUS_PATH)
db_buy_point_temp = SqliteDatabase(configs.BUY_POINT_TEMP_PATH)


# 创建一个数据库代理
db_proxy = DatabaseProxy()


class BuyPoint(Model):
    """买点信息模型"""
    name = CharField(null=False)  # 股票名称
    symbol = CharField(null=False)  # 股票代码
    ts_code = CharField(null=False)  # 股票ts代码
    freq = CharField(null=False)  # 级别（日线、周线等）
    signals = CharField(null=True)  # 第几类买点
    fx_pwr = CharField(null=True)  # 分型强度
    expect_profit = FloatField(null=True)  # 预估收益比例（%）
    industry = CharField(null=True)  # 板块
    mark = CharField(null=True)  # 顶分型、底分型
    high = FloatField(null=True)  # 分型高点
    low = FloatField(null=True)  # 分型低点

    date = DateTimeField(null=False)  # 买点检测到的时间，通常是分型的第三根K线
    reason = TextField(null=True)  # 买点原因

    class Meta:
        database = db_proxy


# 函数用于切换数据库
def switch_database(db_choice: str):
    # logger.debug(f"Attempting to switch to database: {db_choice}")
    if db_choice == "BI":
        db_proxy.initialize(db_buy_point_bi)
    elif db_choice == "BIUS":
        db_proxy.initialize(db_buy_point_bi_us)
    elif db_choice == "XD":
        db_proxy.initialize(db_buy_point_xd)
    elif db_choice == "MA250":
        db_proxy.initialize(db_buy_point_ma250)
    elif db_choice == "MAUS":
        db_proxy.initialize(db_buy_point_maus)
    elif db_choice == "TEMP":
        db_proxy.initialize(db_buy_point_temp)
    else:
        raise ValueError(f"Invalid database choice: {db_choice}. Use BI, XD, MA250, MAUS, TEMP.")
    # logger.debug(f"Successfully switched to database: {db_proxy.obj.database}")


def create_tables():
    # 连接数据库
    switch_database("BI")

    # 创建表格
    db_proxy.create_tables([BuyPoint])


def test_connection(db_choice):
    switch_database(db_choice)
    try:
        print(BuyPoint.select().count())
        print(f"Successfully connected to database {db_choice}")
    except Exception as e:
        print(f"Failed to connect to database {db_choice}: {e}")


if __name__ == '__main__':
    # create_tables()
    # test_connection("BI")  # 测试数据库1
    # test_connection("XD")  # 测试数据库2
    test_connection("MA250")
