import datetime

from peewee import *

from database.configs import BUY_POINT_PATH

# 连接SQLite数据库
db = SqliteDatabase(BUY_POINT_PATH)


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
        database = db


def create_tables():
    # 连接数据库
    db.connect()

    # 创建表格
    db.create_tables([BuyPoint])


if __name__ == '__main__':
    create_tables()
