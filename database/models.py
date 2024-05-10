import datetime

from peewee import *

from database.configs import BUY_POINT_PATH

# 连接SQLite数据库
db = SqliteDatabase(BUY_POINT_PATH)


class BuyPoint(Model):
    """买点信息模型"""
    name = CharField(null=False)  # 股票名称
    symbol = CharField(null=False)  # 股票代码
    ts_code = CharField()  # 股票ts代码
    freq = CharField(null=False)  # 级别（日线、周线等）
    signals = CharField()  # 第几类买点
    fx_pwr = CharField()  # 分型强度
    expect_profit = CharField()  # 预估收益比例（%）
    industry = CharField()  # 板块
    mark = CharField()  # 顶分型、底分型
    high = FloatField()  # 分型高点
    low = FloatField()  # 分型低点

    date = DateField(null=False)  # 买点检测到的日期，通常是分型的第三根K线
    reason = TextField(default=None)  # 买点原因

    class Meta:
        database = db


def create_tables():
    # 连接数据库
    db.connect()

    # 创建表格
    db.create_tables([BuyPoint])


if __name__ == '__main__':
    create_tables()
