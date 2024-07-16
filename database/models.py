import datetime

from peewee import *

from database.configs import *

# 连接SQLite数据库
db_buy_point_bi = SqliteDatabase(BUY_POINT_BI_PATH)
db_buy_point_xd = SqliteDatabase(BUY_POINT_XD_PATH)

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
    if db_choice == "BI":
        db_proxy.initialize(db_buy_point_bi)
    elif db_choice == "XD":
        db_proxy.initialize(db_buy_point_xd)
    else:
        raise ValueError("Invalid database choice. Use BI or XD.")


def create_tables():
    # 连接数据库
    switch_database("BI")

    # 创建表格
    db_proxy.create_tables([BuyPoint])


if __name__ == '__main__':
    create_tables()
