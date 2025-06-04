import os
from peewee import *
from datetime import datetime

# MariaDB数据库连接配置
mariadb_config = {
    'host': os.getenv("STATION_DB_IP"),
    'port': int(os.getenv("STATION_DB_PORT")),
    'user': os.getenv("STATION_DB_USER"),
    'password': os.getenv("STATION_DB_PWD"),
    'database': os.getenv("STATION_DB"),
    'charset': 'utf8mb4'
}

# 创建MariaDB连接
mariadb = MySQLDatabase(**mariadb_config)

class BaseMariaDBModel(Model):
    """MariaDB基础模型"""
    created_at = DateTimeField(default=datetime.now)
    updated_at = DateTimeField(default=datetime.now)
    
    class Meta:
        database = mariadb
        
    def save(self, *args, **kwargs):
        self.updated_at = datetime.now()
        return super().save(*args, **kwargs)


class BuyPointMariaDB(BaseMariaDBModel):
    """买点信息模型 - MariaDB版本"""
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
    date = DateTimeField(null=False)  # 买点检测到的时间
    reason = TextField(null=True)  # 买点原因

    class Meta:
        database = mariadb
        table_name = "buypoint"


class ConceptNameMariaDB(BaseMariaDBModel):
    """概念模型 - MariaDB版本"""
    name = CharField(max_length=100)  # 板块名称
    code = CharField(max_length=20)   # 板块代码
    rank = IntegerField()             # 排名
    rise_ratio = FloatField()         # 涨跌比
    up_count = IntegerField()         # 上涨家数
    down_count = IntegerField()       # 下跌家数
    timestamp = DateTimeField()       # 数据插入时间

    class Meta:
        database = mariadb
        table_name = "concept_name"


class ConceptConsMariaDB(BaseMariaDBModel):
    """概念成分股模型 - MariaDB版本"""
    name = CharField(max_length=100)            # 板块名称
    code = CharField(max_length=20)             # 板块代码
    symbol = CharField(max_length=20)           # 股票代码
    stock_name = CharField(max_length=100)      # 股票名称

    class Meta:
        database = mariadb
        table_name = "concept_cons"
        indexes = (
            (('code', 'symbol'), True),  # 唯一性索引
        )


# 同步状态记录表
class SyncStatus(BaseMariaDBModel):
    """同步状态记录表"""
    table_name = CharField(max_length=50, unique=True)  # 表名
    last_sync_time = DateTimeField()  # 最后同步时间
    last_sqlite_id = IntegerField(default=0)  # SQLite最后同步的ID
    total_synced = IntegerField(default=0)  # 总同步记录数
    
    class Meta:
        database = mariadb
        table_name = "sync_status"