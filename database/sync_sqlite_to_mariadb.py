import sys
import os
import datetime
from peewee import *
# from playhouse.migrate import migrate, SqliteMigrator, MySQLMigrator
from playhouse.shortcuts import model_to_dict

# SQLite数据库连接设置
sqlite_db = SqliteDatabase('your_sqlite_database.db')  # 替换为您的SQLite数据库文件路径
db_concept_em = SqliteDatabase('your_concept_em.db')  # 替换为您的概念数据库文件路径

# MariaDB连接设置
mariadb = MySQLDatabase(
    'your_mariadb_database',  # 替换为您的MariaDB数据库名称
    user='your_username',      # 替换为您的用户名
    password='your_password',  # 替换为您的密码
    host='your_host',          # 替换为您的MariaDB主机地址
    port=3306                  # 替换为您的MariaDB端口号
)

# ========== SQLite模型定义 ==========
class SQLiteBuyPoint(Model):
    """买点信息模型 (SQLite)"""
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
        database = sqlite_db
        table_name = 'buypoint'  # 确保表名与SQLite中的表名一致

class SQLiteConceptName(Model):
    """定义概念模型 (SQLite)"""
    id = AutoField(primary_key=True)  # 自增主键
    name = CharField(max_length=100)  # 板块名称
    code = CharField(max_length=20)   # 板块代码
    rank = IntegerField()             # 排名
    rise_ratio = FloatField()         # 涨跌比
    up_count = IntegerField()         # 上涨家数
    down_count = IntegerField()       # 下跌家数
    timestamp = DateTimeField()       # 数据插入时间
    
    class Meta:
        database = db_concept_em
        table_name = "concept_name"

class SQLiteConceptCons(Model):
    """概念成分股模型 (SQLite)"""
    id = AutoField(primary_key=True)            # 自增主键
    name = CharField(max_length=100)            # 板块名称
    code = CharField(max_length=20)             # 板块代码
    symbol = CharField(max_length=20)           # 股票代码
    stock_name = CharField(max_length=100)      # 股票名称
    
    class Meta:
        database = db_concept_em
        table_name = "concept_cons"


# ========== MariaDB模型定义 ==========
class MariaBuyPoint(Model):
    """买点信息模型 (MariaDB)"""
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
        database = mariadb
        table_name = 'buypoint'

class MariaConceptName(Model):
    """定义概念模型 (MariaDB)"""
    id = AutoField(primary_key=True)  # 自增主键
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

class MariaConceptCons(Model):
    """概念成分股模型 (MariaDB)"""
    id = AutoField(primary_key=True)            # 自增主键
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


def create_mariadb_tables():
    """在MariaDB中创建表"""
    print("创建MariaDB表...")
    mariadb.connect()
    
    # 如果表已存在，先删除它们
    mariadb.execute_sql('DROP TABLE IF EXISTS buypoint;')
    mariadb.execute_sql('DROP TABLE IF EXISTS concept_name;')
    mariadb.execute_sql('DROP TABLE IF EXISTS concept_cons;')
    
    # 创建表
    mariadb.create_tables([MariaBuyPoint, MariaConceptName, MariaConceptCons])
    print("MariaDB表创建完成！")
    mariadb.close()

def sync_buypoint_data():
    """同步买点数据"""
    print("同步买点数据...")
    
    # 连接数据库
    sqlite_db.connect()
    mariadb.connect()
    
    # 获取所有SQLite数据
    sqlite_records = SQLiteBuyPoint.select()
    
    # 批量插入到MariaDB
    with mariadb.atomic():
        # 每次批量插入的数量
        batch_size = 500
        total_records = sqlite_records.count()
        
        for i in range(0, total_records, batch_size):
            batch = sqlite_records.offset(i).limit(batch_size)
            data_to_insert = []
            
            for record in batch:
                # 转换为字典并移除id字段(如果存在)
                record_dict = model_to_dict(record)
                if 'id' in record_dict:
                    del record_dict['id']
                
                data_to_insert.append(record_dict)
            
            if data_to_insert:
                MariaBuyPoint.insert_many(data_to_insert).execute()
            
            print(f"已同步 {min(i + batch_size, total_records)}/{total_records} 条买点数据")
    
    # 关闭连接
    sqlite_db.close()
    mariadb.close()
    print("买点数据同步完成！")

def sync_concept_name_data():
    """同步概念名称数据"""
    print("同步概念名称数据...")
    
    # 连接数据库
    db_concept_em.connect()
    mariadb.connect()
    
    # 获取所有SQLite数据
    sqlite_records = SQLiteConceptName.select()
    
    # 批量插入到MariaDB
    with mariadb.atomic():
        # 每次批量插入的数量
        batch_size = 500
        total_records = sqlite_records.count()
        
        for i in range(0, total_records, batch_size):
            batch = sqlite_records.offset(i).limit(batch_size)
            data_to_insert = []
            
            for record in batch:
                # 转换为字典并移除id字段
                record_dict = model_to_dict(record)
                if 'id' in record_dict:
                    del record_dict['id']  # MariaDB会自动生成ID
                
                data_to_insert.append(record_dict)
            
            if data_to_insert:
                MariaConceptName.insert_many(data_to_insert).execute()
            
            print(f"已同步 {min(i + batch_size, total_records)}/{total_records} 条概念名称数据")
    
    # 关闭连接
    db_concept_em.close()
    mariadb.close()
    print("概念名称数据同步完成！")

def sync_concept_cons_data():
    """同步概念成分股数据"""
    print("同步概念成分股数据...")
    
    # 连接数据库
    db_concept_em.connect()
    mariadb.connect()
    
    # 获取所有SQLite数据
    sqlite_records = SQLiteConceptCons.select()
    
    # 批量插入到MariaDB
    with mariadb.atomic():
        # 每次批量插入的数量
        batch_size = 500
        total_records = sqlite_records.count()
        
        for i in range(0, total_records, batch_size):
            batch = sqlite_records.offset(i).limit(batch_size)
            data_to_insert = []
            
            for record in batch:
                # 转换为字典并移除id字段
                record_dict = model_to_dict(record)
                if 'id' in record_dict:
                    del record_dict['id']  # MariaDB会自动生成ID
                
                data_to_insert.append(record_dict)
            
            if data_to_insert:
                # 使用replace方式插入，避免唯一索引冲突
                MariaConceptCons.insert_many(data_to_insert).on_conflict('REPLACE').execute()
            
            print(f"已同步 {min(i + batch_size, total_records)}/{total_records} 条概念成分股数据")
    
    # 关闭连接
    db_concept_em.close()
    mariadb.close()
    print("概念成分股数据同步完成！")

def main():
    """主函数"""
    try:
        # 创建MariaDB表
        create_mariadb_tables()
        
        # 同步数据
        sync_buypoint_data()
        sync_concept_name_data()
        sync_concept_cons_data()
        
        print("所有数据同步完成！")
    except Exception as e:
        print(f"同步过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
