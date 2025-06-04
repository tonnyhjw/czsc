#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import time
from datetime import datetime
from typing import Tuple
import traceback

# 导入你的原始SQLite模型
from database.models import BuyPoint, ConceptName, ConceptCons

# 导入MariaDB模型
from database.mariadb_models import (
    BuyPointMariaDB, ConceptNameMariaDB, ConceptConsMariaDB, 
    SyncStatus, mariadb
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sync.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataSyncManager:
    """数据同步管理器"""
    
    def __init__(self):
        self.batch_size = 1000  # 批量处理大小
        
    def init_mariadb_tables(self):
        """初始化MariaDB表结构"""
        try:
            mariadb.connect()
            mariadb.create_tables([
                BuyPointMariaDB, 
                ConceptNameMariaDB, 
                ConceptConsMariaDB, 
                SyncStatus
            ], safe=True)
            logger.info("MariaDB表结构初始化完成")
        except Exception as e:
            logger.error(f"初始化MariaDB表结构失败: {e}")
            raise
        finally:
            if not mariadb.is_closed():
                mariadb.close()
    
    def get_sync_status(self, table_name: str) -> Tuple[int, datetime]:
        """获取同步状态"""
        try:
            sync_record = SyncStatus.get(SyncStatus.table_name == table_name)
            return sync_record.last_sqlite_id, sync_record.last_sync_time
        except SyncStatus.DoesNotExist:
            # 首次同步，创建记录
            sync_record = SyncStatus.create(
                table_name=table_name,
                last_sync_time=datetime(1900, 1, 1),
                last_sqlite_id=0
            )
            return 0, sync_record.last_sync_time
    
    def update_sync_status(self, table_name: str, last_id: int, synced_count: int):
        """更新同步状态"""
        sync_record, created = SyncStatus.get_or_create(
            table_name=table_name,
            defaults={
                'last_sync_time': datetime.now(),
                'last_sqlite_id': last_id,
                'total_synced': synced_count
            }
        )
        if not created:
            sync_record.last_sync_time = datetime.now()
            sync_record.last_sqlite_id = last_id
            sync_record.total_synced += synced_count
            sync_record.save()
    
    def sync_buypoint_data(self, full_sync: bool = False):
        """同步BuyPoint数据"""
        logger.info("开始同步BuyPoint数据...")
        
        try:
            mariadb.connect()
            
            if full_sync:
                # 全量同步：清空目标表
                BuyPointMariaDB.delete().execute()
                last_id = 0
                logger.info("执行全量同步，已清空目标表")
            else:
                # 增量同步：获取上次同步位置
                last_id, last_sync_time = self.get_sync_status('buypoint')
                logger.info(f"执行增量同步，从ID {last_id} 开始")
            
            # 查询SQLite中的新数据
            query = BuyPoint.select().where(BuyPoint.id > last_id).order_by(BuyPoint.id)
            total_count = query.count()
            
            if total_count == 0:
                logger.info("没有新数据需要同步")
                return
            
            logger.info(f"找到 {total_count} 条新数据需要同步")
            
            synced_count = 0
            current_batch = []
            max_id = last_id
            
            for record in query:
                current_batch.append({
                    'name': record.name,
                    'symbol': record.symbol,
                    'ts_code': record.ts_code,
                    'freq': record.freq,
                    'signals': record.signals,
                    'fx_pwr': record.fx_pwr,
                    'expect_profit': record.expect_profit,
                    'industry': record.industry,
                    'mark': record.mark,
                    'high': record.high,
                    'low': record.low,
                    'date': record.date,
                    'reason': record.reason
                })
                
                max_id = max(max_id, record.id)
                
                if len(current_batch) >= self.batch_size:
                    # 批量插入
                    BuyPointMariaDB.insert_many(current_batch).execute()
                    synced_count += len(current_batch)
                    current_batch = []
                    logger.info(f"已同步 {synced_count}/{total_count} 条记录")
            
            # 处理剩余数据
            if current_batch:
                BuyPointMariaDB.insert_many(current_batch).execute()
                synced_count += len(current_batch)
            
            # 更新同步状态
            self.update_sync_status('buypoint', max_id, synced_count)
            logger.info(f"BuyPoint数据同步完成，共同步 {synced_count} 条记录")
            
        except Exception as e:
            logger.error(f"同步BuyPoint数据失败: {e}")
            logger.error(traceback.format_exc())
            raise
        finally:
            if not mariadb.is_closed():
                mariadb.close()
    
    def sync_concept_name_data(self, full_sync: bool = False):
        """同步ConceptName数据"""
        logger.info("开始同步ConceptName数据...")
        
        try:
            mariadb.connect()
            
            if full_sync:
                ConceptNameMariaDB.delete().execute()
                last_id = 0
                logger.info("执行全量同步，已清空目标表")
            else:
                last_id, _ = self.get_sync_status('concept_name')
                logger.info(f"执行增量同步，从ID {last_id} 开始")
            
            query = ConceptName.select().where(ConceptName.id > last_id).order_by(ConceptName.id)
            total_count = query.count()
            
            if total_count == 0:
                logger.info("没有新数据需要同步")
                return
            
            logger.info(f"找到 {total_count} 条新数据需要同步")
            
            synced_count = 0
            current_batch = []
            max_id = last_id
            
            for record in query:
                current_batch.append({
                    'name': record.name,
                    'code': record.code,
                    'rank': record.rank,
                    'rise_ratio': record.rise_ratio,
                    'up_count': record.up_count,
                    'down_count': record.down_count,
                    'timestamp': record.timestamp
                })
                
                max_id = max(max_id, record.id)
                
                if len(current_batch) >= self.batch_size:
                    ConceptNameMariaDB.insert_many(current_batch).execute()
                    synced_count += len(current_batch)
                    current_batch = []
                    logger.info(f"已同步 {synced_count}/{total_count} 条记录")
            
            if current_batch:
                ConceptNameMariaDB.insert_many(current_batch).execute()
                synced_count += len(current_batch)
            
            self.update_sync_status('concept_name', max_id, synced_count)
            logger.info(f"ConceptName数据同步完成，共同步 {synced_count} 条记录")
            
        except Exception as e:
            logger.error(f"同步ConceptName数据失败: {e}")
            logger.error(traceback.format_exc())
            raise
        finally:
            if not mariadb.is_closed():
                mariadb.close()
    
    def sync_concept_cons_data(self, full_sync: bool = False):
        """同步ConceptCons数据"""
        logger.info("开始同步ConceptCons数据...")
        
        try:
            mariadb.connect()
            
            if full_sync:
                ConceptConsMariaDB.delete().execute()
                last_id = 0
                logger.info("执行全量同步，已清空目标表")
            else:
                last_id, _ = self.get_sync_status('concept_cons')
                logger.info(f"执行增量同步，从ID {last_id} 开始")
            
            query = ConceptCons.select().where(ConceptCons.id > last_id).order_by(ConceptCons.id)
            total_count = query.count()
            
            if total_count == 0:
                logger.info("没有新数据需要同步")
                return
            
            logger.info(f"找到 {total_count} 条新数据需要同步")
            
            synced_count = 0
            current_batch = []
            max_id = last_id
            
            for record in query:
                # 使用INSERT IGNORE处理唯一约束冲突
                try:
                    ConceptConsMariaDB.insert(
                        name=record.name,
                        code=record.code,
                        symbol=record.symbol,
                        stock_name=record.stock_name
                    ).on_conflict('IGNORE').execute()
                    synced_count += 1
                    max_id = max(max_id, record.id)
                except Exception as e:
                    logger.warning(f"插入记录失败 (ID: {record.id}): {e}")
                    continue
                
                if synced_count % 100 == 0:
                    logger.info(f"已同步 {synced_count}/{total_count} 条记录")
            
            self.update_sync_status('concept_cons', max_id, synced_count)
            logger.info(f"ConceptCons数据同步完成，共同步 {synced_count} 条记录")
            
        except Exception as e:
            logger.error(f"同步ConceptCons数据失败: {e}")
            logger.error(traceback.format_exc())
            raise
        finally:
            if not mariadb.is_closed():
                mariadb.close()
    
    def sync_all_data(self, full_sync: bool = False):
        """同步所有数据"""
        logger.info(f"开始{'全量' if full_sync else '增量'}同步所有数据...")
        start_time = time.time()
        
        try:
            self.init_mariadb_tables()
            
            # 按顺序同步各个表
            self.sync_buypoint_data(full_sync)
            self.sync_concept_name_data(full_sync)
            self.sync_concept_cons_data(full_sync)
            
            elapsed_time = time.time() - start_time
            logger.info(f"数据同步完成，耗时 {elapsed_time:.2f} 秒")
            
        except Exception as e:
            logger.error(f"数据同步失败: {e}")
            raise


def main():
    """主函数"""
    sync_manager = DataSyncManager()
    
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--full':
        # 全量同步
        sync_manager.sync_all_data(full_sync=True)
    else:
        # 增量同步
        sync_manager.sync_all_data(full_sync=False)


if __name__ == "__main__":
    main()