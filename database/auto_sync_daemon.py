#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
自动增量同步守护进程
定期检查SQLite数据库更新并同步到MariaDB
"""

import time
import threading
import signal
import sys
from datetime import datetime
import logging
from database.sync_script import DataSyncManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('auto_sync_daemon.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class AutoSyncDaemon:
    """自动同步守护进程"""
    
    def __init__(self, sync_interval: int = 300):  # 默认5分钟同步一次
        self.sync_interval = sync_interval  # 同步间隔（秒）
        self.sync_manager = DataSyncManager()
        self.running = False
        self.sync_thread = None
        
        # 注册信号处理器
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """信号处理器"""
        logger.info(f"收到信号 {signum}，正在停止守护进程...")
        self.stop()
    
    def _sync_worker(self):
        """同步工作线程"""
        logger.info(f"自动同步守护进程启动，同步间隔: {self.sync_interval} 秒")
        
        while self.running:
            try:
                start_time = time.time()
                logger.info("开始执行增量同步...")
                
                # 执行增量同步
                self.sync_manager.sync_all_data(full_sync=False)
                
                elapsed_time = time.time() - start_time
                logger.info(f"本次同步完成，耗时 {elapsed_time:.2f} 秒")
                
                # 等待下次同步
                remaining_time = self.sync_interval
                while remaining_time > 0 and self.running:
                    sleep_time = min(remaining_time, 10)  # 每10秒检查一次是否需要停止
                    time.sleep(sleep_time)
                    remaining_time -= sleep_time
                    
            except Exception as e:
                logger.error(f"同步过程中发生错误: {e}")
                # 发生错误时等待较短时间后重试
                time.sleep(60)
    
    def start(self):
        """启动守护进程"""
        if self.running:
            logger.warning("守护进程已在运行")
            return
        
        self.running = True
        self.sync_thread = threading.Thread(target=self._sync_worker, daemon=True)
        self.sync_thread.start()
        logger.info("自动同步守护进程已启动")
    
    def stop(self):
        """停止守护进程"""
        if not self.running:
            return
        
        logger.info("正在停止自动同步守护进程...")
        self.running = False
        
        if self.sync_thread and self.sync_thread.is_alive():
            self.sync_thread.join(timeout=30)
        
        logger.info("自动同步守护进程已停止")
    
    def run_forever(self):
        """运行守护进程直到收到停止信号"""
        try:
            self.start()
            
            # 主线程保持运行，直到收到停止信号
            while self.running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("收到键盘中断信号")
        finally:
            self.stop()


class ScheduledSyncDaemon:
    """定时同步守护进程（支持更复杂的调度）"""
    
    def __init__(self):
        self.sync_manager = DataSyncManager()
        self.running = False
        self.scheduler_thread = None
        
        # 注册信号处理器
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # 同步计划配置
        self.sync_schedules = [
            {
                'name': 'frequent_sync',
                'interval': 300,  # 5分钟
                'last_run': datetime.min,
                'enabled': True
            },
            {
                'name': 'hourly_sync',
                'interval': 3600,  # 1小时
                'last_run': datetime.min,
                'enabled': True
            },
            {
                'name': 'daily_full_sync',
                'interval': 86400,  # 24小时
                'last_run': datetime.min,
                'enabled': False,  # 默认关闭全量同步
                'full_sync': True
            }
        ]
    
    def _signal_handler(self, signum, frame):
        """信号处理器"""
        logger.info(f"收到信号 {signum}，正在停止调度守护进程...")
        self.stop()
    
    def _scheduler_worker(self):
        """调度工作线程"""
        logger.info("调度同步守护进程启动")
        
        while self.running:
            try:
                current_time = datetime.now()
                
                for schedule in self.sync_schedules:
                    if not schedule['enabled']:
                        continue
                    
                    # 检查是否到了执行时间
                    time_since_last = (current_time - schedule['last_run']).total_seconds()
                    if time_since_last >= schedule['interval']:
                        logger.info(f"执行同步任务: {schedule['name']}")
                        
                        try:
                            full_sync = schedule.get('full_sync', False)
                            self.sync_manager.sync_all_data(full_sync=full_sync)
                            schedule['last_run'] = current_time
                            logger.info(f"同步任务 {schedule['name']} 完成")
                            
                        except Exception as e:
                            logger.error(f"同步任务 {schedule['name']} 执行失败: {e}")
                
                # 每分钟检查一次
                time.sleep(60)
                
            except Exception as e:
                logger.error(f"调度器发生错误: {e}")
                time.sleep(60)
    
    def start(self):
        """启动调度守护进程"""
        if self.running:
            logger.warning("调度守护进程已在运行")
            return
        
        self.running = True
        self.scheduler_thread = threading.Thread(target=self._scheduler_worker, daemon=True)
        self.scheduler_thread.start()
        logger.info("调度同步守护进程已启动")
    
    def stop(self):
        """停止调度守护进程"""
        if not self.running:
            return
        
        logger.info("正在停止调度同步守护进程...")
        self.running = False
        
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            self.scheduler_thread.join(timeout=30)
        
        logger.info("调度同步守护进程已停止")
    
    def run_forever(self):
        """运行守护进程直到收到停止信号"""
        try:
            self.start()
            
            while self.running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("收到键盘中断信号")
        finally:
            self.stop()


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='自动同步守护进程')
    parser.add_argument('--interval', type=int, default=300, 
                       help='同步间隔（秒），默认300秒')
    parser.add_argument('--mode', choices=['simple', 'scheduled'], default='simple',
                       help='运行模式：simple（简单定时）或 scheduled（复杂调度）')
    
    args = parser.parse_args()
    
    if args.mode == 'simple':
        daemon = AutoSyncDaemon(sync_interval=args.interval)
    else:
        daemon = ScheduledSyncDaemon()
    
    try:
        daemon.run_forever()
    except Exception as e:
        logger.error(f"守护进程运行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()