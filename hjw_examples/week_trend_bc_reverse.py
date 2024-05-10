import os
import sys
import datetime
import traceback
import concurrent
import pandas as pd
from loguru import logger
from concurrent.futures import ProcessPoolExecutor


sys.path.insert(0, '.')
sys.path.insert(0, '..')
from czsc import home_path
from czsc.data import TsDataCache
from hjw_examples.notify import send_email
from hjw_examples.formatters import sort_by_profit, sort_by_fx_pwr
from hjw_examples.templates.email_templates import daily_email_style
from hjw_examples.stock_process import trend_reverse_ubi_entry

idx = 1000
script_name = os.path.basename(__file__)
logger.add("statics/logs/week_trend_bc_reverse.log", rotation="10MB", encoding="utf-8", enqueue=True, retention="10 days")


# ts_code      000001.SZ
# symbol          000001
# name              平安银行
# area                深圳
# industry            银行
# list_date     19910403
# Name: 0, dtype: object


def check():
    stock_basic = TsDataCache(home_path).stock_basic()  # 只用于读取股票基础信息
    results = []  # 用于存储所有股票的结果

    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = {}
        for index, row in stock_basic.iterrows():
            _ts_code = row.get('ts_code')
            logger.info(f"正在分析{_ts_code}")
            future = executor.submit(trend_reverse_ubi_entry, row,
                                     "20200101", datetime.datetime.now().strftime('%Y%m%d'),
                                     'W', 15)
            futures[future] = _ts_code  # 保存future和ts_code的映射

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

    try:
        if results:
            # 将结果转换为 DataFrame
            sorted_results = sorted(results, key=sort_by_profit, reverse=True)
            sorted_results = sorted(sorted_results, key=sort_by_fx_pwr, reverse=True)
            df_results = pd.DataFrame(sorted_results)
            # 生成 HTML 表格
            html_table = df_results.to_html(classes='table table-striped table-hover', border=0, index=False, escape=False)
        else:
            html_table = "<h1>没有发现买点</h1>"

        styled_table = daily_email_style(html_table)

        # 发送电子邮件
        send_email(styled_table, f"[自动盯盘]发现{len(results)}个周线买点")
    except Exception as e_msg:
        tb = traceback.format_exc()  # 获取 traceback 信息
        logger.error(f"发送结果出现报错，{e_msg}\nTraceback: {tb}")


if __name__ == '__main__':
    output_name = f"statics/{script_name}_{datetime.datetime.today().strftime('%Y-%m-%d')}.txt"
    history_csv = f"statics/history/{script_name}.csv"
    check()