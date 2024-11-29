import akshare as ak
import pandas as pd
from datetime import datetime

from database.models import *

logger.add("statics/logs/database.log", rotation="10MB", encoding="utf-8", enqueue=True, retention="10 days")


def get_concept_data():
    # 获取所有概念板块数据
    concept_df = ak.stock_board_concept_name_em()

    # 计算涨跌比（上涨家数 / (上涨家数 + 下跌家数)）
    concept_df['涨跌比'] = concept_df['上涨家数'] / (concept_df['上涨家数'] + concept_df['下跌家数'])

    # 按照涨跌比从大到小排序
    concept_df = concept_df.sort_values(by='涨跌比', ascending=False)

    # 获取涨跌比等于1的板块
    equal_to_1_df = concept_df[concept_df['涨跌比'] == 1]

    # 获取涨跌比小于1的前10位
    less_than_1_top_10_df = concept_df[concept_df['涨跌比'] < 1].head(10)

    # 合并两个结果
    result_df = pd.concat([equal_to_1_df, less_than_1_top_10_df])

    return result_df


def fetch_and_store_concept_name():
    # 获取概念板块数据
    concept_df = ak.stock_board_concept_name_em()

    # 计算涨跌比
    concept_df['涨跌比'] = concept_df['上涨家数'] / (concept_df['上涨家数'] + concept_df['下跌家数'])

    # 遍历数据并存入数据库
    switch_database("CONCEPT")
    with db_proxy.atomic():  # 使用事务确保数据一致性
        for _, row in concept_df.iterrows():
            ConceptName.create(
                name=row['板块名称'],
                code=row['板块代码'],
                rank=row['排名'],
                rise_ratio=row['涨跌比'],
                up_count=row['上涨家数'],
                down_count=row['下跌家数'],
                timestamp=datetime.now()
            )
    logger.info(f"{len(concept_df)} 条数据已成功插入数据库。")


if __name__ == '__main__':
    # # 获取并打印结果
    # result_df = get_concept_data()
    # print(result_df[['板块名称', '涨跌比', '上涨家数', '下跌家数']])
    fetch_and_store_concept_name()
