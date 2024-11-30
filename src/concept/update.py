import datetime
import pandas as pd
import akshare as ak
from loguru import logger


from database.models import ConceptName, ConceptCons, db_concept_em

logger.add("statics/logs/concept.log", rotation="10MB", encoding="utf-8", enqueue=True, retention="10 days")


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
    _timestamp = datetime.datetime.now()

    # 计算涨跌比
    concept_df['涨跌比'] = concept_df['上涨家数'] / (concept_df['上涨家数'] + concept_df['下跌家数'])

    # 遍历数据并存入数据库
    with db_concept_em.atomic():  # 使用事务确保数据一致性
        for _, row in concept_df.iterrows():
            ConceptName.create(
                name=row['板块名称'],
                code=row['板块代码'],
                rank=row['排名'],
                rise_ratio=row['涨跌比'],
                up_count=row['上涨家数'],
                down_count=row['下跌家数'],
                timestamp=_timestamp
            )
    logger.info(f"{len(concept_df)} 条数据已成功插入数据库。")


def fetch_and_store_concept_cons():
    # 获取概念板块数据
    concept_name_df = ak.stock_board_concept_name_em()
    new_concept_name, new_concept_cons = [], []

    # 遍历数据并存入数据库
    with db_concept_em.atomic():  # 使用事务确保数据一致性
        for _, concept_name_row in concept_name_df.iterrows():
            _concept_name = concept_name_row['板块名称']
            _concept_code = concept_name_row['板块代码']

            # 查重：检查概念代码是否已存在
            if not ConceptCons.select().where(ConceptCons.code == _concept_code).exists():
                new_concept_name.append((_concept_name, _concept_code))

            # 获取该概念的成分股
            concept_cons_df = ak.stock_board_concept_cons_em(symbol=_concept_name)

            for _, concept_cons_row in concept_cons_df.iterrows():
                _symbol = concept_cons_row['代码']
                _stock_name = concept_cons_row['名称']

                # 查重：检查 (概念代码, 股票代码) 是否已存在
                if not ConceptCons.select().where(
                    (ConceptCons.code == _concept_code) &
                    (ConceptCons.symbol == _symbol)
                ).exists():
                    new_concept_cons.append({
                        "name": _concept_name,
                        "code": _concept_code,
                        "symbol": _symbol,
                        "stock_name": _stock_name
                    })

        # 批量插入新的概念数据
        if new_concept_cons:
            ConceptCons.insert_many(new_concept_cons).execute()

    # 打印结果
    logger.info(f"新增概念数量: {len(new_concept_name)}")
    logger.info(f"新增成分股记录数量: {len(new_concept_cons)}")
    return {"new_concept_name": new_concept_name, "new_concept_cons": new_concept_cons}


def notify_new_concept_name():
    pass


if __name__ == '__main__':
    # # 获取并打印结果
    # result_df = get_concept_data()
    # print(result_df[['板块名称', '涨跌比', '上涨家数', '下跌家数']])
    fetch_and_store_concept_name()
    fetch_and_store_concept_cons()
