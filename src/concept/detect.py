from peewee import fn
from loguru import logger
from collections import Counter
from playhouse.shortcuts import model_to_dict

from database.models import ConceptName, ConceptCons


logger.add("statics/logs/concept.log", rotation="10MB", encoding="utf-8", enqueue=True, retention="10 days")


def detect_rank_improvement(start_time, end_time, threshold):
    """
    优化版：通过 SQL 聚合查询，获取最低排名和最终排名，并附带板块名称
    """

    # 查询最低排名和对应板块名称
    query = (ConceptName
             .select(
                 ConceptName.code,
                 ConceptName.name,
                 fn.MAX(ConceptName.rank).alias('lowest_rank'),  # 最低排名
                 ConceptName.timestamp.alias('lowest_timestamp')  # 时间戳
             )
             .where((ConceptName.timestamp >= start_time) & (ConceptName.timestamp <= end_time))
             .group_by(ConceptName.code)
             )

    # 获取最终排名
    final_rank_query = (ConceptName
                        .select(
                            ConceptName.code,
                            fn.MAX(ConceptName.timestamp).alias('final_timestamp'),
                            ConceptName.rank.alias('final_rank')
                        )
                        .where((ConceptName.timestamp >= start_time) & (ConceptName.timestamp <= end_time))
                        .group_by(ConceptName.code))

    # 将最低排名与最终排名结果合并
    rank_changes = {}
    for row in query:
        rank_changes[row.code] = {
            "name": row.name,
            "lowest_rank": row.lowest_rank,
            "lowest_timestamp": row.lowest_timestamp
        }

    for row in final_rank_query:
        if row.code in rank_changes:
            rank_changes[row.code]["final_rank"] = row.final_rank
            rank_changes[row.code]["final_timestamp"] = row.final_timestamp

    # 计算排名提升并筛选
    result = []
    for code, ranks in rank_changes.items():
        if "final_rank" in ranks:  # 确保有最终排名
            rank_improvement = ranks["lowest_rank"] - ranks["final_rank"]
            if rank_improvement > threshold:
                result.append({
                    "code": code,
                    "name": ranks["name"],
                    "rank_improvement": rank_improvement,
                    "initial_rank": ranks["lowest_rank"],
                    "final_rank": ranks["final_rank"],
                })

    return result


def get_latest_concepts_with_criteria(top_n: int = 10):
    """
    从最新插入数据库的一批概念中筛选满足条件的板块：
    1. 涨跌比=1
    2. 涨跌比小于1的前n位，默认n=10
    """
    # 获取最新一批插入的时间戳
    latest_timestamp = (ConceptName
                        .select(fn.MAX(ConceptName.timestamp))
                        .scalar())

    if not latest_timestamp:
        logger.info("数据库中没有概念数据。")
        return []

    # 获取最新批次插入的数据，并按涨跌比降序排序
    latest_concepts = (ConceptName
                       .select()
                       .where(ConceptName.timestamp == latest_timestamp)
                       .order_by(ConceptName.rise_ratio.desc()))

    # 合并筛选逻辑：记录涨跌比=1的和涨跌比<1的前10位
    equal_to_1 = []
    less_than_1 = []

    for concept in latest_concepts:
        if concept.rise_ratio == 1:
            equal_to_1.append(model_to_dict(concept))
        elif concept.rise_ratio < 1 and len(less_than_1) < top_n:
            less_than_1.append(model_to_dict(concept))

        # 当两个条件都满足时可以提前结束循环
        if len(less_than_1) >= top_n:
            break

    # 合并结果
    result = equal_to_1 + less_than_1

    return result


def get_top_n_concepts_excluding(top_n=10, exclude_codes=None):
    """
    获取最新插入的数据，排除指定的概念，返回排名前10的概念。
    """
    # 获取最新一批插入的时间戳
    latest_timestamp = (ConceptName
                        .select(fn.MAX(ConceptName.timestamp))
                        .scalar())

    if not latest_timestamp:
        logger.info("数据库中没有概念数据。")
        return []

    # 获取最新批次插入的数据，按排名升序（越小排名越靠前）
    query = (ConceptName
             .select()
             .where(ConceptName.timestamp == latest_timestamp)
             .order_by(ConceptName.rank.asc()))

    if exclude_codes:
        query = query.where(~ConceptName.code.in_(exclude_codes))

    # 获取前10名（排序后前10）
    top_10_concepts = query.limit(top_n)
    result = [model_to_dict(concept) for concept in top_10_concepts]

    return result


def get_stocks_in_multiple_concepts(top_n: int = 10, min_concept_count: int = 2, exclude_codes=None):
    """
    获取在前 N 名概念中出现至少两个概念板块的个股
    """
    # 获取前 N 名的概念
    top_n_concepts = get_top_n_concepts_excluding(top_n, exclude_codes)

    # 获取这些概念的代码
    concept_codes = [concept.get("code") for concept in top_n_concepts]

    # 从 ConceptCons 中选出这些概念板块下的所有个股
    stocks_in_concepts = (ConceptCons
                          .select(ConceptCons.stock_name, ConceptCons.symbol, ConceptCons.name, ConceptCons.code)
                          .where(ConceptCons.code.in_(concept_codes)))

    # 统计每个股票出现在多少个概念板块中
    stock_counter = Counter(stock.symbol for stock in stocks_in_concepts)

    # # 过滤出出现在至少两个概念板块中的股票
    # multiple_concept_stocks = [stock for stock, count in stock_counter.items() if count >= min_concept_count]

    # # 获取这些个股的详细信息
    # result = (ConceptCons
    #           .select(ConceptCons.symbol, ConceptCons.stock_name, ConceptCons.name, ConceptCons.code)
    #           .where(ConceptCons.symbol.in_(multiple_concept_stocks)))
    # result = [model_to_dict(concept) for concept in result]
    result = []
    for symbol, count in stock_counter.most_common():
        if count >= min_concept_count:
            _concepts_of_stock = (stocks_in_concepts
                                  .select(ConceptCons.name, ConceptCons.stock_name)
                                  .where(ConceptCons.symbol == symbol))
            result.append({
                "symbol": symbol,
                "stock_name": _concepts_of_stock.first().stock_name,
                "count": count,
                "concepts": [c.name for c in _concepts_of_stock],
            })
    return result
