from peewee import fn, SQL
from loguru import logger
from typing import List, Dict, Optional
from collections import Counter
from datetime import datetime, timedelta, date
from playhouse.shortcuts import model_to_dict

from database.models import ConceptName, ConceptCons, BuyPoint, switch_database
from src.concept.utils import merge_concept_stocks


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


def get_latest_concepts_with_criteria(top_n: int = 10, latest_timestamp=None):
    """
    从最新插入数据库的一批概念中筛选满足条件的板块：
    1. 涨跌比=1
    2. 涨跌比小于1的前n位，默认n=10
    """
    # 获取精确时间戳
    precise_timestamp = get_precise_latest_timestamp(ConceptName, latest_timestamp)

    if not precise_timestamp:
        logger.info("数据库中没有概念数据。")
        return []

    # 创建查询
    latest_concepts = (query_by_precise_timestamp(ConceptName, precise_timestamp)
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


# 修改后的第一个函数
def get_top_n_concepts_excluding(top_n=10, exclude_codes=None, latest_timestamp=None):
    """
    获取最新插入的数据，排除指定的概念，返回排名前n的概念。
    """
    # 获取精确时间戳
    precise_timestamp = get_precise_latest_timestamp(ConceptName, latest_timestamp)

    if not precise_timestamp:
        logger.info("数据库中没有概念数据。")
        return []

    # 创建查询
    query = query_by_precise_timestamp(ConceptName, precise_timestamp)
    query = query.order_by(ConceptName.rank.asc())

    # 如果有需要排除的概念代码，添加过滤条件
    if exclude_codes:
        query = query.where(~ConceptName.code.in_(exclude_codes))

    # 获取指定数量的概念
    top_concepts = query.limit(top_n)
    result = [model_to_dict(concept) for concept in top_concepts]

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

    result = []
    for symbol, count in stock_counter.most_common():
        if count >= min_concept_count:
            _concepts_of_stock = (stocks_in_concepts
                                  .select(ConceptCons.stock_name, ConceptCons.name)
                                  .where(ConceptCons.symbol == symbol))
            _concepts = [c.name for c in _concepts_of_stock]
            _stock_name = _concepts_of_stock.first().stock_name
            result.append({
                "symbol": symbol,
                "stock_name": _stock_name,
                "count": count,
                "concepts": _concepts,
            })
    return result


def monitor_concept_rank_drop(
        top_n: int = 10,  # 头部强势板块的初始范围
        rank_threshold: int = 50,  # 排名下降阈值
        avg_rank_window: int = 3,  # 计算平均排名的天数
        latest_timestamp=None  # 可选的精确时间戳
) -> List[Dict]:
    """
    监控概念板块排名变化，识别从强势板块快速下跌的概念

    参数:
    - top_n: 初始强势板块的数量
    - rank_threshold: 排名下降的阈值
    - avg_rank_window: 计算平均排名的窗口期
    - latest_timestamp: 可选的精确时间戳

    返回: 符合条件的概念板块列表
    """
    # 获取精确的最新时间戳
    precise_timestamp = get_precise_latest_timestamp(ConceptName, latest_timestamp)

    if not precise_timestamp:
        logger.info("数据库中没有概念数据。")
        return []

    # 计算平均排名窗口的起始时间
    avg_start_time = precise_timestamp - timedelta(days=avg_rank_window)

    # 获取最新批次数据
    latest_data = (
        query_by_precise_timestamp(ConceptName, precise_timestamp)
    )

    # 计算每个概念板块在平均排名窗口期的平均排名
    avg_ranks = (
        ConceptName
        .select(
            ConceptName.name,
            ConceptName.code,
            fn.AVG(ConceptName.rank).alias('avg_rank')
        )
        .where(ConceptName.timestamp.between(avg_start_time, precise_timestamp))
        .group_by(ConceptName.name, ConceptName.code)
        .order_by(SQL('avg_rank').asc())  # 排名越小越靠前
        .limit(top_n)
    )

    # 转换为字典以便快速查找
    avg_rank_dict = {concept.code: concept.avg_rank for concept in avg_ranks}

    # 筛选结果
    result = []
    for concept in latest_data:
        # 判断是否从强势板块快速下跌
        # 排名阈值改为大于avg_rank_threshold
        # rank越小越靠前，所以排名变大意味着下跌
        if concept.code in avg_rank_dict and concept.rank > rank_threshold:
            result.append({
                'name': concept.name,
                'code': concept.code,
                'current_rank': concept.rank,
                'avg_rank': avg_rank_dict[concept.code],
                'timestamp': concept.timestamp
            })

    return result


def find_concept_stocks_with_latest_buypoints(
        concept_code: str,
        start_date: Optional[datetime.date] = None,
        end_date: Optional[datetime.date] = None,
        database_type: str = "BI"
) -> List[Dict]:
    """
    查找特定概念板块的股票最新买点信息

    Args:
        concept_code (str): 概念板块代码
        start_date (Optional[datetime.date]): 开始日期
        end_date (Optional[datetime.date]): 结束日期
        database_type (str): 数据库类型，默认为 "BI"

    Returns:
        List[Dict]: 带有最新买点信息的股票列表
    """
    # 切换数据库
    switch_database(database_type)

    # 找出该板块的所有成分股
    concept_stocks = (ConceptCons
                      .select(ConceptCons.symbol, ConceptCons.stock_name, ConceptCons.name)
                      .where(ConceptCons.code == concept_code))

    # 如果没有指定日期范围，默认查询最近1年
    if start_date is None:
        start_date = date.today() - timedelta(days=3)
    if end_date is None:
        end_date = date.today()

    # 存储结果的列表
    results = []

    # 遍历每个成分股
    for stock in concept_stocks:
        # 查找该股票在指定日期范围内的最新买点
        latest_buypoint = (BuyPoint
                           .select()
                           .where(
                                (BuyPoint.symbol == stock.symbol) &
                                (BuyPoint.date >= start_date) &
                                (BuyPoint.date <= end_date)
                            )
                           .order_by(BuyPoint.date.desc())
                           .first())  # 只选择最新的买点

        # 如果有买点，则加入结果列表
        if latest_buypoint:
            results.append({
                'ts_code': latest_buypoint.ts_code,
                'stock_name': stock.stock_name,
                'name': stock.name,
                'signals': latest_buypoint.signals,
                'fx_pwr': latest_buypoint.fx_pwr,
                'bp_date': latest_buypoint.date,
            })

    return results


def get_buypoints_for_multiple_concepts(
        concepts: List[Dict],
        start_date: Optional[datetime.date] = None,
        end_date: Optional[datetime.date] = None,
        database_type: str = "BI"
) -> List[Dict]:
    """
    获取多个概念板块的最新买点信息

    Args:
        concepts (List[Dict]): 概念板块列表，每个字典包含 'code' 字段
        start_date (Optional[datetime.date]): 开始日期
        end_date (Optional[datetime.date]): 结束日期
        database_type (str): 数据库类型，默认为 "BI"

    Returns:
        List[Dict]: 所有概念板块的最新买点信息合并列表
    """
    # 存储最终结果的列表
    all_buypoints = []

    # 遍历每个概念板块
    for concept in concepts:
        # 获取当前概念板块的最新买点
        concept_buypoints = find_concept_stocks_with_latest_buypoints(
            concept_code=concept.get('code'),
            start_date=start_date,
            end_date=end_date,
            database_type=database_type
        )

        # 将当前概念板块的买点加入总列表
        all_buypoints.extend(concept_buypoints)
    all_buypoints = merge_concept_stocks(all_buypoints)
    return all_buypoints


def get_precise_latest_timestamp(model, timestamp=None):
    """
    获取精确到分钟的最新时间戳

    :param model: Peewee模型类
    :param timestamp: 可选的自定义时间戳（字符串或datetime对象）
    :return: 精确到分钟的datetime对象，如果没有数据返回None
    """
    # 如果没有提供 timestamp，则获取数据库中最新的时间戳
    if timestamp is None:
        timestamp = (model
                     .select(fn.MAX(model.timestamp))
                     .scalar())

    if not timestamp:
        return None

    # 如果提供的是字符串，转换为datetime对象
    if isinstance(timestamp, str):
        timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M")

    return timestamp


def query_by_precise_timestamp(model, timestamp):
    """
    根据精确到分钟的时间戳创建查询

    :param model: Peewee模型类
    :param timestamp: 精确到分钟的datetime对象
    :return: Peewee查询对象
    """
    return (model
    .select()
    .where(
        (model.timestamp.year == timestamp.year) &
        (model.timestamp.month == timestamp.month) &
        (model.timestamp.day == timestamp.day) &
        (model.timestamp.hour == timestamp.hour) &
        (model.timestamp.minute == timestamp.minute)
    ))
