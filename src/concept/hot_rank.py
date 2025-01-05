from peewee import *
from datetime import datetime
from typing import List, Dict, Optional
from enum import Enum

from database.models import ConceptName
from src.concept.configs import *


class RankType(Enum):
    """排名分析类型"""
    TOP = 'top'  # 分析排名靠前的概念
    BOTTOM = 'bottom'  # 分析排名靠后的概念


class ConceptHotRank:
    """概念股排名分析器"""

    def __init__(self, exclude_codes: Optional[List[str]] = None):
        """
        初始化分析器

        Args:
            exclude_codes: 需要排除的概念板块代码列表
        """
        self.exclude_codes = exclude_codes or EXCLUDE_CODES

    def analyze_concepts(
            self,
            start_date: datetime,
            end_date: datetime,
            rank_threshold: int,
            limit_n: int,
            rank_type: RankType
    ) -> List[Dict[str, any]]:
        """
        分析指定时间范围内的概念板块排名

        Args:
            start_date: 开始日期
            end_date: 结束日期
            rank_threshold: 排名阈值
            limit_n: 返回的概念数量
            rank_type: 分析类型（TOP表示分析排名高于阈值的，BOTTOM表示分析排名低于阈值的）

        Returns:
            List[Dict[str, any]]: 返回字典列表，每个字典包含概念的详细信息
        """
        # 根据分析类型确定查询条件和排序方式
        if rank_type == RankType.TOP:
            rank_condition = ConceptName.rank <= rank_threshold
            order_expression = fn.COUNT(ConceptName.id).desc()
        else:
            rank_condition = ConceptName.rank >= rank_threshold
            order_expression = fn.COUNT(ConceptName.id).desc()  # 仍按次数降序，因为我们要找出频繁出现在末尾的概念

        # 构建查询
        query = (ConceptName
                 .select(
            ConceptName.name,
            ConceptName.code,
            fn.COUNT(ConceptName.id).alias('count')
        )
                 .where(
            (ConceptName.timestamp.between(start_date, end_date)) &
            rank_condition &
            ~(ConceptName.code << self.exclude_codes)
        )
                 .group_by(ConceptName.code, ConceptName.name)
                 .order_by(order_expression)
                 .limit(limit_n))

        # 将查询结果转换为字典列表
        return [
            {
                'name': row.name,
                'code': row.code,
                'count': row.count,
                'sequence_number': idx + 1
            }
            for idx, row in enumerate(query)
        ]

    def analyze_top_concepts(
            self,
            start_date: datetime,
            end_date: datetime,
            rank_threshold: int,
            top_n: int
    ) -> List[Dict[str, any]]:
        """分析排名靠前的概念（向下兼容的方法）"""
        return self.analyze_concepts(
            start_date,
            end_date,
            rank_threshold,
            top_n,
            RankType.TOP
        )

    def analyze_bottom_concepts(
            self,
            start_date: datetime,
            end_date: datetime,
            rank_threshold: int,
            bottom_n: int
    ) -> List[Dict[str, any]]:
        """分析排名靠后的概念"""
        return self.analyze_concepts(
            start_date,
            end_date,
            rank_threshold,
            bottom_n,
            RankType.BOTTOM
        )

    def print_results(self, results: List[Dict[str, any]], rank_type: RankType) -> None:
        """
        打印分析结果

        Args:
            results: analyze_concepts返回的结果列表
            rank_type: 分析类型
        """
        type_desc = "领先" if rank_type == RankType.TOP else "落后"
        print(f"\n概念板块{type_desc}统计结果:")
        print("-" * 50)
        print(f"{'序号':<6}{'概念名称':<20}{'代码':<10}{'达标次数':<8}")
        print("-" * 50)

        for result in results:
            print(
                f"{result['sequence_number']:<6}"
                f"{result['name']:<20}"
                f"{result['code']:<10}"
                f"{result['count']:<8}"
            )
