from peewee import *
from datetime import datetime
from typing import List, Dict, Optional

from database.models import ConceptName
from src.concept.configs import *


class ConceptHotRank:
    """概念股排名分析器"""

    def __init__(self, exclude_codes: Optional[List[str]] = None):
        """
        初始化分析器

        Args:
            exclude_codes: 需要排除的概念板块代码列表
        """
        self.exclude_codes = exclude_codes or EXCLUDE_CODES

    def analyze_top_concepts(
            self,
            start_date: datetime,
            end_date: datetime,
            rank_threshold: int,
            top_n: int
    ) -> List[Dict[str, any]]:
        """
        分析指定时间范围内排名达到阈值的概念板块

        Args:
            start_date: 开始日期
            end_date: 结束日期
            rank_threshold: 排名阈值（小于等于此值视为达标）
            top_n: 返回排名次数最多的前N个概念

        Returns:
            List[Dict[str, any]]: 返回字典列表，每个字典包含概念的详细信息
                                {
                                    'name': str,     # 概念名称
                                    'code': str,     # 概念代码
                                    'count': int,    # 达标次数
                                    'rank': int      # 排名序号
                                }
        """
        # 构建查询
        query = (ConceptName
                 .select(
            ConceptName.name,
            ConceptName.code,
            fn.COUNT(ConceptName.id).alias('count')
        )
                 .where(
            (ConceptName.timestamp.between(start_date, end_date)) &
            (ConceptName.rank <= rank_threshold) &
            ~(ConceptName.code << self.exclude_codes)  # 排除指定的概念板块
        )
                 .group_by(ConceptName.code, ConceptName.name)
                 .order_by(fn.COUNT(ConceptName.id).desc())
                 .limit(top_n))

        # 将查询结果转换为字典列表
        return [
            {
                'name': row.name,
                'code': row.code,
                'count': row.count,
                'sequence_number': idx + 1  # 添加排名序号
            }
            for idx, row in enumerate(query)
        ]

    def print_results(self, results: List[Dict[str, any]]) -> None:
        """
        打印分析结果

        Args:
            results: analyze_top_concepts返回的结果列表
        """
        print("\n概念板块排名统计结果:")
        print("-" * 50)
        print(f"{'排名':<6}{'概念名称':<20}{'代码':<10}{'达标次数':<8}")
        print("-" * 50)

        for result in results:
            print(
                f"{result['sequence_number']:<6}"
                f"{result['name']:<20}"
                f"{result['code']:<10}"
                f"{result['count']:<8}"
            )


