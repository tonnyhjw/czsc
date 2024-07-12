from dataclasses import dataclass, field
from typing import List, Optional


from czsc.objects import *


def has_gap(elem_1, elem_2):
    return elem_1.low > elem_2.high or elem_2.low > elem_1.high


@dataclass
class FeatureElement:
    bi: BI
    symbol: str
    high: float
    low: float
    direction: Direction
    bi_index: int

    def __init__(self, bi: BI, bi_index):
        self.bi = bi
        self.symbol = bi.symbol
        self.high = bi.high
        self.low = bi.low
        self.direction = bi.direction
        self.bi_index = bi_index


@dataclass
class TZFX:
    symbol: str
    dt: datetime
    mark: Mark
    high: float
    low: float
    fx: float
    fx_bi_index: int
    elements: List[FeatureElement] = field(default_factory=list)

    def has_gap(self) -> bool:
        """判断两个特征元素之间是否存在缺口"""
        # return self.elements[0].low > self.elements[1].high or self.elements[1].low > self.elements[2].high
        return has_gap(self.elements[0], self.elements[1])


@dataclass
class XD:
    symbol: str
    bis: List[BI]
    start_bi: BI
    start_bi_index: int
    end_bi: Optional[BI] = None
    end_bi_index: Optional[int] = None
    start_fx: Optional[TZFX] = None     # 起始的分型
    end_fx: Optional[TZFX] = None     # 结束的分型

    @property
    def direction(self) -> Direction:
        return self.start_bi.direction

    @property
    def is_valid(self) -> bool:
        # 实现线段有效性检查逻辑
        return (self.start_bi is not None and
                self.end_bi is not None and
                self.end_bi_index is not None and
                len(self.bis) >= 3 and
                not has_gap(self.bis[0], self.bis[2]))


@dataclass
class FeatureSequence:
    xd_direction: Direction
    elem_direction: Direction
    sequence: List[FeatureElement] = field(default_factory=list)
    last_tzfx: Optional[TZFX] = None
