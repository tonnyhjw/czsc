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
    bis: List[BI]
    start_bi: BI
    start_bi_index: int
    end_bi: Optional[BI] = None
    end_bi_index: Optional[int] = None
    start_fx: Optional[TZFX] = None     # 起始的分型
    end_fx: Optional[TZFX] = None     # 结束的分型

    def __post_init__(self):
        self.symbol = self.bis[0].symbol

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

    @property
    def high(self):
        """线段最高点"""
        return max([x.high for x in self.bis])

    @property
    def low(self):
        """线段最低点"""
        return min([x.low for x in self.bis])




@dataclass
class FeatureSequence:
    xd_direction: Direction
    elem_direction: Direction
    sequence: List[FeatureElement] = field(default_factory=list)
    last_tzfx: Optional[TZFX] = None


@dataclass
class XDZS:
    """中枢对象，主要用于辅助信号函数计算"""
    xds: List[XD]
    bis: List[BI]
    cache: dict = field(default_factory=dict)  # cache 用户缓存

    def __post_init__(self):
        self.symbol = self.bis[0].symbol

    @property
    def zg(self):
        """中枢上沿"""
        return min([x.high for x in self.xds[:3]])

    @property
    def zd(self):
        """中枢下沿"""
        return max([x.low for x in self.xds[:3]])

    @property
    def sdt(self):
        """中枢开始时间"""
        return self.bis[0].sdt

    @property
    def edt(self):
        """中枢结束时间"""
        return self.bis[-1].edt

    @property
    def sdir(self):
        """中枢第一笔方向，sdir 是 start direction 的缩写"""
        return self.bis[0].direction

    @property
    def edir(self):
        """中枢倒一笔方向，edir 是 end direction 的缩写"""
        return self.bis[-1].direction

    @property
    def zz(self):
        """中枢中轴"""
        return self.zd + (self.zg - self.zd) / 2

    @property
    def gg(self):
        """中枢最高点"""
        return max([x.high for x in self.bis])

    @property
    def dd(self):
        """中枢最低点"""
        return min([x.low for x in self.bis])


    @property
    def is_valid(self):
        """中枢是否有效"""
        if self.zg < self.zd or len(self.xds) < 3:
            return False

        for xd in self.xds:
            # 中枢内的笔必须与中枢的上下沿有交集
            if (
                self.zg >= xd.high >= self.zd
                or self.zg >= xd.low >= self.zd
                or xd.high >= self.zg > self.zd >= xd.low
            ):
                continue
            else:
                return False

        return True

    def __repr__(self):
        return (
            f"ZS(sdt={self.sdt}, sdir={self.sdir}, edt={self.edt}, edir={self.edir}, "
            f"len_bis={len(self.bis)}, zg={self.zg}, zd={self.zd}, "
            f"gg={self.gg}, dd={self.dd}, zz={self.zz})"
        )