import datetime
import pprint
from typing import List

from src.objects import XD, BI, Direction, has_gap


def bi_has_gap(bi1: BI, bi2: BI) -> bool:
    """检查两笔之间是否有缺口"""
    if bi1.direction == Direction.Down:
        return bi2.low > bi1.high
    else:
        return bi2.high < bi1.low


def is_broken_by_bi(xd: XD, bi: BI) -> bool:
    """检查线段是否被笔破坏"""
    if xd.direction == Direction.Up:
        return bi.low <= xd.bis[-2].high
    else:
        return bi.high >= xd.bis[-2].low


def is_broken_by_xd(xd: XD, bi_index: int, bis: List[BI]):
    """检查线段是否被另一个线段破坏，当线段被笔破坏的同时，破坏线段的笔还能线段反向线段就说明破坏成功"""
    new_bi = bis[bi_index]
    if new_bi.direction == xd.direction or bi_index >= len(bis) - 2:
        # 与线段同向的笔不可能破坏线段，最后两笔不够组成线段破坏线段
        return False
    if is_broken_by_bi(xd, new_bi) and not has_gap(new_bi, bis[bi_index + 2]):
        return True
    return False


def can_start_xd(bi_index: int, bis: List[BI]):
    new_bi = bis[bi_index]
    next_same_dir_bi = bis[bi_index + 2]
    if new_bi.direction == Direction.Up and next_same_dir_bi.high >= new_bi.high:
        return True
    elif new_bi.direction == Direction.Down and new_bi.low >= next_same_dir_bi.low:
        return True
    return False


def analyze_xd(bis: List[BI]) -> List[XD]:
    xds = []
    current_xd = None

    for bi_index in range(len(bis)):
        bi = bis[bi_index]

        if current_xd is None:
            if can_start_xd(bi_index, bis):
                current_xd = XD(bis=[bi], start_bi=bi, start_bi_index=bi_index)
        else:
            if current_xd.direction == bi.direction:
                current_xd.bis.append(bi)
            else:
                if len(current_xd.bis) > 2 and is_broken_by_xd(current_xd, bi_index, bis):
                    current_xd.end_bi = bis[bi_index - 1]
                    current_xd.end_bi_index = bi_index - 1
                    xds.append(current_xd)

                    current_xd = XD(bis=[bi], start_bi=bi, start_bi_index=bi_index)
                else:
                    current_xd.bis.append(bi)

    if current_xd and current_xd.end_bi is None and len(current_xd.bis) >= 3:
        current_xd.end_bi_index = len(bis) - 1

        if current_xd.bis[-1].direction != current_xd.direction:
            current_xd.bis.pop()
            current_xd.end_bi_index -= 1
        current_xd.end_bi = current_xd.bis[-1]
        xds.append(current_xd)

    return xds

