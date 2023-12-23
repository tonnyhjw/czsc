# -*- coding: utf-8 -*-
"""
author: hjw
email: xxx@163.com
create_dt: 2023/12/21 19:29
describe:
"""
import numpy as np
import pandas as pd
from typing import List
from czsc import CZSC
from czsc.traders.base import CzscSignals
from czsc.objects import FX, BI, Direction, ZS, Mark
from czsc.utils import get_sub_elements, create_single_signal
from czsc.utils.sig import get_zs_seq
from czsc.signals.tas import update_ma_cache, update_macd_cache
from collections import OrderedDict


def pzbc_macd_bc_V231221(c: CZSC, **kwargs):
    """MACD盘整背驰辅助

    参数模板："{freq}_D{di}N{n}M{m}#MACD{fastperiod}#{slowperiod}#{signalperiod}_BCV221201"

    **信号逻辑：**

    1. 近n个最低价创近m个周期新低（以收盘价为准），macd柱子不创新低，这是底部背驰信号
    2. 若底背驰信号出现时 macd 为红柱，相当于进一步确认
    3. 顶部背驰反之

    **信号列表：**

    - Signal('15分钟_D1N3M50#MACD12#26#9_BCV221201_底部_绿柱_任意_0')
    - Signal('15分钟_D1N3M50#MACD12#26#9_BCV221201_底部_红柱_任意_0')
    - Signal('15分钟_D1N3M50#MACD12#26#9_BCV221201_顶部_红柱_任意_0')
    - Signal('15分钟_D1N3M50#MACD12#26#9_BCV221201_顶部_绿柱_任意_0')

    :param c: CZSC对象
    :param di: 倒数第i根K线
    :param n: 近期窗口大小
    :param m: 远期窗口大小
    :return: 信号识别结果
    """

    di = int(kwargs.get('di', 1))
    n = int(kwargs.get('n', 3))
    m = int(kwargs.get('m', 50))

    fastperiod = int(kwargs.get('fastperiod', 12))
    slowperiod = int(kwargs.get('slowperiod', 26))
    signalperiod = int(kwargs.get('signalperiod', 9))

    cache_key = update_macd_cache(c, **kwargs)
    k1, k2, k3 = f"{c.freq.value}_D{di}N{n}M{m}#MACD{fastperiod}#{slowperiod}#{signalperiod}_BCV221201".split('_')
    bars = get_sub_elements(c.bars_raw, di=di, n=n + m)
    for bi in c.bi_list:
        print(bi)
    assert n >= 3, "近期窗口大小至少要大于3"

    v1 = "其他"
    v2 = "任意"
    if len(bars) == n + m:
        n_bars = bars[-n:]
        m_bars = bars[:m]
        assert len(n_bars) == n and len(m_bars) == m
        n_close = [x.close for x in n_bars]
        n_macd = [x.cache[cache_key]['macd'] for x in n_bars]
        m_close = [x.close for x in m_bars]
        m_macd = [x.cache[cache_key]['macd'] for x in m_bars]

        if n_macd[-1] > n_macd[-2] and min(n_close) < min(m_close) and min(n_macd) > min(m_macd):
            v1 = '底部'
        elif n_macd[-1] < n_macd[-2] and max(n_close) > max(m_close) and max(n_macd) < max(m_macd):
            v1 = '顶部'

        v2 = "红柱" if n_macd[-1] > 0 else "绿柱"

    return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2)