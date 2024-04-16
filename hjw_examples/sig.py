import datetime
import pprint
from collections import OrderedDict

from czsc import CZSC
from czsc.objects import Direction
from czsc.utils import get_sub_elements, create_single_signal
from czsc.signals.tas import update_macd_cache
from czsc.utils.sig import get_zs_seq
from czsc.enum import Mark


def macd_pzbc_ubi(c: CZSC, **kwargs) -> OrderedDict:
    """盘整背驰，主要针对大级别使用（周以上）

    **信号逻辑：**

    1. 取最后三个笔（含未完成笔）；
    2. 向上则第一笔创新高，否则创新低；
    3. 第一笔macd绝对值大于第二笔macd绝对值；

    主要用于用于探测周、月线盘整背驰

    :param c: CZSC对象
    :param kwargs:
    :return: 信号识别结果
    """
    freq = c.freq.value
    k1, k2, k3 = f"{freq}_MACD背驰_UBI观察V230804".split('_')
    v1 = '其他'
    cache_key = update_macd_cache(c)
    bi_c = c.ubi
    if len(c.bi_list) < 3 or not bi_c or len(bi_c['raw_bars']) < 7:
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    bis = get_sub_elements(c.bi_list, di=1, n=2)
    bi_a, bi_b = bis[-2:]

    if bi_c['direction'] == Direction.Up and bi_c['high'] > bi_a.high:
        bi_c_dif = max(x.cache[cache_key]['dif'] for x in bi_c['raw_bars'])
        bi_a_dif = max(x.cache[cache_key]['dif'] for x in bi_a.raw_bars)
        bi_c_macd_area = sum(macd for x in bi_c['raw_bars'] if (macd := x.cache[cache_key]['macd']) > 0)
        bi_a_macd_area = sum(macd for x in bi_a.raw_bars if (macd := x.cache[cache_key]['macd']) > 0)

        if 0 < bi_c_dif < bi_a_dif and abs(bi_c_macd_area) < abs(bi_a_macd_area):
            v1 = '空头'

    if bi_c['direction'] == Direction.Down and bi_c['low'] < bi_a.low:
        bi_c_dif = min(x.cache[cache_key]['dif'] for x in bi_c['raw_bars'])
        bi_a_dif = min(x.cache[cache_key]['dif'] for x in bi_a.raw_bars)
        bi_c_macd_area = sum(macd for x in bi_c['raw_bars'] if (macd := x.cache[cache_key]['macd']) < 0)
        bi_a_macd_area = sum(macd for x in bi_a.raw_bars if (macd := x.cache[cache_key]['macd']) < 0)
        if 0 > bi_c_dif > bi_a_dif and abs(bi_c_macd_area) < abs(bi_a_macd_area):
            v1 = '多头'

    return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)


def trend_reverse_ubi(c: CZSC, **kwargs) -> OrderedDict:
    """盘整背驰，主要针对大级别使用（周以上）

    **信号逻辑：**

    1. 取最后三个笔（含未完成笔）；
    2. 向上则第一笔创新高，否则创新低；
    3. 第一笔macd绝对值大于第二笔macd绝对值；

    主要用于用于探测周、月线盘整背驰

    :param c: CZSC对象
    :param kwargs:
    :return: 信号识别结果
    """
    freq = c.freq.value
    k1, k2, k3 = f"{freq}_趋势反转_UBI观察V230804".split('_')
    v1 = '无买点'
    cache_key = update_macd_cache(c)
    ubi = c.ubi
    bis = c.bi_list
    latest_fx = c.ubi_fxs[-1]       # 最近一个分型
    latest_fx_dt_delta = datetime.datetime.now() - latest_fx.dt    # 最近一个分型是多久之前？

    if len(bis) < 15 or not ubi or len(ubi['raw_bars']) < 3:
        v1 = 'K线不合标准'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    if latest_fx.mark != Mark.D or abs(latest_fx_dt_delta.days) > 5:
        v1 = '没有底分型'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    zs_seq = get_zs_seq(bis)
    if len(zs_seq) < 3:
        v1 = '中枢<3'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    zs1, zs2, zs3 = zs_seq[-3:]

    cur_price = c.bars_raw[-1].close
    if zs3.is_valid:
        if (
                ubi['direction'] == Direction.Down
                and len(ubi['fxs']) > 2
                and ubi['low'] > zs3.zg
                and zs1.zd > zs2.zg
                and zs2.zd > zs3.zg
        ):
            estimated_profit = (ubi['high'] - cur_price) / cur_price
            v1, v2 = '多头', '三买'
            return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2, v3=estimated_profit)
        if (
                # and zs1.zd > zs2.zg
                ubi['direction'] == Direction.Up
                and len(ubi['fxs']) < 2
                and ubi['low'] < zs3.zd
                and zs2.zd > zs3.zg
        ):
            bi_a, bi_b = zs2.bis[-1], zs3.bis[-1]
            bi_a_dif = min(x.cache[cache_key]['dif'] for x in bi_a.raw_bars)
            bi_b_dif = min(x.cache[cache_key]['dif'] for x in bi_b.raw_bars)

            bi_a_macd_area = sum(macd for x in bi_a.raw_bars if (macd := x.cache[cache_key]['macd']) < 0)
            bi_b_macd_area = sum(macd for x in bi_b.raw_bars if (macd := x.cache[cache_key]['macd']) < 0)

            bi_b_max_macd = max(abs(macd) for x in bi_b.raw_bars if (macd := x.cache[cache_key]['macd']) < 0)
            bi_b_last_macd = bi_b.raw_bars[-1].cache[cache_key]['macd']

            estimated_profit = (zs3.zd - cur_price) / cur_price

            if (
                    0 > bi_b_dif > bi_a_dif
                    and abs(bi_b_macd_area) < abs(bi_a_macd_area)
                    and abs(bi_b_last_macd) < bi_b_max_macd
                    # and bi_b_last_macd < 0
                    and estimated_profit >= 0.03
            ):
                v1, v2 = '多头', '一买'
                return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2, v3=estimated_profit)
    elif zs2.is_valid:
        bi_a = zs1.bis[-1]
        bi_a_dif = min(x.cache[cache_key]['dif'] for x in bi_a.raw_bars)
        bi_a_macd_area = sum(macd for x in bi_a.raw_bars if (macd := x.cache[cache_key]['macd']) < 0)

        bi_c_raw_bars = zs2.bis[-1].raw_bars
        for _bi in zs3.bis:     # 扩展bi_c
            bi_c_raw_bars += _bi.raw_bars
        if ubi['direction'] == Direction.Down:
            bi_c_raw_bars += ubi['raw_bars']
        bi_c_peak_dif = sum(macd for x in bi_c_raw_bars if (macd := x.cache[cache_key]['macd']) < 0)
        bi_c_macd_area = sum(macd for x in bi_c_raw_bars if (macd := x.cache[cache_key]['macd']) < 0)
        bi_c_max_macd = max(abs(macd) for x in bi_c_raw_bars if (macd := x.cache[cache_key]['macd']) < 0)
        bi_c_last_macd = bi_c_raw_bars[-1].cache[cache_key]['macd']
        estimated_profit = (zs2.dd - cur_price) / cur_price
        if (
                0 > bi_c_peak_dif > bi_a_dif
                and abs(bi_c_macd_area) < abs(bi_a_macd_area)
                and abs(bi_c_last_macd) < bi_c_max_macd
                and bi_c_last_macd < 0
                and estimated_profit >= 0.03
                and zs1.zd > zs2.zg
        ):
            v1, v2 = '多头', '一买'
            return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2, v3=estimated_profit)
    return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)


def get_valid_zs_seq(zs_seq: list, valid_count: int = 3):
    # 假设 zs_seq 是 ZS 对象的列表
    index = len(zs_seq)

    # 从列表末尾开始反向遍历
    for zs in reversed(zs_seq):
        index -= 1
        if zs.is_valid:
            valid_count += 1
        if valid_count >= 3:
            break

    # 获取子列表
    return zs_seq[index:]
