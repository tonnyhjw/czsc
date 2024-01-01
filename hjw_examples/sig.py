from collections import OrderedDict

from czsc import CZSC
from czsc.objects import Direction, ZS
from czsc.utils import get_sub_elements, create_single_signal
from czsc.signals.tas import update_macd_cache
from czsc.utils.sig import get_zs_seq


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
    v1 = '其他'
    cache_key = update_macd_cache(c)
    ubi = c.ubi
    bis = c.bi_list
    if len(bis) < 15 or not ubi or len(ubi['raw_bars']) < 3:
        v1 = 'K线不合标准'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    zs_seq = get_zs_seq(bis)
    if len(zs_seq) < 3:
        v1 = '中枢<3'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    zs1, zs2, zs3 = zs_seq[-3:]
    if not (zs1.zd > zs2.zg or zs2.zd > zs3.zg):
        v1 = '不是下行趋势'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    last_bi = zs3.bis[-1]
    if (zs3.is_valid
            and ubi['direction'] == Direction.Down
            and len(ubi['fxs']) < 3
            and ubi['low'] > zs3.zg
            and last_bi.low < zs3.zd
    ):
        v1, v2 = '多头', '三买'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2)
    if (zs3.is_valid
            and ubi['direction'] == Direction.Down
            and len(ubi['fxs']) > 2
            and ubi['low'] < zs3.dd
    ):
        bi_a = zs2.bis[-1]
        ubi_dif = min(x.cache[cache_key]['dif'] for x in ubi['raw_bars'])
        bi_a_dif = min(x.cache[cache_key]['dif'] for x in bi_a.raw_bars)
        ubi_macd_area = sum(macd for x in ubi['raw_bars'] if (macd := x.cache[cache_key]['macd']) < 0)
        bi_a_macd_area = sum(macd for x in bi_a.raw_bars if (macd := x.cache[cache_key]['macd']) < 0)
        if 0 > ubi_dif > bi_a_dif and abs(ubi_macd_area) < abs(bi_a_macd_area):
            v1, v2 = '多头', '一买'
            return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2)
    return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
