import pprint
import gc
import datetime
from loguru import logger
from itertools import chain
from collections import OrderedDict
from typing import List, Any, Dict, Union, Tuple

from czsc import CZSC
from czsc.objects import Direction, FX, BI, ZS
from czsc.utils import create_single_signal
from czsc.signals.tas import update_macd_cache
from czsc.utils.sig import get_zs_seq
from czsc.enum import Mark
from database import history
from src.xd.analyze_by_break import analyze_xd
from src.objects import XD, XDZS


logger.add("statics/logs/sig_xd.log", level="INFO", rotation="10MB", encoding="utf-8", enqueue=True, retention="10 days")


def macd_pzbc_ubi(c: CZSC, fx_dt_limit: int = 30, **kwargs) -> OrderedDict:
    """盘整背驰，主要针对大级别使用（周以上）

    **信号逻辑：**

    1. 取最后三个笔（含未完成笔）；
    2. 向上则第一笔创新高，否则创新低；
    3. 第一笔macd绝对值大于第二笔macd绝对值；

    主要用于用于探测周、月线盘整背驰

    :param c: CZSC对象
    :param fx_dt_limit: int, 分型时效性限制
    :param kwargs:
    :return: 信号识别结果
    """
    freq = c.freq.value
    v1 = '其他'
    edt = kwargs.get('edt', datetime.datetime.now())
    name, ts_code, symbol = kwargs.get('name'), kwargs.get('ts_code'), kwargs.get('symbol')
    k1, k2, k3 = freq, symbol, edt.strftime("%Y%m%d")
    industry, freq = kwargs.get('industry'), kwargs.get('freq')

    cache_key = update_macd_cache(c)
    ubi = c.ubi
    bis = c.bi_list
    cur_price = c.bars_raw[-1].close
    latest_fx = c.ubi_fxs[-1]  # 最近一个分型
    fx_is_exceed = date_exceed_rawbars(c.bars_raw, latest_fx.dt, fx_dt_limit)

    if len(bis) < 4 or not ubi or len(ubi['raw_bars']) < 3:
        v1 = 'K线不合标准'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    if latest_fx.mark != Mark.D or fx_is_exceed:
        v1 = '没有底分型'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    elif history.buy_point_exists(symbol, latest_fx.dt, freq):
        v1 = '已存在'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    else:
        v2 = latest_fx.power_str

    # zs_seq = get_zs_seq(bis)
    # if len(zs_seq) == 0:
    #     v1 = '无中枢'
    #     return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    #
    # zs1 = zs_seq[-1]
    # # 当最后的中枢少于3笔，就将最后的中枢和倒数第二个中枢合并再计算
    # if not zs1.is_valid and zs1.edir == Direction.Down and len(zs_seq) > 1:
    #     zs1 = ZS(zs_seq[-2].bis + zs1.bis)
    # # 查找 BI.high 等于 zs2 的 gg 那一笔，并切片
    # bi_a_index = next((i for i, bi in enumerate(zs1.bis) if bi.high == zs1.gg and bi.direction == Direction.Down), None)
    # remaining_bis = zs1.bis[bi_a_index:]

    remaining_bis = select_pzbc_bis(bis)
    if not remaining_bis:
        v1 = '无中枢'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)

    zs2 = ZS(remaining_bis)
    estimated_profit = (zs2.zd - cur_price) / cur_price

    bi_a = zs2.bis[0]
    bi_b = zs2.bis[-1]
    bi_a_dif = min(x.cache[cache_key]['dif'] for x in bi_a.raw_bars)
    bi_b_dif = min(x.cache[cache_key]['dif'] for x in bi_b.raw_bars)

    bi_a_macd_area = sum(macd for x in bi_a.raw_bars if (macd := x.cache[cache_key]['macd']) < 0)
    bi_b_macd_area = sum(macd for x in bi_b.raw_bars if (macd := x.cache[cache_key]['macd']) < 0)

    pzbc_conditions = (
        (zs2.is_valid, "zs2.is_valid"),
        (ubi['direction'] == Direction.Up, "ubi['direction'] == Direction.Up"),
        (len(ubi['fxs']) < 2, "len(ubi['fxs']) < 2"),
        (zs2.sdir == Direction.Down, "zs2.sdir == Direction.Down"),
        (zs2.edir == Direction.Down, "zs2.edir == Direction.Down"),
        (zs2.dd == bi_b.low, "zs2.dd == bi_b.low"),
        (0 > bi_b_dif > bi_a_dif or abs(bi_a_macd_area) > abs(bi_b_macd_area), "0 > bi_b_dif > bi_a_dif or abs(bi_a_macd_area) > abs(bi_b_macd_area)")
    )
    failed_pzbc_conditions = select_failed_conditions(pzbc_conditions)

    if not failed_pzbc_conditions:
        v1 = '一买'
        # 插入数据库
        history.insert_buy_point(name, symbol, ts_code, freq, v1, latest_fx.power_str, estimated_profit,
                                 industry, latest_fx.dt)
        if v2 != '弱':

            return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2, v3=estimated_profit)
    else:
        logger.info(f"{name}{symbol}盘整背驰不成立原因: {failed_pzbc_conditions}")
    return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)


def trend_reverse_ubi(c: CZSC, fx_dt_limit: int = 5, **kwargs) -> OrderedDict:
    """盘整背驰，主要针对大级别使用（周以上）

    **信号逻辑：**

    1. 取最后三个笔（含未完成笔）；
    2. 向上则第一笔创新高，否则创新低；
    3. 第一笔macd绝对值大于第二笔macd绝对值；

    主要用于用于探测周、月线盘整背驰

    :param c: CZSC对象
    :param fx_dt_limit: int, 分型时效性限制
    :param kwargs:
    :return: 信号识别结果
    """
    freq = c.freq.value
    v1 = '其他'
    edt = kwargs.get('edt', datetime.datetime.now())
    name, ts_code, symbol = kwargs.get('name'), kwargs.get('ts_code'), kwargs.get('symbol')
    k1, k2, k3 = freq, symbol, edt.strftime("%Y%m%d")
    industry, freq = kwargs.get('industry'), kwargs.get('freq')

    cache_key = update_macd_cache(c)
    ubi = c.ubi
    bis = c.bi_list
    cur_price = c.bars_raw[-1].close
    latest_fx = c.ubi_fxs[-1]       # 最近一个分型
    fx_is_exceed = date_exceed_rawbars(c.bars_raw, latest_fx.dt, fx_dt_limit)

    if len(bis) < 4 or not ubi or len(ubi['raw_bars']) < 3:
        v1 = 'K线不合标准'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    if latest_fx.mark != Mark.D or fx_is_exceed:
        v1 = '没有底分型'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    elif history.buy_point_exists(symbol, latest_fx.dt, freq):
        v1 = '已存在'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    else:
        v2 = latest_fx.power_str

    xds = analyze_xd(bis)

    zs_seq = get_xd_zs_seq(xds)
    if len(zs_seq) < 3:
        v1 = '中枢<3'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)

    zs1, zs2, zs3 = zs_seq[-3:]
    estimated_profit = (zs3.zd - cur_price) / cur_price

    # 是否一买
    if zs3.is_valid:
        xd_a, xd_b = zs2.xds[-1], zs3.xds[-1]
        xd_a_bis, xd_b_bis = xd_a.bis, xd_b.bis
        xd_a_raw_bars = list(chain.from_iterable(bi.raw_bars for bi in xd_a_bis))
        xd_b_raw_bars = list(chain.from_iterable(bi.raw_bars for bi in xd_b_bis))
        xd_a_dif = min(x.cache[cache_key]['dif'] for x in xd_a_raw_bars)
        xd_b_dif = min(x.cache[cache_key]['dif'] for x in xd_b_raw_bars)

        xd_a_macd_area = sum(macd for x in xd_a_raw_bars if (macd := x.cache[cache_key]['macd']) < 0)
        xd_b_macd_area = sum(macd for x in xd_b_raw_bars if (macd := x.cache[cache_key]['macd']) < 0)

        trend_bc_conditions = (
            (ubi['direction'] == Direction.Up, "ubi['direction'] == Direction.Up"),
            (len(ubi['fxs']) < 2, "len(ubi['fxs']) < 2"),
            (ubi['low'] < zs3.zd, "ubi['low'] < zs3.zd"),
            (zs2.zd > zs3.zg, "zs2.zd > zs3.zg"),
            (0 > xd_b_dif > xd_a_dif, f"{xd_b_dif=} <= {xd_a_dif=}"),
            (abs(xd_b_macd_area) < abs(xd_a_macd_area), f"{abs(xd_b_macd_area)=} >= {abs(xd_a_macd_area)=}"),
            (estimated_profit >= 0.03, "estimated_profit >= 0.03"),
            (xd_b.low == zs3.dd, "xd_b.low == zs3.dd")
        )
        failed_trend_bc_conditions = select_failed_conditions(trend_bc_conditions)
        if not failed_trend_bc_conditions:
            # 无条件不通过则判断为一买
            v1 = '一买'
            # 插入数据库
            history.insert_buy_point(name, symbol, ts_code, freq, v1, latest_fx.power_str, estimated_profit,
                                     industry, latest_fx.dt)
            if v2 == '强':
                return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2, v3=estimated_profit)
        else:
            logger.info(f"{name}{symbol}一买不成立原因: {failed_trend_bc_conditions}")

    # 30 * N天内是否有过一买且向上笔, 存在一买则检测二三买
    if history.check_duplicate(symbol, edt, days=30 * 6, signals='一买'):
        latest_1st_buy_point = history.query_latest_buy_point(symbol, signals='一买')

        # 提取一买后的bi_list
        bis_after_1st_buy = [bi for bi in bis if bi.sdt.date() >= latest_1st_buy_point.date.date()]
        xds_after_1st_buy = analyze_xd(bis_after_1st_buy)
        zs_seq_after_1st_buy = get_xd_zs_seq(xds_after_1st_buy)
        is_lower_freq_pzbc = detect_lower_freq_pzbc(bis_after_1st_buy, cache_key)

        bis_pzbc_conditions = (
            (0 < len(zs_seq_after_1st_buy) < 3, "0 < len(zs_seq_after_1st_buy) < 3"),
            (ubi['direction'] == Direction.Up, "ubi['direction'] == Direction.Up"),
            (len(ubi['fxs']) < 2, "len(ubi['fxs']) < 2"),
            (is_lower_freq_pzbc, "is_lower_freq_pzbc"),
            # (raw_bar_increase_within_limit(latest_fx.raw_bars, 0.08), "raw_bar_increase_within_limit")
        )
        failed_bis_pzbc_conditions = select_failed_conditions(bis_pzbc_conditions)

        if not failed_bis_pzbc_conditions:
            zs1_after_1st_buy = zs_seq_after_1st_buy[0]
            # 判断二买
            if (
                    latest_fx.low < zs1_after_1st_buy.zg
                    and len(zs_seq_after_1st_buy) == 1
                    and len(zs1_after_1st_buy.xds) >= 2
            ):
                v1 = '二买'
                # 插入数据库
                history.insert_buy_point(name, symbol, ts_code, freq, v1, latest_fx.power_str, estimated_profit,
                                         industry, latest_fx.dt)
                if v2 != '弱':
                    return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2, v3=estimated_profit)

            # 判断三买
            zs2_after_1st_buy = zs_seq_after_1st_buy[1]
            if (latest_fx.low > zs1_after_1st_buy.zg and
                    len(zs_seq_after_1st_buy) == 2 and
                    len(zs2_after_1st_buy.xds) == 1):
                v1 = '三买'
                # 插入数据库
                history.insert_buy_point(name, symbol, ts_code, freq, v1, latest_fx.power_str, estimated_profit,
                                         industry, latest_fx.dt)
                # if v2 != '弱':
                return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2, v3=estimated_profit)
        else:
            logger.info(f"{name}{symbol}二三买不成立原因: {failed_bis_pzbc_conditions}")
    del c
    del xds
    del zs_seq
    # del bis_after_1st_buy
    # del xds_after_1st_buy
    # del zs_seq_after_1st_buy
    # del zs1_after_1st_buy
    # del zs2_after_1st_buy
    gc.collect()

    return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)


def date_exceed_rawbars(bars_raw, fx_dt: datetime, lookback_bars: int = 5) -> bool:
    """
    检查特定日期的 RawBar 是否距离今天超过指定数量的 RawBar。

    :param bars_raw: raw_bars列表
    :param edt: 目标日期
    :param fx_dt: 分型日期
    :param lookback_bars: 要检查的 RawBar 数量，默认为5
    :return: 如果超过指定数量的 RawBar 则返回 True，否则返回 False
    """

    # 找到今天和目标日期的索引
    n = len(bars_raw)
    edt_index = n-1
    fx_dt_index = None

    for i in range(edt_index, -1, -1):
        bar = bars_raw[i]
        if bar.dt.to_pydatetime().date() == fx_dt.date() and fx_dt_index is None:
            fx_dt_index = i
        # 如果两个索引都找到了，可以提前结束遍历
        if fx_dt_index is not None:
            break

    # 检查是否找到对应的索引
    if edt_index is None or fx_dt_index is None:
        raise ValueError(f"Could not find the RawBar for today or the target date. {edt_index=} {fx_dt_index=}")

    # 计算索引差异
    index_difference = edt_index - fx_dt_index

    return index_difference > lookback_bars


def select_pzbc_bis(bis):
    zs_seq = get_zs_seq(bis)
    if len(zs_seq) == 0:
        return None
    zs1 = zs_seq[-1]
    # 当最后的中枢少于3笔，就将最后的中枢和倒数第二个中枢合并再计算
    if not zs1.is_valid and zs1.edir == Direction.Down and len(zs_seq) > 1:
        zs1 = ZS(zs_seq[-2].bis + zs1.bis)
    # 查找 BI.high 等于 zs2 的 gg 那一笔，并切片
    bi_a_index = next((i for i, bi in enumerate(zs1.bis) if bi.high == zs1.gg and bi.direction == Direction.Down), None)
    remaining_bis = zs1.bis[bi_a_index:]
    return remaining_bis


def detect_lower_freq_pzbc(bis, cache_key):
    """
    检查次级别的盘整背驰。

    :param bis: bi列表
    :param cache_key: cache参数
    :return: 是否疑似次级别盘整底背驰， True或False
    """
    remaining_bis = select_pzbc_bis(bis)
    if not remaining_bis:
        return None

    zs = ZS(remaining_bis)
    bi_a = zs.bis[0]
    bi_b = zs.bis[-1]
    remaining_raw_bars = list(chain.from_iterable(bi.raw_bars for bi in remaining_bis))
    max_abs_dea = max(abs(x.cache[cache_key]['dea']) for x in remaining_raw_bars)
    latest_dea = remaining_raw_bars[-1].cache[cache_key]['dea']
    latest_dif = remaining_raw_bars[-1].cache[cache_key]['dif']

    if (
            zs.is_valid and
            zs.sdir == Direction.Down and
            zs.edir == Direction.Down and
            # zs.dd == bi_b.low and
            (abs(latest_dea) <= 0.5 * max_abs_dea or latest_dif >= latest_dea)
    ):
        return True
    else:
        return False


def raw_bar_increase_within_limit(raw_bars, percentage=0.05):
    begin_price = min(raw_bars[-1].open, raw_bars[-2].close)
    end_price = raw_bars[-1].high
    change = (end_price - begin_price) / end_price
    return change <= percentage


def select_failed_conditions(conditions):
    return [desc for cond, desc in conditions if not cond]


def get_xd_zs_seq(xds: List[XD]) -> List[XDZS]:
    zs_list = []
    if not xds:
        return []

    for xd in xds:
        if not zs_list:
            zs_list.append(XDZS(xds=[xd], bis=xd.bis))
            continue

        zs = zs_list[-1]
        if not zs.xds:
            zs.xds.append(xd)
            zs.bis += xd.bis
            zs_list[-1] = zs
        else:
            if (xd.direction == Direction.Up and xd.high < zs.zd) or (
                xd.direction == Direction.Down and xd.low > zs.zg
            ):
                zs_list.append(XDZS(xds=[xd], bis=xd.bis))
            else:
                zs.xds.append(xd)
                zs.bis += xd.bis
                zs_list[-1] = zs

    return zs_list
