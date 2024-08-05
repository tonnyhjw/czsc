import pprint
import gc
import datetime
from loguru import logger
from itertools import chain
from collections import OrderedDict

from czsc import CZSC
from czsc.objects import Direction, FX, BI, ZS
from czsc.utils import get_sub_elements, create_single_signal
from czsc.signals.tas import update_macd_cache
from czsc.utils.sig import get_zs_seq
from czsc.enum import Mark
from database import history
from src.sig.utils import *
from src.xd.analyze_by_break import analyze_xd
from src.sig.powers import ma_is_up

logger.add("statics/logs/trend_reverse.log", level="INFO", rotation="10MB", encoding="utf-8", enqueue=True, retention="10 days")


def trend_reverse_bi(c: CZSC, fx_dt_limit: int = 5, **kwargs) -> OrderedDict:
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
    db = kwargs.get("db", "BI")
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
    elif history.buy_point_exists(symbol, latest_fx.dt, freq, db=db):
        v1 = '已存在'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    else:
        v2 = latest_fx.power_str

    zs_seq = get_zs_seq(bis)
    if len(zs_seq) < 3:
        v1 = '中枢<3'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)

    zs1, zs2, zs3 = zs_seq[-3:]
    estimated_profit = (zs3.zd - cur_price) / cur_price

    # 是否一买
    if zs3.is_valid:
        bi_a, bi_b = zs2.bis[-1], zs3.bis[-1]
        bi_a_dif = min(x.cache[cache_key]['dif'] for x in bi_a.raw_bars)
        bi_b_dif = min(x.cache[cache_key]['dif'] for x in bi_b.raw_bars)

        bi_a_macd_area = sum(macd for x in bi_a.raw_bars if (macd := x.cache[cache_key]['macd']) < 0)
        bi_b_macd_area = sum(macd for x in bi_b.raw_bars if (macd := x.cache[cache_key]['macd']) < 0)

        trend_bc_conditions = (
            (ubi['direction'] == Direction.Up, "ubi['direction'] == Direction.Up"),
            (len(ubi['fxs']) < 2, f"{len(ubi['fxs'])=} < 2"),
            (ubi['low'] < zs3.zd, f"{ubi['low']=} < {zs3.zd=}"),
            (zs2.zd > zs3.zg, f"{zs2.zd=} > {zs3.zg=}"),
            (0 > bi_b_dif > bi_a_dif, f"{bi_b_dif=} <= {bi_a_dif=}"),
            (abs(bi_b_macd_area) < abs(bi_a_macd_area), f"{abs(bi_b_macd_area)=} >= {abs(bi_a_macd_area)=}"),
            (estimated_profit >= 0.03, f"{estimated_profit=} >= 0.03"),
            (bi_b.low == zs3.dd, f"{bi_b.low=} == zs3.dd"),
            (abs(bi_b.change) >= 0.7 * abs(bi_a.change), f"{bi_b.change=} >= 70% * {bi_a.change=}")
        )
        failed_trend_bc_conditions = select_failed_conditions(trend_bc_conditions)
        if not failed_trend_bc_conditions:
            # 无条件不通过则判断为一买
            v1 = '一买'
            # 插入数据库
            history.insert_buy_point(name, symbol, ts_code, freq, v1, latest_fx.power_str, estimated_profit,
                                     industry, latest_fx.dt, db=db)
            if v2 == '强':
                return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2, v3=estimated_profit)
        else:
            logger.info(f"{name}{symbol}一买不成立原因: {failed_trend_bc_conditions}")

    # 30 * N天内是否有过一买且向上笔, 存在一买则检测二三买
    if history.check_duplicate(symbol, edt, days=30 * 6, signals='一买', db=db):
        latest_1st_buy_point = history.query_latest_buy_point(symbol, signals='一买', db=db)

        # 提取一买后的bi_list
        bis_after_1st_buy = [bi for bi in bis if bi.sdt.date() >= latest_1st_buy_point.date.date()]
        zs_seq_after_1st_buy = get_zs_seq(bis_after_1st_buy)
        is_lower_freq_pzbc = detect_lower_freq_pzbc(bis_after_1st_buy, cache_key)
        _has_uncover_gap = has_uncover_gap(bis_after_1st_buy, kind_is_up=True)
        _ma_is_up = ma_is_up(c, last_n=3, ma_type="SMA", timeperiod=60)

        bis_pzbc_conditions = (
            (0 < len(zs_seq_after_1st_buy) < 3, "0 < len(zs_seq_after_1st_buy) < 3"),
            (ubi['direction'] == Direction.Up, "ubi['direction'] == Direction.Up"),
            (len(ubi['fxs']) < 2, "len(ubi['fxs']) < 2"),
            (is_lower_freq_pzbc, "is_lower_freq_pzbc"),
            (raw_bar_increase_within_limit(latest_fx.raw_bars, 0.08), "increase_break_limit"),
            (_has_uncover_gap or _ma_is_up, f"{_has_uncover_gap=} or {_ma_is_up=}")
        )
        failed_bis_pzbc_conditions = select_failed_conditions(bis_pzbc_conditions)

        if not failed_bis_pzbc_conditions:
            zs1_after_1st_buy = zs_seq_after_1st_buy[0]
            # 判断二买
            if (
                    latest_fx.low < zs1_after_1st_buy.zg
                    and len(zs_seq_after_1st_buy) == 1
                    and len(zs1_after_1st_buy.bis) > 2
            ):
                v1 = '二买'
                # 插入数据库
                history.insert_buy_point(name, symbol, ts_code, freq, v1, latest_fx.power_str, estimated_profit,
                                         industry, latest_fx.dt, db=db)
                if v2 != '弱':
                    return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2, v3=estimated_profit)

            # 判断三买
            zs2_after_1st_buy = zs_seq_after_1st_buy[1]
            if latest_fx.low > zs1_after_1st_buy.zg:
                v1 = '三买'
                # 插入数据库
                history.insert_buy_point(name, symbol, ts_code, freq, v1, latest_fx.power_str, estimated_profit,
                                         industry, latest_fx.dt, db=db)
                if v2 != '弱':
                    return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2, v3=estimated_profit)
        else:
            logger.info(f"{name}{symbol}二三买不成立原因: {failed_bis_pzbc_conditions}")

    return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)


def trend_reverse_xd(c: CZSC, fx_dt_limit: int = 5, **kwargs) -> OrderedDict:
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
    db = "XD"
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
    elif history.buy_point_exists(symbol, latest_fx.dt, freq, db=db):
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
                                     industry, latest_fx.dt, db=db)
            if v2 == '强':
                return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2, v3=estimated_profit)
        else:
            logger.info(f"{name}{symbol}一买不成立原因: {failed_trend_bc_conditions}")

    # 30 * N天内是否有过一买且向上笔, 存在一买则检测二三买
    if history.check_duplicate(symbol, edt, days=30 * 6, signals='一买', db=db):
        latest_1st_buy_point = history.query_latest_buy_point(symbol, signals='一买', db=db)

        # 提取一买后的bi_list
        bis_after_1st_buy = [bi for bi in bis if bi.sdt.date() >= latest_1st_buy_point.date.date()]
        xds_after_1st_buy = analyze_xd(bis_after_1st_buy)
        if xds_after_1st_buy[0].direction == Direction.Down:
            xds_after_1st_buy.pop(0)
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
                                         industry, latest_fx.dt, db=db)
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
                                         industry, latest_fx.dt, db=db)
                # if v2 != '弱':
                return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2, v3=estimated_profit)
        else:
            logger.info(f"{name}{symbol}二三买不成立原因: {failed_bis_pzbc_conditions}")

    gc.collect()

    return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
