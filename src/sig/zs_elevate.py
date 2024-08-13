import pprint
import datetime
from loguru import logger
from itertools import chain
from collections import OrderedDict

from czsc import CZSC
from czsc.objects import Direction
from czsc.utils import create_single_signal
from czsc.utils.sig import get_zs_seq
from czsc.enum import Mark
from database import history
from src.sig.utils import select_failed_conditions, date_exceed_rawbars, get_xd_zs_seq
from src.xd.analyze_by_break import analyze_xd


logger.add("statics/logs/zs_elevate.log", level="INFO", rotation="10MB", encoding="utf-8", enqueue=True, retention="10 days")


def third_buy_bi(c: CZSC, fx_dt_limit: int = 5, **kwargs) -> OrderedDict:
    """三买对应的中枢上移，主要针对大级别使用（周以上）
        不要求有一买，只需中枢有三笔以上
    **信号逻辑：**

    1. 获取中枢列表；
    2. 倒数第二个中枢有3笔以上；
    3. 最后一个中枢只有一笔，且笔内部结构有次级别中枢；

    主要用于用于探测周、月线中枢上移

    :param c: CZSC对象
    :param fx_dt_limit: int, 分型时效性限制
    :param kwargs:
    :return: 信号识别结果
    """
    db = "BI"
    freq = c.freq.value
    v1 = '其他'
    edt = kwargs.get('edt', datetime.datetime.now())
    name, ts_code, symbol = kwargs.get('name'), kwargs.get('ts_code'), kwargs.get('symbol')
    k1, k2, k3 = freq, symbol, edt.strftime("%Y%m%d")
    industry, freq = kwargs.get('industry'), kwargs.get('freq')

    ubi = c.ubi
    bis = c.bi_list
    cur_price = c.bars_raw[-1].close
    latest_fx = c.ubi_fxs[-1]  # 最近一个分型
    fx_is_exceed = date_exceed_rawbars(c.bars_raw, latest_fx.dt, fx_dt_limit)

    if len(bis) < 5 or not ubi or len(ubi['raw_bars']) < 3:
        v1 = 'K线不合标准'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    if latest_fx.mark != Mark.D or fx_is_exceed:
        v1 = '没有底分型'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    elif history.buy_point_exists(symbol, latest_fx.dt, freq, db):
        v1 = '已存在'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    else:
        v2 = latest_fx.power_str

    zs_seq = get_zs_seq(bis)
    if len(zs_seq) < 2:
        v1 = '中枢不够'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)

    zs1, zs2 = zs_seq[-2], zs_seq[-1]
    if len(zs1.bis) < 9 or len(zs2.bis) < 3:
        v1 = '中枢笔不足'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)

    estimated_profit = (zs2.zg - cur_price) / cur_price
    zs2_bi_a, zs2_bi_c = zs2.bis[0], zs2.bis[-1]

    third_buy_conditions = (
        (ubi['direction'] == Direction.Up, "ubi['direction'] == Direction.Up"),
        (len(ubi['fxs']) < 2, f"{len(ubi['fxs'])=} < 2"),
        (zs2_bi_a == Direction.Down, "zs2_bi_a == Direction.Down"),
        (zs2_bi_c == Direction.Down, "zs2_bi_c == Direction.Down"),
        # (zs2_bi_c.low <= zs2.dd, "zs2_bi_c.low <= zs2.dd"),
        (zs1.zg < latest_fx.low, f"{zs1.zg=} < {latest_fx.low=}"),
    )
    failed_third_buy_conditions = select_failed_conditions(third_buy_conditions)

    if not failed_third_buy_conditions:
        v1 = '三买'
        # 插入数据库
        history.insert_buy_point(name, symbol, ts_code, freq, v1, latest_fx.power_str, estimated_profit,
                                 industry, latest_fx.dt, reason="zs_elevate", db=db)
        if v2 != '弱':
            return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2, v3=estimated_profit)
    else:
        logger.info(f"{name}{symbol}中枢上移三买不成立原因: {failed_third_buy_conditions}")
    return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)


def third_buy_xd(c: CZSC, fx_dt_limit: int = 5, **kwargs) -> OrderedDict:
    """三买对应的中枢上移，主要针对大级别使用（周以上）
        不要求有一买，只需中枢有三笔以上
    **信号逻辑：**

    1. 获取中枢列表；
    2. 倒数第二个中枢有3笔以上；
    3. 最后一个中枢只有一笔，且笔内部结构有次级别中枢；

    主要用于用于探测周、月线中枢上移

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

    ubi = c.ubi
    bis = c.bi_list
    cur_price = c.bars_raw[-1].close
    latest_fx = c.ubi_fxs[-1]  # 最近一个分型
    fx_is_exceed = date_exceed_rawbars(c.bars_raw, latest_fx.dt, fx_dt_limit)

    if len(bis) < 5 or not ubi or len(ubi['raw_bars']) < 3:
        v1 = 'K线不合标准'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    if latest_fx.mark != Mark.D or fx_is_exceed:
        v1 = '没有底分型'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    elif history.buy_point_exists(symbol, latest_fx.dt, freq, db):
        v1 = '已存在'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    else:
        v2 = latest_fx.power_str

    xds = analyze_xd(bis)

    zs_seq = get_xd_zs_seq(xds)

    if len(zs_seq) < 2:
        v1 = '中枢不够'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)

    zs1, zs2 = zs_seq[-2], zs_seq[-1]
    if len(zs1.xds) < 3:
        v1 = 'zs1不成立'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)

    estimated_profit = (zs2.zg - cur_price) / cur_price
    last_xd = zs2.xds[0]
    third_buy_conditions = (
        (ubi['direction'] == Direction.Up, "ubi['direction'] == Direction.Up"),
        (len(ubi['fxs']) < 2, "len(ubi['fxs']) < 2"),
        (last_xd == Direction.Down, "last_xd == Direction.Down"),
        (len(zs2.xds) == 1, "len(zs2.xds) == 1"),
        (2 < len(last_xd.bis), "2 < len(last_xd.bis)"),
        # (last_xd.bis[-1].low < last_xd.bis[-1].low, "2 < len(last_xd.bis)"),
        (zs1.zg < latest_fx.low, f"{zs1.zg=} < {latest_fx.low=}"),
    )
    failed_third_buy_conditions = select_failed_conditions(third_buy_conditions)

    if not failed_third_buy_conditions:
        v1 = '三买'
        # 插入数据库
        history.insert_buy_point(name, symbol, ts_code, freq, v1, latest_fx.power_str, estimated_profit,
                                 industry, latest_fx.dt, reason="zs_elevate", db=db)
        if v2 != '弱':
            return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2, v3=estimated_profit)
    else:
        logger.info(f"{name}{symbol}中枢上移三买不成立原因: {failed_third_buy_conditions}")
    return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)