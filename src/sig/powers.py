import datetime
import numpy as np
from loguru import logger
from collections import OrderedDict

from czsc import CZSC, RawBar
from czsc.utils import get_sub_elements, create_single_signal
from czsc.signals.tas import update_macd_cache, update_ma_cache
from src.sig.utils import *
from src.objects import has_gap
from database import history

logger.add("statics/logs/pzbc.log", level="INFO", rotation="10MB", encoding="utf-8", enqueue=True, retention="10 days")


def long_term_ma_support(c: CZSC, fx_dt_limit: int = 5, **kwargs) -> OrderedDict:
    """长期均线强势

    **信号逻辑：**

    1. 长期均线斜率 >= 0；
    2. 最终价格在长期均线之上；
    3. k线出现盘整底背驰；

    主要用于用于大级别寻找强势股切入点

    :param c: CZSC对象
    :param fx_dt_limit: int, 分型时效性限制
    :param kwargs:
    :return: 信号识别结果
    """
    db = kwargs.get("db", "MA250")
    freq = c.freq.value
    v1 = '其他'
    estimated_profit = 0
    edt = kwargs.get('edt', datetime.datetime.now())
    name, ts_code, symbol = kwargs.get('name'), kwargs.get('ts_code'), kwargs.get('symbol')
    k1, k2, k3 = freq, symbol, edt.strftime("%Y%m%d")
    industry, freq = kwargs.get('industry'), kwargs.get('freq')
    timeperiod = kwargs.get('timeperiod', 250)
    last_n = kwargs.get('last_n', 5)

    ubi = c.ubi
    bis = c.bi_list
    cur_price = c.bars_raw[-1].high
    latest_fx = c.ubi_fxs[-1]  # 最近一个分型
    fx_is_exceed = date_exceed_rawbars(c.bars_raw, latest_fx.dt, fx_dt_limit)

    if len(bis) < 4 or not ubi or len(ubi['raw_bars']) < 3:
        v1 = 'K线不合标准'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    if latest_fx.mark != Mark.D or fx_is_exceed or has_gap(latest_fx.elements[0], latest_fx.elements[1]):
        v1 = '没有底分型'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    elif history.buy_point_exists(symbol, latest_fx.dt, freq, db=db):
        v1 = '已存在'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    elif not ma_break_and_support(c, ma_type="SMA", timeperiod=timeperiod, cur_price=cur_price):
        v1 = '均线不达标'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    else:
        v2 = latest_fx.power_str

    if is_macd_pzbc_bi(c, name=name, symbol=symbol):
        v1 = '强势盘整背驰'
        # 插入数据库
        history.insert_buy_point(name, symbol, ts_code, freq, v1, latest_fx.power_str, estimated_profit,
                                 industry, latest_fx.dt, latest_fx.high, latest_fx.low,
                                 reason="long_term_ma_support", db=db)
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2, v3=estimated_profit)
    return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)


def long_term_ma_up(c: CZSC, fx_dt_limit: int = 5, **kwargs) -> OrderedDict:
    """长期均线强势

    **信号逻辑：**

    1. 长期均线斜率 >= 0；
    2. k线出现盘整底背驰；

    主要用于用于大级别寻找强势股切入点

    :param c: CZSC对象
    :param fx_dt_limit: int, 分型时效性限制
    :param kwargs:
    :return: 信号识别结果
    """
    db = kwargs.get("db", "MA250")
    freq = c.freq.value
    v1 = '其他'
    estimated_profit = 0
    edt = kwargs.get('edt', datetime.datetime.now())
    name, ts_code, symbol = kwargs.get('name'), kwargs.get('ts_code'), kwargs.get('symbol')
    k1, k2, k3 = freq, symbol, edt.strftime("%Y%m%d")
    industry, freq = kwargs.get('industry'), kwargs.get('freq')
    timeperiod = kwargs.get('timeperiod', 250)
    last_n = kwargs.get('last_n', 5)

    ubi = c.ubi
    bis = c.bi_list
    cur_price = c.bars_raw[-1].high
    latest_fx = c.ubi_fxs[-1]  # 最近一个分型
    fx_is_exceed = date_exceed_rawbars(c.bars_raw, latest_fx.dt, fx_dt_limit)

    if len(bis) < 4 or not ubi or len(ubi['raw_bars']) < 3:
        v1 = 'K线不合标准'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    if latest_fx.mark != Mark.D or fx_is_exceed or has_gap(latest_fx.elements[0], latest_fx.elements[1]):
        v1 = '没有底分型'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    elif history.buy_point_exists(symbol, latest_fx.dt, freq, db=db):
        v1 = '已存在'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    elif not ma_is_up(c, last_n=last_n, ma_type="SMA", timeperiod=timeperiod, cur_price=cur_price):
        v1 = '均线不达标'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
    else:
        v2 = latest_fx.power_str

    if is_macd_pzbc_bi(c, name=name, symbol=symbol):
        v1 = 'MA向上盘背'
        # 插入数据库
        history.insert_buy_point(name, symbol, ts_code, freq, v1, latest_fx.power_str, estimated_profit,
                                 industry, latest_fx.dt, latest_fx.high, latest_fx.low, reason="long_term_ma_up", db=db)
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2, v3=estimated_profit)
    return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)


def is_macd_pzbc_bi(c: CZSC, **kwargs) -> bool:
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

    cache_key = update_macd_cache(c)
    bis = c.bi_list
    ubi = c.ubi
    name, symbol = kwargs.get('name'), kwargs.get('symbol')

    remaining_bis = select_pzbc_bis(bis)
    if not remaining_bis:
        logger.debug('笔结构不符合要求')
        return False

    zs2 = ZS(remaining_bis)

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
        (len(zs2.bis) >= 5, "len(zs2.bis)"),
        (abs(bi_b.change) >= 0.7 * abs(bi_a.change), f"{bi_b.change=} >= 70% * {bi_a.change=}"),
        (0 > bi_b_dif > bi_a_dif or abs(bi_a_macd_area) > abs(bi_b_macd_area), "0 > bi_b_dif > bi_a_dif or abs(bi_a_macd_area) > abs(bi_b_macd_area)")
    )
    failed_pzbc_conditions = select_failed_conditions(pzbc_conditions)

    if not failed_pzbc_conditions:
        return True
    else:
        logger.debug(f"{name}{symbol}盘整背驰不成立原因: {failed_pzbc_conditions}")
    return False


def ma_is_up(c: CZSC, last_n: int, ma_type: str,  timeperiod: int, **kwargs) -> bool:
    """均线连续向上

    **信号逻辑：**

    1. 取最后三个笔（含未完成笔）；
    2. 向上则第一笔创新高，否则创新低；
    3. 第一笔macd绝对值大于第二笔macd绝对值；

    主要用于用于探测周、月线盘整背驰

    :param c: CZSC对象
    :param last_n 最后连续n天
    :param ma_type: 均线类型，可选值：SMA, EMA, WMA, KAMA, TEMA, DEMA, MAMA, TRIMA
    :param timeperiod: 计算周期
    :param kwargs:
    :return: 信号识别结果
    """
    bars_raw = c.bars_raw
    ma = update_ma_cache(c, ma_type=ma_type, timeperiod=timeperiod)
    cur_price = kwargs.get("cur_price")

    if len(bars_raw) < last_n:
        return False

    # if cur_price < bars_raw[-1].cache[ma] or np.isnan(bars_raw[-1].cache[ma]):
    #     return False
    if np.isnan(bars_raw[-1].cache[ma]):
        return False

    last_n_bars = bars_raw[-last_n:]
    for i in range(1, last_n):
        if last_n_bars[i].cache[ma] < last_n_bars[i - 1].cache[ma]:
            return False
    return True


def ma_break_and_support(c: CZSC, ma_type: str,  timeperiod: int, **kwargs) -> bool:
    """放量突破均线，缩量盘整背驰回调后，价格高于均线

    **信号逻辑：**

    1. 取最后三个笔（含未完成笔）；
    2. 向上则第一笔创新高，否则创新低；
    3. 第一笔macd绝对值大于第二笔macd绝对值；

    主要用于用于探测周、月线盘整背驰

    :param c: CZSC对象
    :param last_n 最后连续n天
    :param ma_type: 均线类型，可选值：SMA, EMA, WMA, KAMA, TEMA, DEMA, MAMA, TRIMA
    :param timeperiod: 计算周期
    :param kwargs:
    :return: 信号识别结果
    """
    bars_raw = c.bars_raw
    ma = update_ma_cache(c, ma_type=ma_type, timeperiod=timeperiod)
    bis = c.bi_list
    avg_bi_power_volume = np.mean([bi.power_volume for bi in bis])
    cur_price = kwargs.get("cur_price")

    def has_break_ma_bi():
        for bi in bis:
            is_break_ipo_bi = (
                (bi.direction == Direction.Up, "bi.direction == Direction.Up"),
                (bi.power_volume > avg_bi_power_volume, "bi.power_volume > avg_bi_power_volume"),
                (bi.fx_b.fx > bi.fx_b.new_bars[1].cache[ma], "bi.high > ipo_high")
            )
            if not select_failed_conditions(is_break_ipo_bi):
                return True
        return False

    if cur_price < bars_raw[-1].cache[ma] or np.isnan(bars_raw[-1].cache[ma]):
        return False


    return True


def break_ipo_high(c: CZSC):
    ipo_high = c.bars_raw[0].high
    bis = c.bi_list[1:]
    avg_bi_power_volume = np.mean([bi.power_volume for bi in bis])

    def has_break_ipo_bi():
        for bi in bis:
            is_break_ipo_bi = (
                (bi.direction == Direction.Up, "bi.direction == Direction.Up"),
                (bi.power_volume > avg_bi_power_volume, "bi.power_volume > avg_bi_power_volume"),
                (bi.high > ipo_high, "bi.high > ipo_high")
            )
            if not select_failed_conditions(is_break_ipo_bi):
                return True
        return False

    break_ipo_high_conditions = (
        (has_break_ipo_bi(), "has_break_ipo_bi"),

    )
    failed_break_ipo_high_conditions = select_failed_conditions(break_ipo_high_conditions)

    if not failed_break_ipo_high_conditions:
        return True
    else:
        return False


