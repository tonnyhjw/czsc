import pprint
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


logger.add("statics/logs/pzbc.log", level="INFO", rotation="10MB", encoding="utf-8", enqueue=True, retention="10 days")


def macd_pzbc_bi(c: CZSC, fx_dt_limit: int = 30, **kwargs) -> OrderedDict:
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
    db = "BI"
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
    elif history.buy_point_exists(symbol, latest_fx.dt, freq, db):
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
        (abs(bi_b.change) >= 0.7 * abs(bi_a.change), f"{bi_b.change=} >= 70% * {bi_a.change=}"),
        (0 > bi_b_dif > bi_a_dif or abs(bi_a_macd_area) > abs(bi_b_macd_area), "0 > bi_b_dif > bi_a_dif or abs(bi_a_macd_area) > abs(bi_b_macd_area)")
    )
    failed_pzbc_conditions = select_failed_conditions(pzbc_conditions)

    if not failed_pzbc_conditions:
        v1 = '一买'
        # 插入数据库
        history.insert_buy_point(name, symbol, ts_code, freq, v1, latest_fx.power_str, estimated_profit,
                                 industry, latest_fx.dt, db)
        if v2 != '弱':

            return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2, v3=estimated_profit)
    else:
        logger.info(f"{name}{symbol}盘整背驰不成立原因: {failed_pzbc_conditions}")
    return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)