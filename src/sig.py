import pprint
import datetime
from loguru import logger
from collections import OrderedDict

from czsc import CZSC
from czsc.objects import Direction, FX, BI, ZS
from czsc.utils import get_sub_elements, create_single_signal
from czsc.signals.tas import update_macd_cache
from czsc.utils.sig import get_zs_seq
from czsc.enum import Mark
from database import history


logger.add("statics/logs/sig.log", level="INFO", rotation="10MB", encoding="utf-8", enqueue=True, retention="10 days")


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

    if len(bis) < 15 or not ubi or len(ubi['raw_bars']) < 3:
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

    zs_seq = get_zs_seq(bis)
    if len(zs_seq) == 0:
        v1 = '无中枢'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)

    zs1 = zs_seq[-1]
    # 当最后的中枢少于3笔，就将最后的中枢和倒数第二个中枢合并再计算
    if not zs1.is_valid and zs1.edir == Direction.Down and len(zs_seq) > 1:
        zs1 = ZS(zs_seq[-2].bis + zs1.bis)
    # 查找 BI.high 等于 zs2 的 gg 那一笔，并切片
    bi_a_index = next((i for i, bi in enumerate(zs1.bis) if bi.high == zs1.gg and bi.direction == Direction.Down), None)
    remaining_bis = zs1.bis[bi_a_index:]
    zs2 = ZS(remaining_bis)
    estimated_profit = (zs2.zd - cur_price) / cur_price

    bi_a = zs2.bis[0]
    bi_b = zs2.bis[-1]
    bi_a_dif = min(x.cache[cache_key]['dif'] for x in bi_a.raw_bars)
    bi_b_dif = min(x.cache[cache_key]['dif'] for x in bi_b.raw_bars)

    bi_a_macd_area = sum(macd for x in bi_a.raw_bars if (macd := x.cache[cache_key]['macd']) < 0)
    bi_b_macd_area = sum(macd for x in bi_b.raw_bars if (macd := x.cache[cache_key]['macd']) < 0)
    # print(zs2)
    # print(bi_a)
    # print(bi_b)
    # print(bi_a_macd_area, bi_b_macd_area)
    # print(bi_b_dif, bi_a_dif)
    # print(zs2.is_valid)
    # print(ubi['direction'] == Direction.Up)
    # print(len(ubi['fxs']) < 2)
    # print(zs2.sdir == Direction.Down)
    # print(zs2.edir == Direction.Down)
    # print(zs2.dd == bi_b.low)
    # print((0 > bi_b_dif > bi_a_dif or abs(bi_a_macd_area) > abs(bi_b_macd_area)))
    # print(v2)

    if (
            zs2.is_valid and
            ubi['direction'] == Direction.Up and
            len(ubi['fxs']) < 2 and
            zs2.sdir == Direction.Down and
            zs2.edir == Direction.Down and
            zs2.dd == bi_b.low and
            (0 > bi_b_dif > bi_a_dif or abs(bi_a_macd_area) > abs(bi_b_macd_area))
    ):
        v1 = '一买'
        # 插入数据库
        history.insert_buy_point(name, symbol, ts_code, freq, v1, latest_fx.power_str, estimated_profit,
                                 industry, latest_fx.dt)
        if v2 != '弱':

            return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2, v3=estimated_profit)

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

    if len(bis) < 15 or not ubi or len(ubi['raw_bars']) < 3:
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

    zs_seq = get_zs_seq(bis)
    if len(zs_seq) < 3:
        v1 = '中枢<3'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)

    zs1, zs2, zs3 = zs_seq[-3:]
    estimated_profit = (zs3.zd - cur_price) / cur_price

    # 是否一买
    if zs3.is_valid:
        if (
            ubi['direction'] == Direction.Up
            and len(ubi['fxs']) < 2
            and ubi['low'] < zs3.zd
            and zs2.zd > zs3.zg
        ):
            # 否则检测一买
            bi_a, bi_b = zs2.bis[-1], zs3.bis[-1]
            bi_a_dif = min(x.cache[cache_key]['dif'] for x in bi_a.raw_bars)
            bi_b_dif = min(x.cache[cache_key]['dif'] for x in bi_b.raw_bars)

            bi_a_macd_area = sum(macd for x in bi_a.raw_bars if (macd := x.cache[cache_key]['macd']) < 0)
            bi_b_macd_area = sum(macd for x in bi_b.raw_bars if (macd := x.cache[cache_key]['macd']) < 0)

            if (
                0 > bi_b_dif > bi_a_dif
                and abs(bi_b_macd_area) < abs(bi_a_macd_area)
                and estimated_profit >= 0.03
            ):
                if bi_b.low == zs3.dd:
                    v1 = '一买'
                    # 插入数据库
                    history.insert_buy_point(name, symbol, ts_code, freq, v1, latest_fx.power_str, estimated_profit,
                                             industry, latest_fx.dt)
                    if v2 != '弱':
                        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2, v3=estimated_profit)

    # elif zs2.is_valid:
    #     bi_a = zs1.bis[-1]
    #     bi_a_dif = min(x.cache[cache_key]['dif'] for x in bi_a.raw_bars)
    #     bi_a_macd_area = sum(macd for x in bi_a.raw_bars if (macd := x.cache[cache_key]['macd']) < 0)
    #
    #     bi_c_raw_bars = zs2.bis[-1].raw_bars
    #     for _bi in zs3.bis:     # 扩展bi_c
    #         bi_c_raw_bars += _bi.raw_bars
    #     if ubi['direction'] == Direction.Down:
    #         bi_c_raw_bars += ubi['raw_bars']
    #     bi_c_peak_dif = min(macd for x in bi_c_raw_bars if (macd := x.cache[cache_key]['dif']) < 0)  # todo 有bug
    #     bi_c_macd_area = sum(macd for x in bi_c_raw_bars if (macd := x.cache[cache_key]['macd']) < 0)
    #     # bi_c_max_macd = max(abs(macd) for x in bi_c_raw_bars if (macd := x.cache[cache_key]['macd']) < 0)
    #     # bi_c_last_macd = bi_c_raw_bars[-1].cache[cache_key]['macd']
    #     estimated_profit = (zs2.zd - cur_price) / cur_price
    #     if (
    #             0 > bi_c_peak_dif > bi_a_dif
    #             and abs(bi_c_macd_area) < abs(bi_a_macd_area)
    #             # and abs(bi_c_last_macd) < bi_c_max_macd
    #             # and bi_c_last_macd < 0
    #             and estimated_profit >= 0.03
    #             and zs1.dd > zs2.gg
    #             and zs2.dd > zs3.gg
    #     ):
    #         v1 = '一买'
    #         # 插入数据库
    #         history.insert_buy_point(name, symbol, ts_code, freq, v1, latest_fx.power_str, estimated_profit,
    #                                  industry, latest_fx.dt)
    #         return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2, v3=estimated_profit)

    # 30 * N天内是否有过一买且向上笔, 存在一买则检测二三买
    if history.check_duplicate(symbol, edt, days=30 * 6, signals='一买'):
        latest_1st_buy_point = history.query_latest_buy_point(symbol, signals='一买')

        # 提取一买后的bi_list
        bis_after_1st_buy = [bi for bi in bis if bi.sdt.date() >= latest_1st_buy_point.date.date()]
        zs_seq_after_1st_buy = get_zs_seq(bis_after_1st_buy)
        max_macd_of_bi_0 = max(abs(x.cache[cache_key]['macd']) for x in bis_after_1st_buy[0].raw_bars)

        # pprint.pp(zs_seq_after_1st_buy[-1].bis)
        # pprint.pp(bis[-1])
        # print(ubi['direction'] == Direction.Up)
        # print(len(ubi['fxs']) < 2)
        # print(abs(c.bars_raw[-1].cache[cache_key]['macd']) < max_macd_of_bi_0 / 3)
        # print(c.bars_raw[-1].cache[cache_key]['macd'] > c.bars_raw[-2].cache[cache_key]['macd'])
        # print(c.bars_raw[-1].cache[cache_key]['dif'] > 0)
        # print(c.bars_raw[-1].cache[cache_key]['dea'] > 0)

        if (
            0 < len(zs_seq_after_1st_buy) < 3
            and ubi['direction'] == Direction.Up
            and len(ubi['fxs']) < 2
            and abs(c.bars_raw[-1].cache[cache_key]['macd']) < max_macd_of_bi_0 / 3
            and c.bars_raw[-1].cache[cache_key]['macd'] > c.bars_raw[-2].cache[cache_key]['macd']
            and c.bars_raw[-1].cache[cache_key]['dif'] > 0
            and c.bars_raw[-1].cache[cache_key]['dea'] > 0
        ):
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
                                         industry, latest_fx.dt)
                # if v2 != '弱':
                return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2, v3=estimated_profit)

            # 判断三买
            zs2_after_1st_buy = zs_seq_after_1st_buy[1]
            if latest_fx.low > zs1_after_1st_buy.zg and len(zs2_after_1st_buy.bis) < 3:
                v1 = '三买'
                # 插入数据库
                history.insert_buy_point(name, symbol, ts_code, freq, v1, latest_fx.power_str, estimated_profit,
                                         industry, latest_fx.dt)
                # if v2 != '弱':
                return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2, v3=estimated_profit)
    return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)


def is_strong_bot_fx(c: CZSC, latest_fx: FX, edt: datetime.datetime, **kwargs) -> bool:
    fx_mark_cond = latest_fx.mark == Mark.D
    delta_dt_cond = (edt - latest_fx.dt).days < 15
    fx_power_cond = latest_fx.power_str == '强'
    ubi_dir_cond = c.ubi['direction'] == Direction.Up
    ubi_fx_cnt_cond = len(c.ubi['fxs']) < 2
    logger.debug(f"{latest_fx.symbol}:{fx_mark_cond=} {delta_dt_cond=} {fx_power_cond=} {ubi_dir_cond=} {ubi_fx_cnt_cond=}")

    if fx_mark_cond and delta_dt_cond and fx_power_cond and ubi_dir_cond and ubi_fx_cnt_cond:
    # if fx_mark_cond and delta_dt_cond and fx_power_cond:
        return True
    else:
        return False


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
