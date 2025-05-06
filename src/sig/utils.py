import datetime
from loguru import logger
from itertools import chain
from typing import List, Optional
from dateutil.relativedelta import relativedelta


from czsc.utils.sig import get_zs_seq, check_gap_info
from czsc import CZSC
from czsc.enum import Mark
from czsc.objects import Direction, FX, BI, ZS
from src.objects import XD, XDZS


def is_strong_bot_fx(c: CZSC, latest_fx: FX, edt: datetime.datetime, **kwargs) -> bool:
    fx_mark_cond = latest_fx.mark == Mark.D
    delta_dt_cond = (edt - latest_fx.dt).days < 15
    fx_power_cond = latest_fx.power_str == '强'
    ubi_dir_cond = c.ubi['direction'] == Direction.Up
    ubi_fx_cnt_cond = len(c.ubi['fxs']) < 2
    logger.debug(f"{latest_fx.symbol}:{fx_mark_cond=} {delta_dt_cond=} {fx_power_cond=} {ubi_dir_cond=} {ubi_fx_cnt_cond=}")

    if fx_mark_cond and delta_dt_cond and fx_power_cond and ubi_dir_cond and ubi_fx_cnt_cond:
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
            # zs.is_valid and
            zs.sdir == Direction.Down and
            zs.edir == Direction.Down and
            (abs(bi_b.change) >= 0.7 * abs(bi_a.change), f"{bi_b.change=} >= 70% * {bi_a.change=}") and
            (abs(latest_dea) <= 0.5 * max_abs_dea or latest_dif >= latest_dea)
    ):
        return True
    else:
        return False


def has_uncover_gap(bis: List[BI], kind_is_up: bool):
    kind = "向上缺口" if kind_is_up else "向下缺口"
    bars_raw = list(chain.from_iterable(bi.raw_bars for bi in bis))
    gap_info = check_gap_info(bars_raw)
    for gap in reversed(gap_info):
        if gap.get("kind") == kind and gap.get("cover") == "未补":
            return True
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


def get_zs_seq_change_limited(bis: List[BI], change_limit: float=0.1) -> List[ZS]:
    """获取连续笔中的中枢序列

    :param bis: 连续笔对象列表
    :param change_limit: 笔幅度限制
    :return: 中枢序列
    """
    zs_list = []
    if not bis:
        return []

    for bi in bis:
        if not zs_list:
            zs_list.append(ZS(bis=[bi]))
            continue

        zs = zs_list[-1]
        if not zs.bis:
            zs.bis.append(bi)
            zs_list[-1] = zs
        else:
            if (bi.direction == Direction.Up and bi.high < zs.zd) or (
                bi.direction == Direction.Down and bi.low > zs.zg) or (
                bi.direction == Direction.Up and len(zs.bis) >= 3 and abs(bi.low - zs.zd)/zs.zd > change_limit) or (
                bi.direction == Direction.Down and len(zs.bis) >= 3 and abs(bi.high - zs.zg)/zs.zg > change_limit
            ):
                zs_list.append(ZS(bis=[bi]))
            else:
                zs.bis.append(bi)
                zs_list[-1] = zs
    return zs_list


MONEY_FLOW_SORT_KEYS_AMOUNT = dict(
    # （万元）
    net_mf_amount="净额",
    buy_sm_amount="小单额",
    buy_md_amount="中单额",
    buy_lg_amount="大单额",
    buy_elg_amount="特大单额"
)

MONEY_FLOW_SORT_KEYS_VOL = dict(
    # （手）
    net_mf_vol="净量",
    buy_sm_vol="小单量",
    buy_md_vol="中单量",
    buy_lg_vol="大单量",
    buy_elg_vol="特大单量"
)


def get_relative_str_date(date_str: str, n_day=30):
    # 将字符串转换为日期对象
    date_obj = datetime.datetime.strptime(date_str, "%Y%m%d")

    # 计算一个月前的日期
    relative_date = date_obj - relativedelta(days=n_day)

    # 将日期对象转换回字符串格式
    relative_date = relative_date.strftime("%Y%m%d")
    return relative_date
