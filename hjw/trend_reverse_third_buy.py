# -*- coding: utf-8 -*-
"""
author: zengbin93
email: zeng_bin8888@163.com
create_dt: 2022/7/12 14:22
describe: CZSC 逐K线播放
https://pyecharts.org/#/zh-cn/web_flask
"""
import sys

sys.path.insert(0, '.')
sys.path.insert(0, '..')
from czsc import CZSC, home_path
from czsc.data import TsDataCache
from czsc.utils import sig


dc = TsDataCache(home_path)
bars = dc.pro_bar('000001.SH', start_date="20220101", end_date="20231230", freq='D', asset="I", adj='qfq', raw_bar=True)
idx = 1000


def demo():
    global idx
    idx += 1
    _bars = bars[idx:]
    print(idx, _bars[-1].dt)

    bi_list = CZSC(_bars).bi_list
    zs_seq = sig.get_zs_seq(bi_list)
    return zs_seq


if __name__ == "__main__":
    demo()
