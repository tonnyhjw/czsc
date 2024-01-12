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
from flask import Flask, render_template
from czsc import CZSC, home_path
from czsc.data import TsDataCache


dc = TsDataCache(home_path)
app = Flask(__name__, static_folder="templates")
bars = dc.pro_bar('603613.SH', start_date="20200101", freq='D', asset="E", adj='qfq', raw_bar=True)
idx = 1000


def bar_base():
    global idx
    idx += 1
    # _bars = bars[:idx]
    print(idx, bars[-1].dt)

    c = CZSC(bars).to_echarts()
    return c


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/barChart")
def get_bar_chart():
    c = bar_base()
    return c.dump_options_with_quotes()


if __name__ == "__main__":
    app.run()


