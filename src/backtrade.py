import os
import pandas as pd
import backtrader as bt
from datetime import datetime

from czsc import CZSC, home_path
from czsc.data import TsDataCache
from czsc.connectors.ts_connector import format_kline


class MyStrategy(bt.Strategy):
    params = (
        ('buy_points', []),
        ('fxs', []),
    )

    def __init__(self):
        self.order = None
        self.buy_signal = False
        self.sell_signal = False

        # 用于记录买卖点
        self.buy_dates = []
        self.sell_dates = []

    def next(self):
        current_date = self.datas[0].datetime.date(0)

        # 检查是否有买入信号
        for buy_point in self.params.buy_points:
            if (current_date - buy_point.date).days == 2 and self.position.size == 0:
                self.buy_signal = True

        # 检查是否有卖出信号
        if self.position.size > 0:
            for idx, fx in enumerate(self.params.fxs):
                if fx.date == current_date:
                    self.sell_signal = True
                    # 截断fxs列表，只保留未处理部分
                    self.params.fxs = self.params.fxs[idx + 1:]
                    break

        # 执行买入操作
        if self.buy_signal:
            self.order = self.buy(size=self.broker.get_cash() / self.data.close[0])
            self.buy_dates.append(current_date)
            self.buy_signal = False

        # 执行卖出操作
        if self.sell_signal:
            self.order = self.sell(size=self.position.size)
            self.sell_dates.append(current_date)
            self.sell_signal = False

    def notify_order(self, order):
        if order.status in [order.Completed]:
            if order.isbuy():
                print(f'BUY EXECUTED, {order.executed.price}')
            elif order.issell():
                print(f'SELL EXECUTED, {order.executed.price}')
        self.order = None


def get_bt_data(df):
    # pro = ts.pro_api()
    # df = pro.daily(ts_code=stock_code, start_date=start_date, end_date=end_date)
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df.set_index('trade_date', inplace=True)
    df = df.sort_index()
    data = bt.feeds.PandasData(dataname=df, datetime=None, open='open', high='high', low='low', close='close',
                               volume='vol')
    return data


def run_demo(ts_code='000001.SZ', edt: str = datetime.now().strftime('%Y%m%d'), freq="D"):
    from database.history import query_all_buy_point
    cerebro = bt.Cerebro()

    symbol = ts_code.split(".")[0]

    # 设置策略参数
    buy_points = query_all_buy_point(symbol, fx_pwr="强", signals="一买")
    if not buy_points:
        return
    sdt = buy_points[0].date.strftime('%Y%m%d')
    tdc = TsDataCache(home_path)
    df = tdc.pro_bar(ts_code, start_date=sdt, end_date=edt, freq=freq, asset="E", adj='qfq', raw_bar=False)
    bars = format_kline(df, tdc.freq_map(freq))
    bt_data = get_bt_data(df)

    c = CZSC(bars)
    fxs = c.fx_list

    # 传入数据
    cerebro.adddata(bt_data)

    # 创建策略实例并传递参数
    cerebro.addstrategy(MyStrategy, buy_points=buy_points, fxs=fxs)

    # 设置初始现金
    cerebro.broker.set_cash(100000.0)

    # 设置交易手续费
    cerebro.broker.setcommission(commission=0.001)

    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    result = cerebro.run()
    print('Ending Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # 绘图
    cerebro.plot(style='candlestick')

