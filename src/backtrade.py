import os
import gc
import pandas as pd
import backtrader as bt
from datetime import datetime
from pprint import pprint

from czsc import CZSC, home_path
from czsc.data import TsDataCache
from czsc.data.ts import format_kline
from czsc.enum import Mark
from database.history import query_all_buy_point


class MyStrategy(bt.Strategy):
    params = (
        ('buy_points', []),
        ('fxs', []),
        ('stop_loss', 0.05),  # 添加止损参数
    )

    def __init__(self):
        self.order = None
        self.buy_signal = False
        self.sell_signal = False
        self.all_buy_points_consumed = False

        # 用于记录买卖点
        self.buy_dates = []
        self.sell_dates = []

        # 记录买入价格
        self.buy_price = None

    def next(self):
        if self.all_buy_points_consumed and self.position.size == 0:
            print("All buy points consumed and position cleared. Ending early.")
            self.env.runstop()
            return

        current_date = bt.num2date(self.datas[0].datetime[0])  # 将当前时间转换为datetime对象

        # 检查是否有买入信号
        if not self.all_buy_points_consumed:
            for buy_point in self.params.buy_points:
                if (current_date - buy_point.date).days == 2 and self.position.size == 0:
                    self.buy_signal = True
                    self.params.buy_points.remove(buy_point)
                    break

            if not self.params.buy_points:
                self.all_buy_points_consumed = True

        # 执行买入操作
        if self.buy_signal and self.order is None:
            available_cash = self.broker.get_cash()
            price = self.data.close[0]
            commission = self.broker.getcommissioninfo(self.data).getcommission(price, 1)
            buy_size = int(available_cash / (price * (1 + commission)))
            if buy_size > 0:
                self.buy_price = price  # 记录买入价格
                stop_loss_price = price * (1 - self.params.stop_loss)
                self.order = self.buy_bracket(size=buy_size,
                                              limitprice=None,
                                              stopprice=stop_loss_price)
                self.buy_dates.append(current_date)
                print(f'BUY ORDER CREATED: {buy_size} shares at {price} with stop loss at {stop_loss_price} on {current_date}')
            else:
                print('Insufficient cash to create buy order')
            self.buy_signal = False

        # 检查是否有卖出信号
        if self.position.size > 0:
            for idx, fx in enumerate(self.params.fxs):
                if fx.dt == current_date:
                    self.sell_signal = True
                    # 截断fxs列表，只保留未处理部分
                    self.params.fxs = self.params.fxs[idx + 1:]
                    break

        # 执行卖出操作
        if self.sell_signal and self.order is None:
            if self.position.size > 0:
                self.order = self.sell(size=self.position.size)
                self.sell_dates.append(current_date)
                print(f'SELL ORDER CREATED: {self.position.size} shares at {self.data.close[0]} on {current_date}')
            else:
                print('No position to sell')
            self.sell_signal = False

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return  # 等待订单被处理

        if order.status in [order.Completed]:
            if order.isbuy():
                print(f'BUY EXECUTED, {order.executed.price}')
            elif order.issell():
                print(f'SELL EXECUTED, {order.executed.price}')

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            print(f'Order Canceled/Margin/Rejected: {order.info}')

        # 订单完成后，将self.order重置为None
        self.order = None


def get_bt_data(df):
    # 确保日期列是datetime类型并且设置为索引
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df.set_index('trade_date', inplace=True)
    df = df.sort_index()

    # 确保列名符合backtrader期望的格式
    df.rename(columns={
        'open': 'open',
        'high': 'high',
        'low': 'low',
        'close': 'close',
        'vol': 'volume'
    }, inplace=True)

    # 确保数据类型正确
    df = df[['open', 'high', 'low', 'close', 'volume']].astype(float)

    # # 打印调试信息
    # print("Renamed DataFrame head:\n", df.head())
    # print("Renamed DataFrame columns:\n", df.columns)

    # 使用backtrader的PandasData数据源
    data = bt.feeds.PandasData(dataname=df)
    return data


def run_single_stock_backtest(ts_code='000001.SZ', edt: str = datetime.now().strftime('%Y%m%d'), freq="D"):
    cerebro = bt.Cerebro()

    symbol = ts_code.split(".")[0]

    # 设置策略参数
    buy_points = list(query_all_buy_point(symbol, fx_pwr=["弱"], signals="三买", freq=freq))
    if not buy_points:
        # print(f"No buy points for {ts_code}")
        return {"trade_analyzer": None, "sharpe_ratio": None}
    sdt = buy_points[0].date.strftime('%Y%m%d')
    tdc = TsDataCache(home_path)
    df = tdc.pro_bar(ts_code, start_date=sdt, end_date=edt, freq=freq, asset="E", adj='qfq', raw_bar=False)
    bars = format_kline(df, tdc.freq_map[freq])

    # 获取格式化后的backtrader数据
    bt_data = get_bt_data(df)

    c = CZSC(bars)
    fxs = c.fx_list

    # 传入数据
    cerebro.adddata(bt_data)

    # 创建策略实例并传递参数
    cerebro.addstrategy(MyStrategy, buy_points=buy_points, fxs=fxs)

    # 添加分析器
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trade_analyzer')
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe_ratio')

    # 设置初始现金
    cerebro.broker.set_cash(100000.0)

    # 设置交易手续费
    cerebro.broker.setcommission(commission=0.001)

    print(f'Starting Portfolio Value for {ts_code}: %.2f' % cerebro.broker.getvalue())
    results = cerebro.run()
    result = results[0]
    print(f'Ending Portfolio Value for {ts_code}: %.2f' % cerebro.broker.getvalue())

    # 获取分析器结果
    trade_analyzer = result.analyzers.trade_analyzer.get_analysis()
    sharpe_ratio = result.analyzers.sharpe_ratio.get_analysis()

    # # 打印分析器结果
    # print(f'Trade Analysis Results for {ts_code}:')
    # pprint(trade_analyzer)
    #
    # print(f'Sharpe Ratio Analysis for {ts_code}:')
    # pprint(sharpe_ratio)

    # # 绘图并保存到文件
    # fig = cerebro.plot(style='candlestick')[0][0]
    # fig.savefig(f'statics/bt_imgs/{ts_code}_{freq}_{sdt}-{edt}.png')

    # 手动释放内存
    del df
    del bt_data
    del cerebro
    del bars
    del c
    del fxs
    gc.collect()

    return {"trade_analyzer": trade_analyzer, "sharpe_ratio": sharpe_ratio}

