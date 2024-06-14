import vectorbt as vbt
import tushare as ts
import backtrader as bt
import pandas as pd
import time

# 设置Tushare的token
ts.set_token('your_tushare_token')
pro = ts.pro_api()

# 获取更多数据
df = pro.daily(ts_code='000001.SZ', start_date='20000101', end_date='20231231')
df['trade_date'] = pd.to_datetime(df['trade_date'])
df.set_index('trade_date', inplace=True)
df = df.sort_index()

# 确保数据格式正确
df = df[['close']]
df.columns = ['Close']

# 向量化回测 (VectorBT)
def run_vectorbt():
    close = df['Close']
    fast_ma = vbt.MA.run(close, window=15)
    entries = close > fast_ma.ma
    exits = close < fast_ma.ma

    start_time = time.time()
    portfolio = vbt.Portfolio.from_signals(
        close, entries, exits, init_cash=100000, fees=0.001,
        freq='D', slippage=0.001  # 增加频率和滑点设置
    )
    end_time = time.time()

    print(f"VectorBT 回测执行时间: {end_time - start_time} 秒")
    return portfolio

# 定义Backtrader策略
class MyStrategy(bt.Strategy):
    def __init__(self):
        self.sma = bt.indicators.SimpleMovingAverage(self.data.close, period=15)

    def next(self):
        if not self.position:
            if self.data.close > self.sma:
                self.buy(size=100)
        elif self.data.close < self.sma:
            self.sell(size=100)

# 逐条数据处理回测 (Backtrader)
def run_backtrader():
    data = bt.feeds.PandasData(dataname=df)

    # 创建回测引擎
    cerebro = bt.Cerebro()
    cerebro.addstrategy(MyStrategy)
    cerebro.adddata(data)
    cerebro.broker.set_cash(100000)
    cerebro.broker.setcommission(commission=0.001)

    # 运行回测
    start_time = time.time()
    cerebro.run()
    end_time = time.time()

    print(f"Backtrader 回测执行时间: {end_time - start_time} 秒")

if __name__ == '__main__':
    print("运行 VectorBT 回测...")
    run_vectorbt()

    print("\n运行 Backtrader 回测...")
    run_backtrader()
