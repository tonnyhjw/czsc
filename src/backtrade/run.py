import gc
import pprint

import backtrader as bt
from datetime import datetime


from czsc import CZSC, home_path, Direction
from czsc.data import TsDataCache
from czsc.data.ts import format_kline
from database.history import query_all_buy_point
from src.backtrade.strategy import NaiveStrategy
from src.backtrade.utils import get_bt_data


def run_single_stock_backtest(ts_code='000001.SZ', edt: str = datetime.now().strftime('%Y%m%d'),
                              fx_pwr="弱", signals="二买", freq="D", db="BI"):
    cerebro = bt.Cerebro()

    symbol = ts_code.split(".")[0]

    # 设置策略参数
    buy_points = list(query_all_buy_point(symbol, fx_pwr=fx_pwr, signals=signals, freq=freq, db=db))
    if not buy_points:
        # print(f"No buy points for {ts_code}")
        return {"trade_analyzer": None, "sharpe_ratio": None}
    sdt = buy_points[0].date.strftime('%Y%m%d')
    name = buy_points[0].name
    tdc = TsDataCache(home_path)
    df = tdc.pro_bar(ts_code, start_date=sdt, end_date=edt, freq=freq, asset="E", adj='qfq', raw_bar=False)
    bars = format_kline(df, tdc.freq_map[freq])

    # 获取格式化后的backtrader数据
    bt_data = get_bt_data(df)

    c = CZSC(bars)
    fxs = [bi.fx_b for bi in c.bi_list if bi.direction == Direction.Up]

    # 传入数据
    cerebro.adddata(bt_data)

    # 创建策略实例并传递参数
    cerebro.addstrategy(NaiveStrategy, buy_points=buy_points, fxs=fxs)

    # 添加分析器
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trade_analyzer')
    cerebro.addanalyzer(bt.analyzers.SharpeRatio_A, _name='sharpe_ratio')

    # 设置初始现金
    cerebro.broker.set_cash(100000.0)

    # 设置交易手续费
    cerebro.broker.setcommission(commission=0.001)

    # print(f'Starting Portfolio Value for {ts_code}: %.2f' % cerebro.broker.getvalue())
    results = cerebro.run()
    result = results[0]
    # print(f'Ending Portfolio Value for {ts_code}: %.2f' % cerebro.broker.getvalue())
    # 获取分析器结果
    trade_analyzer = result.analyzers.trade_analyzer.get_analysis()
    sharpe_ratio = result.analyzers.sharpe_ratio.get_analysis()
    trade_detail = dict(name=name, symbol=symbol)
    try:
        trade_detail['gross_profit'] = round(trade_analyzer['pnl']['gross']['total'], 2)
        trade_detail['net_profit'] = round(trade_analyzer['pnl']['gross']['average'], 2)
        trade_detail['buy_dates'] = result.buy_dates
        trade_detail['sell_dates'] = result.sell_dates
    except KeyError:
        trade_detail['gross_profit'] = -1000
        trade_detail['net_profit'] = -1000
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

    return {"trade_analyzer": trade_analyzer, "sharpe_ratio": sharpe_ratio, "trade_detail": trade_detail}


def run_buy_lists():
    pass
