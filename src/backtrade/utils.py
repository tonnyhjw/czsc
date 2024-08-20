import pandas as pd


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