import pandas as pd
import backtrader as bt


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


def combine_trade_analyzers(analyzers):
    combined = bt.AutoOrderedDict()
    for analyzer_data in analyzers:
        if not analyzer_data:
            continue
        for key, value in analyzer_data.items():
            if key in combined:
                combined[key] = combine_dicts(combined[key], value)
            else:
                combined[key] = value
    return combined


def combine_dicts(dict1, dict2):
    combined = dict1.copy()
    for key, value in dict2.items():
        if key in combined:
            if isinstance(value, bt.AutoOrderedDict):
                combined[key] = combine_dicts(combined[key], value)
            else:
                if isinstance(combined[key], bt.AutoOrderedDict):
                    combined[key] = add_dicts(combined[key], {key: value})
                else:
                    combined[key] += value
        else:
            combined[key] = value
    return combined


def add_dicts(dict1, dict2):
    combined = dict1.copy()
    for key, value in dict2.items():
        if key in combined:
            combined[key] += value
        else:
            combined[key] = value
    return combined


def combine_sharpe_ratios(ratios):
    combined = bt.AutoOrderedDict()
    for ratio in ratios:
        for key, value in ratio.items():
            if key not in combined:
                if value is not None:
                    combined[key] = value
            else:
                if value is not None and combined[key] is not None:
                    combined[key] = (combined[key] + value) / 2  # 计算平均值
    return combined
