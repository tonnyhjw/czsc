import os
import pickle
import pprint
import json

import pandas as pd
import datetime


def load_previous_result(result_file: str):
    """
    加载上次警报的结果
    """
    try:
        result_path = os.path.join('statics/cache_results', result_file)
        with open(result_path, 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        # 如果文件不存在，返回空列表
        return []


def store_current_result_code(result, result_file: str, store_field: str):
    """
    存储当前的 result，用于下次对比
    """
    result = [r.get(store_field) for r in result]
    result_path = os.path.join('statics/cache_results', result_file)
    with open(result_path, 'wb') as f:
        pickle.dump(result, f)


def new_element(store_field, previous_result_file, cur_result):
    # 加载上次警报的结果（如果存在）
    previous_result = load_previous_result(previous_result_file)

    # 返回新增的元素列表
    return [result.get(store_field) for result in cur_result if result.get(store_field) not in previous_result]


def embed_code_href(input_df: pd.DataFrame):
    input_df['code'] = input_df['code'].apply(
        lambda x: f'<a href="https://quote.eastmoney.com/center/gridlist.html#boards2-90.{x}">{x}</a>')
    return input_df


def embed_symbol_href(input_df: pd.DataFrame):
    input_df['symbol'] = input_df['symbol'].apply(
        lambda x: f'<a href="https://quote.eastmoney.com/{x}.html">{x}</a>')
    return input_df


def embed_ts_code_href(input_df: pd.DataFrame):
    def create_link(ts_code):
        _symbol, _hs = ts_code.split('.')
        return f'<a href="https://xueqiu.com/S/{_hs}{_symbol}">{_symbol}</a>'

    input_df['ts_code'] = input_df['ts_code'].map(create_link)
    return input_df


def get_recent_n_trade_dates_boundary(n: int = 3, latest_timestamp=None):
    from czsc import home_path
    from czsc.data import TsDataCache

    if latest_timestamp is None:
        today = datetime.datetime.now()
    else:
        today = datetime.datetime.strptime(latest_timestamp[:10], "%Y%m%d")
    sdt = (today - datetime.timedelta(days=n+10)).strftime("%Y%m%d")
    edt = today.strftime("%Y%m%d")
    trade_dates = TsDataCache(home_path).get_dates_span(sdt, edt, is_open=True)
    return trade_dates[-n], trade_dates[-1]


def merge_concept_stocks(stock_list):
    """
    合并相同股票买点但不同概念名的股票信息

    Args:
        stock_list: List[dict] 包含股票买点和概念信息的字典列表

    Returns:
        List[dict] 合并后的字典列表，相同股票的概念名会被合并到name字段中
    """
    # 用于存储合并结果的字典
    merged = {}

    # 遍历所有股票信息
    for stock in stock_list:
        ts_code = stock['ts_code']

        # 如果股票代码已存在，则合并概念名
        if ts_code in merged:
            # 确保不重复添加相同的概念名
            if stock['name'] not in merged[ts_code]['name']:
                merged[ts_code]['name'] = merged[ts_code]['name'] + ',' + stock['name']
        # 如果是新的股票代码，直接添加
        else:
            merged[ts_code] = stock.copy()

    # 将字典转换回列表
    return list(merged.values())


def load_concepts_from_json(config_path):
    """从JSON文件加载concepts配置"""
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    return config['concepts']


if __name__ == '__main__':
    print(get_recent_n_trade_dates_boundary(latest_timestamp="2024-12-05 11:30"))
