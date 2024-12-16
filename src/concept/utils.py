import os
import pickle
import pprint

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
        lambda x: f'<a href="https://quote.eastmoney.com/center/boardlist.html#boards-{x}">{x}</a>')
    return input_df


def embed_symbol_href(input_df: pd.DataFrame):
    input_df['symbol'] = input_df['symbol'].apply(
        lambda x: f'<a href="https://quote.eastmoney.com/{x}.html">{x}</a>')
    return input_df


def get_recent_n_trade_dates_boundary(n: int = 3):
    from czsc import home_path
    from czsc.data import TsDataCache

    today = datetime.datetime.now()
    sdt = (today - datetime.timedelta(days=n+10)).strftime("%Y%m%d")
    edt = today.strftime("%Y%m%d")
    trade_dates = TsDataCache(home_path).get_dates_span(sdt, edt, is_open=True)
    return trade_dates[-n], trade_dates[-1]


if __name__ == '__main__':
    get_recent_n_trade_dates_boundary()
