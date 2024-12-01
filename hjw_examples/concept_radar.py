import os
import pickle
import pprint

import pandas as pd
from loguru import logger
from datetime import datetime, timedelta


from src.concept import detect


def rise_ratio_top_n(top_n=10):
    previous_result_file = "rise_ratio_previous_code.pkl"
    result = detect.get_latest_concepts_with_criteria(top_n)
    # 加载上次警报的结果（如果存在）
    previous_result = load_previous_result_code(previous_result_file)

    # 获取新增的概念板块
    new_concepts = [concept for concept in result if concept.get('code') not in previous_result]

    # 如果有新增概念，触发警报
    if new_concepts:
        logger.info(f"警报！有{len(new_concepts)}个新增的概念进入")
        # 在此触发相关的警报操作，如发送邮件或消息等

    # 存储当前的 result 以便下次对比
    store_current_result_code(result, previous_result_file)

    # 将 result 转换为 DataFrame 并返回
    result_df = pd.DataFrame(result)

    return result_df


def rank_improvement(days: int = 3, threshold: int = 5):
    now = datetime.now()
    start_time = now - timedelta(days=days)  # 查询过去 1 天的数据
    end_time = now

    result = detect.detect_rank_improvement(start_time, end_time, threshold)
    # 将 result 转换为 DataFrame 并返回
    result_df = pd.DataFrame(result)

    return result_df


def rank_top_n(top_n=10):
    exclude_codes = ["BK0817", "BK1050", "BK1051"]
    result = detect.get_top_n_concepts_excluding(top_n, exclude_codes=exclude_codes)
    # 将 result 转换为 DataFrame 并返回
    result_df = pd.DataFrame(result)
    result_df = embed_code_href(result_df)
    return result_df


def load_previous_result_code(result_file: str):
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


def store_current_result_code(result, result_file: str):
    """
    存储当前的 result，用于下次对比
    """
    result = [r.get('code') for r in result]
    result_path = os.path.join('statics/cache_results', result_file)
    with open(result_path, 'wb') as f:
        pickle.dump(result, f)


def embed_code_href(input_df: pd.DataFrame):
    input_df['code'] = input_df['code'].apply(
        lambda x: f'<a href="https://quote.eastmoney.com/center/boardlist.html#boards-{x}">{x}</a>')
    return input_df


def demo():
    top_n = rank_top_n()
    pprint.pp(top_n)


if __name__ == "__main__":
    demo()

