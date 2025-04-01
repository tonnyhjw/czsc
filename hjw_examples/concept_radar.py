from datetime import datetime, timedelta

import pandas as pd

from src.concept import detect
from src.decorate import timer
from src.notify import notify_concept_radar
from src.concept import utils
from src.concept.configs import *

SUBJ_LV1 = "自动盯盘"   # 或改为"测试"
EDT: str = datetime.now().strftime('%Y%m%d')


def rise_ratio_top_n(top_n=10, bp_days_limit=3, latest_timestamp=None):
    store_field = 'code'
    previous_result_file = "rise_ratio_previous_code.pkl"
    result = detect.get_latest_concepts_with_criteria(top_n, latest_timestamp)
    result_df, buy_points_df = pd.DataFrame(), pd.DataFrame()
    bp_sdt, bp_edt = utils.get_recent_n_trade_dates_boundary(bp_days_limit, latest_timestamp)
    bp_sdt = datetime.strptime(bp_sdt, '%Y%m%d').date()
    bp_edt = datetime.strptime(bp_edt, '%Y%m%d').date()

    # 如果有新增概念，触发警报
    if utils.new_element(store_field, previous_result_file, result):
        # 在此触发相关的警报操作，如发送邮件或消息等
        result_df = pd.DataFrame(result)
        result_df = utils.embed_code_href(result_df)
        email_subject = f"[{SUBJ_LV1}][概念板块][A股]{EDT}发现{len(result_df)}个头部涨跌比概念(全涨+top{top_n})"

        buy_points = detect.get_buypoints_for_multiple_concepts(result, bp_sdt, bp_edt)
        if buy_points:
            buy_points_df = pd.DataFrame(buy_points)
            buy_points_df = utils.embed_ts_code_href(buy_points_df)

        notify_concept_radar(result_df, email_subject, buy_points_df)

    # 存储当前的 result 以便下次对比
    utils.store_current_result_code(result, previous_result_file, store_field)


def rank_improvement(hours: int = 24, threshold: int = 5, bp_days_limit=3, latest_timestamp=None):
    if latest_timestamp is not None:
        now = datetime.strptime(latest_timestamp, "%Y-%m-%d %H:%M").date()
    else:
        now = datetime.now()
    start_time = now - timedelta(hours=hours)  # 查询过去 1 天的数据
    end_time = now
    result_df, buy_points_df = pd.DataFrame(), pd.DataFrame()
    bp_sdt, bp_edt = utils.get_recent_n_trade_dates_boundary(bp_days_limit, latest_timestamp)
    bp_sdt = datetime.strptime(bp_sdt, '%Y%m%d').date()
    bp_edt = datetime.strptime(bp_edt, '%Y%m%d').date()

    result = detect.detect_rank_improvement(start_time, end_time, threshold)
    if result:
        # 将 result 转换为 DataFrame 并返回
        result_df = pd.DataFrame(result)
        result_df = utils.embed_code_href(result_df)
        result_df.sort_values(by='rank_improvement', ascending=False, inplace=True)
        email_subject = f"[{SUBJ_LV1}][概念板块][A股]{EDT}发现{len(result_df)}个{hours}小时内排名提升超过{threshold}的概念"

        buy_points = detect.get_buypoints_for_multiple_concepts(result, bp_sdt, bp_edt)
        if buy_points:
            buy_points_df = pd.DataFrame(buy_points)
            buy_points_df = utils.embed_ts_code_href(buy_points_df)
        notify_concept_radar(result_df, email_subject, buy_points_df)


def rank_top_n(top_n=10, bp_days_limit=3, latest_timestamp=None):
    store_field = 'code'
    previous_result_file = "rank_top_n_code.pkl"
    result = detect.get_top_n_concepts_excluding(top_n, exclude_codes=EXCLUDE_CODES, latest_timestamp=latest_timestamp)
    result_df, buy_points_df = pd.DataFrame(), pd.DataFrame()
    bp_sdt, bp_edt = utils.get_recent_n_trade_dates_boundary(bp_days_limit, latest_timestamp)
    bp_sdt = datetime.strptime(bp_sdt, '%Y%m%d').date()
    bp_edt = datetime.strptime(bp_edt, '%Y%m%d').date()

    # 如果有新增概念板块共振个股，触发警报
    if utils.new_element(store_field, previous_result_file, result):

        # 将 result 转换为 DataFrame 并返回
        result_df = pd.DataFrame(result)
        result_df = utils.embed_code_href(result_df)
        email_subject = f"[{SUBJ_LV1}][概念板块][A股]{EDT}排名前{top_n}的概念"

        buy_points = detect.get_buypoints_for_multiple_concepts(result, bp_sdt, bp_edt)
        if buy_points:
            buy_points_df = pd.DataFrame(buy_points)
            buy_points_df = utils.embed_ts_code_href(buy_points_df)

        notify_concept_radar(result_df, email_subject, buy_points_df)    # 存储当前的 result 以便下次对比
    utils.store_current_result_code(result, previous_result_file, store_field)


def multi_concepts(top_n=10, min_concept_count=2):
    store_field = 'symbol'
    previous_result_file = "multi_concepts_symbol.pkl"

    result = detect.get_stocks_in_multiple_concepts(top_n, min_concept_count, EXCLUDE_CODES)

    # 如果有新增概念板块共振个股，触发警报
    new_elem = utils.new_element(store_field, previous_result_file, result)
    if new_elem:
        # 在此触发相关的警报操作，如发送邮件或消息等
        result_df = pd.DataFrame(result)
        result_df = result_df[result_df['symbol'].isin(new_elem)]
        result_df = utils.embed_symbol_href(result_df)
        email_subject = f"[{SUBJ_LV1}][概念板块][A股]{EDT}发现{len(result_df)}个前{top_n}概念板块共振"

        notify_concept_radar(result_df, email_subject)

    # 存储当前的 result 以便下次对比
    utils.store_current_result_code(result, previous_result_file, store_field)


def rank_drop(top_n=10, rank_threshold=50, avg_rank_window=3, bp_days_limit=3, latest_timestamp=None):
    store_field = 'code'
    previous_result_file = "concept_rank_drop_code.pkl"
    result_df, buy_points_df = pd.DataFrame(), pd.DataFrame()
    bp_sdt, bp_edt = utils.get_recent_n_trade_dates_boundary(bp_days_limit, latest_timestamp)
    bp_sdt = datetime.strptime(bp_sdt, '%Y%m%d').date()
    bp_edt = datetime.strptime(bp_edt, '%Y%m%d').date()

    result = detect.monitor_concept_rank_drop(top_n=top_n, rank_threshold=rank_threshold,
                                              avg_rank_window=avg_rank_window, latest_timestamp=latest_timestamp)

    # 如果有新增概念板块共振个股，触发警报
    if utils.new_element(store_field, previous_result_file, result):

        # 将 result 转换为 DataFrame 并返回
        result_df = pd.DataFrame(result)
        result_df = utils.embed_code_href(result_df)
        email_subject = f"[{SUBJ_LV1}][概念板块][A股]{EDT}近{avg_rank_window}天热门概念急跌至低于{rank_threshold}"

        buy_points = detect.get_buypoints_for_multiple_concepts(result, bp_sdt, bp_edt)
        if buy_points:
            buy_points_df = pd.DataFrame(buy_points)
            buy_points_df = utils.embed_ts_code_href(buy_points_df)

        notify_concept_radar(result_df, email_subject, buy_points_df)
        # 存储当前的 result 以便下次对比
    utils.store_current_result_code(result, previous_result_file, store_field)


def liked(bp_days_limit = 5, latest_timestamp = None):
    store_field = 'ts_code'
    previous_result_file = "liked_concept_by_ts_code.pkl"
    result_df, buy_points_df = pd.DataFrame(), pd.DataFrame()
    # 从配置文件加载concepts
    concepts = utils.load_concepts_from_json(LIKED_CONCEPTS)

    bp_sdt, bp_edt = utils.get_recent_n_trade_dates_boundary(bp_days_limit, latest_timestamp)
    bp_sdt = datetime.strptime(bp_sdt, '%Y%m%d').date()
    bp_edt = datetime.strptime(bp_edt, '%Y%m%d').date()
    # print(bp_sdt, bp_edt)
    buy_points = detect.get_buypoints_for_multiple_concepts(concepts, bp_sdt, bp_edt)
    if not buy_points:
        return
    # 如果有新增概念板块共振个股，触发警报
    if utils.new_element(store_field, previous_result_file, buy_points):
        # 将 result 转换为 DataFrame 并返回
        result_df = pd.DataFrame(concepts)
        result_df = utils.embed_code_href(result_df)
        email_subject = f"[{SUBJ_LV1}][概念板块][A股]{EDT}被关注概念从{bp_sdt}到{bp_edt}存在的个股买点"

        buy_points_df = pd.DataFrame(buy_points)
        buy_points_df = utils.embed_ts_code_href(buy_points_df)

        notify_concept_radar(result_df, email_subject, buy_points_df)
        # 存储当前的 result 以便下次对比
    utils.store_current_result_code(buy_points, previous_result_file, store_field)


def hot_rank_notify(rank_threshold: int = 10, limit_n: int = 5, bp_days_limit = 5, latest_timestamp = None,
                    subj_lv1 = SUBJ_LV1):
    from src.concept.hot_rank import ConceptHotRank, RankType
    bp_sdt, bp_edt = utils.get_recent_n_trade_dates_boundary(bp_days_limit, latest_timestamp)
    email_subject = f"[{subj_lv1}][概念板块][A股]{bp_edt}近{bp_days_limit}交易日热度榜前{rank_threshold}最高频题材概念"

    start_date = datetime.strptime(bp_sdt, '%Y%m%d')
    end_date = datetime.strptime(bp_edt, '%Y%m%d')
    bp_sdt = datetime.strptime(bp_sdt, '%Y%m%d').date()
    bp_edt = datetime.strptime(bp_edt, '%Y%m%d').date()

    # 创建分析器实例
    analyzer = ConceptHotRank()

    # 分析排名靠前的概念
    top_concepts = analyzer.analyze_concepts(
        start_date=start_date,
        end_date=end_date,
        rank_threshold=rank_threshold,
        limit_n=limit_n,
        rank_type=RankType.TOP
    )
    if top_concepts:

        buy_points = detect.get_buypoints_for_multiple_concepts(top_concepts, bp_sdt, bp_edt)

        top_concepts_df = pd.DataFrame(top_concepts)
        top_concepts_df = utils.embed_code_href(top_concepts_df)

        buy_points_df = pd.DataFrame(buy_points)
        buy_points_df = utils.embed_ts_code_href(buy_points_df)
        notify_concept_radar(top_concepts_df, email_subject, buy_points_df)


@timer
def demo(latest_timestamp=None):
    # 监控涨跌比前排
    rise_ratio_top_n(top_n=10, latest_timestamp=latest_timestamp)
    # 监控排名提升
    rank_improvement(hours=24, threshold=300, latest_timestamp=latest_timestamp)
    # rank_improvement(hours=1, threshold=250, latest_timestamp=latest_timestamp)
    rank_drop(top_n=50, rank_threshold=400, avg_rank_window=3, latest_timestamp=latest_timestamp)
    # 监控新晋排名前排
    rank_top_n(top_n=10, latest_timestamp=latest_timestamp)
    # 监控新前排板块共振
    # multi_concepts(top_n=10)


@timer
def run():
    # 监控涨跌比前排
    rise_ratio_top_n(top_n=10)
    # 监控排名提升
    rank_improvement(hours=24, threshold=300)
    rank_improvement(hours=1, threshold=250)
    rank_drop(top_n=50, rank_threshold=400, avg_rank_window=3)
    # 监控新晋排名前排
    rank_top_n(top_n=10)
    # 监控新前排板块共振
    multi_concepts(top_n=10)


if __name__ == "__main__":
    run()

