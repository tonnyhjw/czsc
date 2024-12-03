from datetime import datetime, timedelta

from src.concept import detect
from src.decorate import timer
from src.notify import notify_concept_radar
from src.concept.utils import *

EXCLUDE_CODES = ["BK0815", "BK0816", "BK0817", "BK1050", "BK1051"]
SUBJ_LV1 = "自动盯盘"   # 或改为"测试"
EDT: str = datetime.now().strftime('%Y%m%d')


def rise_ratio_top_n(top_n=10):
    store_field = 'code'
    previous_result_file = "rise_ratio_previous_code.pkl"
    result = detect.get_latest_concepts_with_criteria(top_n)

    # 如果有新增概念，触发警报
    if new_element(store_field, previous_result_file, result):
        # 在此触发相关的警报操作，如发送邮件或消息等
        result_df = pd.DataFrame(result)
        result_df = embed_code_href(result_df)
        email_subject = f"[{SUBJ_LV1}][概念板块][A股]{EDT}发现{len(result_df)}个头部涨跌比概念(全涨+top{top_n})"

        notify_concept_radar(result_df, email_subject)

    # 存储当前的 result 以便下次对比
    store_current_result_code(result, previous_result_file, store_field)


def rank_improvement(hours: int = 24, threshold: int = 5):
    now = datetime.now()
    start_time = now - timedelta(hours=hours)  # 查询过去 1 天的数据
    end_time = now

    result = detect.detect_rank_improvement(start_time, end_time, threshold)
    if result:
        # 将 result 转换为 DataFrame 并返回
        result_df = pd.DataFrame(result)
        result_df = embed_code_href(result_df)
        result_df.sort_values(by='rank_improvement', ascending=False, inplace=True)
        email_subject = f"[{SUBJ_LV1}][概念板块][A股]{EDT}发现{len(result_df)}个{hours}小时内排名提升超过{threshold}的概念"

        notify_concept_radar(result_df, email_subject)


def rank_top_n(top_n=10):
    store_field = 'code'
    previous_result_file = "rank_top_n_code.pkl"
    result = detect.get_top_n_concepts_excluding(top_n, exclude_codes=EXCLUDE_CODES)

    # 如果有新增概念板块共振个股，触发警报
    if new_element(store_field, previous_result_file, result):

        # 将 result 转换为 DataFrame 并返回
        result_df = pd.DataFrame(result)
        result_df = embed_code_href(result_df)
        email_subject = f"[{SUBJ_LV1}][概念板块][A股]{EDT}排名前{top_n}的概念"

        notify_concept_radar(result_df, email_subject)
    # 存储当前的 result 以便下次对比
    store_current_result_code(result, previous_result_file, store_field)


def multi_concepts(top_n=10, min_concept_count=2):
    store_field = 'symbol'
    previous_result_file = "multi_concepts_symbol.pkl"

    result = detect.get_stocks_in_multiple_concepts(top_n, min_concept_count, EXCLUDE_CODES)

    # 如果有新增概念板块共振个股，触发警报
    new_elem = new_element(store_field, previous_result_file, result)
    if new_elem:
        # 在此触发相关的警报操作，如发送邮件或消息等
        result_df = pd.DataFrame(result)
        result_df = result_df[result_df['symbol'].isin(new_elem)]
        result_df = embed_symbol_href(result_df)
        email_subject = f"[{SUBJ_LV1}][概念板块][A股]{EDT}发现{len(result_df)}个前{top_n}概念板块共振"

        notify_concept_radar(result_df, email_subject)

    # 存储当前的 result 以便下次对比
    store_current_result_code(result, previous_result_file, store_field)


@timer
def run():
    # 监控涨跌比前排
    rise_ratio_top_n(top_n=10)
    # 监控排名提升
    rank_improvement(hours=24, threshold=300)
    rank_improvement(hours=1, threshold=50)
    # 监控新晋排名前排
    rank_top_n(top_n=10)
    # 监控新前排板块共振
    multi_concepts(top_n=10)


if __name__ == "__main__":
    run()

