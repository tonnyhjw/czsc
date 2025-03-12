import datetime

from src.sig import dividend
from src.concept import utils
from src.concept.hot_rank import ConceptHotRank, RankType
from src.notify import notify_concept_radar


SUBJ_LV1 = "自动盯盘"   # 或改为"测试"
EDT: str = datetime.datetime.now().strftime('%Y%m%d')

def run(concept_top_n: int = 30, concept_rank_threshold: int = 10):
    selector = dividend.DividendStockSelector()
    trade_date = datetime.datetime.now().strftime('%Y%m%d')


    # 选择市值小于50亿元的股票
    selected_stocks = selector.select_stocks(
        market_cap_threshold=100 * 10000,  # 50亿元
        top_n=300,  # 前30只股票
        trade_date=trade_date  # 最近的交易日期
    )

    concepts_end_date = datetime.datetime.strptime(trade_date, '%Y%m%d')
    concepts_start_date = concepts_end_date - datetime.timedelta(days=15)

    # 创建分析器实例
    _analyzer = ConceptHotRank()

    # 分析排名靠前的概念
    top_concepts = _analyzer.analyze_concepts(
        start_date=concepts_start_date,
        end_date=concepts_end_date,
        rank_threshold=concept_rank_threshold,
        limit_n=concept_top_n,
        rank_type=RankType.TOP
    )
    top_concepts_codes = [c.get('code') for c in top_concepts]

    # 与主题结合的示例
    results_df = selector.add_top_concepts_to_stocks(selected_stocks, top_concepts_codes)

    if results_df.empty:
       print("没有符合条件的分红个股")
    else:
        email_subject = f"[{SUBJ_LV1}][分红][A股]{EDT}发现{len(results_df)}个个股今天分红除权"
        results_df = utils.embed_ts_code_href(results_df)
        notify_concept_radar(results_df, email_subject)


if __name__ == '__main__':
    run()