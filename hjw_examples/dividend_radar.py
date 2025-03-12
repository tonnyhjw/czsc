import datetime

from src.sig import dividend
from src.concept import utils
from src.concept.hot_rank import ConceptHotRank, RankType
from src.notify import notify_concept_radar


SUBJ_LV1 = "自动盯盘"   # 或改为"测试"
EDT: str = datetime.datetime.now().strftime('%Y%m%d')

mapping = {
    # 接口：dividend的字段
    'ts_code': 'TS代码',
    'end_date': '分红年度',
    'ann_date': '预案公告日',
    'div_proc': '实施进度',
    'stk_div': '每股送转',
    'stk_bo_rate': '每股送股比例',
    'stk_co_rate': '每股转增比例',
    'cash_div': '每股分红（税后）',
    'cash_div_tax': '每股分红（税前）',
    'record_date': '股权登记日',
    'ex_date': '除权除息日',
    'pay_date': '派息日',
    'div_listdate': '红股上市日',
    'imp_ann_date': '实施公告日',
    'base_date': '基准日',
    'base_share': '基准股本（万）',

    # 接口：daily_basic的字段
    'trade_date': '交易日期',
    'close': '当日收盘价',
    'turnover_rate': '换手率（%）',
    'turnover_rate_f': '换手率（自由流通股）',
    'volume_ratio': '量比',
    'pe': '市盈率（总市值/净利润，亏损的PE为空）',
    'pe_ttm': '市盈率（TTM，亏损的PE为空）',
    'pb': '市净率（总市值/净资产）',
    'ps': '市销率',
    'ps_ttm': '市销率（TTM）',
    'dv_ratio': '股息率（%）',
    'dv_ttm': '股息率（TTM）（%）',
    'total_share': '总股本（万股）',
    'float_share': '流通股本（万股）',
    'free_share': '自由流通股本（万）',
    'total_mv': '总市值（万元）',
    'circ_mv': '流通市值（万元）',

    # 常用字段
    'stock_name': '股票名',
    'top_concepts': '概念题材'
}

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
        results_df = results_df.rename(columns=mapping)

        notify_concept_radar(results_df, email_subject)


if __name__ == '__main__':
    run()