
def sort_by_industry(item_dictionary):
    return item_dictionary['industry']


def sort_by_profit(item_dictionary):
    return item_dictionary['expect_profit(%)']


def sort_by_fx_pwr(item_dictionary):
    return item_dictionary['fx_pwr']


def sort_by_fx_dt(item_dictionary):
    return item_dictionary['latest_fx_dt']


def sort_by_signals(item_dictionary):
    return item_dictionary['signals']


def sort_by_gross_profit(item_dictionary):
    return item_dictionary['gross_profit']

TITLE_MAPPING = {
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
    'total_div': '总分红（万元，税后）',
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

    # 题材数据库字段
    'name': '板块名称',
    'code': '板块代码',
    'symbol': '股票代码',
    'stock_name': '企业名',

    'rank': '排名',
    'rise_ratio': '涨跌比',
    'up_count': '上涨个股',
    'down_count': '下跌个股',

    # 常用字段
    'top_concepts': '头部概念',
    'concepts': '概念',
    'sequence_number': '序号',
    'count': '出现次数',
    'signals': '买点类型',
    'fx_pwr': '分型强度',
    'bp_date': '买点日期',
    'freq': '级别'
}