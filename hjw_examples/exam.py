import datetime
import os
import pprint

from src.stock_process import *
from database import history
from src.decorate import *
import czsc

# cache_path = os.getenv("TS_CACHE_PATH", os.path.expanduser("~/.ts_data_cache"))
# dc = czsc.DataClient(url="http://api.tushare.pro", cache_path=cache_path)
# os.environ['czsc_min_bi_len'] = '7'


def play_day_trend_reverse():
    row = dict(ts_code="TSLA", symbol="TSLA", name="Tesla, Inc.", industry="Automobile Manufacturers")
    sdt, edt = "20180501", "20240612"
    # result = trend_reverse_ubi_entry(row=row, sdt=sdt, edt=edt, freq="D", fx_dt_limit=5)
    result = trend_reverse_ubi_entry_us(row=row, sdt=sdt, edt=edt, freq="D", fx_dt_limit=5)
    pprint.pprint(result)


def play_pzbc():
    row = dict(ts_code="600187.sh", symbol="600187", name="国中水务", industry="污水处理")
    sdt, edt = "20180501", "20240712"
    result = bottom_pzbc(row, sdt, edt, "W", fx_dt_limit=30)
    pprint.pprint(result)


@timer
def xd_dev():
    from src.xd.analyze_by_break import analyze_xd
    from src.sig_xd import get_xd_zs_seq
    row = dict(ts_code="300510.SZ", symbol="300510", name="金冠股份", industry="电气设备")
    sdt, edt = "20180501", "20240712"
    c = row_2_czsc(row, sdt, edt, "D")
    xds = analyze_xd(c.bi_list)
    zs_seq = get_xd_zs_seq(xds)
    for zs in zs_seq[-3:]:
        print(len(zs.xds))
        pprint.pprint(zs)
        pprint.pp(zs.xds)
    print(zs_seq[-1].is_valid)


def bi_dev():
    os.environ['czsc_min_bi_len'] = '7'

    row = dict(ts_code="601969.sh", symbol="601969", name="海南矿业", industry="普钢")
    sdt, edt = "20180501", "20240418"

    c = row_2_czsc(row, sdt, edt, "D")
    pprint.pprint(c.bi_list)
    pprint.pprint(c.ubi)


@timer
def us_data_yf(symbol="TSLA"):
    import yfinance as yf
    stock = yf.Ticker(symbol)
    info = stock.info
    pprint.pprint(info)
    return symbol, info.get('sector', 'N/A'), info.get('industry', 'N/A')


@timer
def us_raw_bar():
    from czsc import home_path
    from src.connectors.yf_cache import YfDataCache

    dc = YfDataCache(home_path, refresh=True)
    bars = dc.history("TSLA", "20180101")
    # pprint.pprint(bars)


def us_members():
    import pandas as pd
    from src.connectors.yf_cache import YfDataCache
    ydc = YfDataCache(home_path)  # 在每个进程中创建独立的实例

    sp500 = ydc.wiki_snp500_member()
    nd100 = ydc.nsdq_100_member()
    df_combined = pd.concat([nd100, sp500], axis=0, ignore_index=True)
    for index, row in df_combined.iterrows():
        print(index, row)


def new_stock_break_ipo(sdt="20230101", edt="20240430"):
    from czsc.data import TsDataCache
    from src.sig.powers import break_ipo_high

    tdc = TsDataCache(home_path)
    stock_basic = tdc.stock_basic()  # 只用于读取股票基础信息
    total_stocks = len(stock_basic)
    results = []  # 用于存储所有股票的结果

    for index, row in stock_basic.iterrows():
        _ts_code = row.get("ts_code")
        _symbol = row.get('symbol')
        _hs = _ts_code.split(".")[-1]
        bars = tdc.pro_bar(_ts_code, start_date=sdt, end_date=edt, freq="D", asset="E", adj='qfq', raw_bar=True)
        if len(bars) > 250:
            continue
        try:
            c = CZSC(bars)
            if break_ipo_high(c):
                print(f"https://xueqiu.com/S/{_hs}{_symbol}")
        except Exception as e_msg:
            pass


@timer
def play_sw_members():
    from czsc.connectors import ts_connector
    members = ts_connector.get_sw_members(level="L3")
    for index, row in members.iterrows():
        print(index, row["industry_name"], row["con_code"])


@timer
def get_hk_hold():
    holds = dc.hk_hold(trade_date='20240805', exchange='SZ')
    holds = holds.sort_values('ratio', ascending=False, ignore_index=True)
    for index, hold in holds.iterrows():
        print(index, hold)


def db_dev():
    business_date = "20240205"
    _business_date = datetime.datetime.strptime(business_date, "%Y%m%d")
    buy_points = history.query_all_buy_point('300059', edt=_business_date)
    pprint.pprint(list(buy_points.dicts()))


@timer
def get_hsgt():
    top_stocks = dc.hsgt_top10(trade_date='20240805', market_type='1')
    top_stocks = top_stocks.sort_values('net_amount', ascending=False, ignore_index=True)
    print(top_stocks)
    # for index, stock in top_stocks.iterrows():
    #     print(index, "="*30)
    #     print(stock)


def elevate_dev():
    from src.stock_process import zs_elevate_3rd_buy_bi
    row = dict(ts_code="601328.sh", symbol="601328", name="交通银行", industry="国有银行")
    sdt, edt = "20180101", "20240321"
    result = zs_elevate_3rd_buy_bi(row, sdt, edt, "D")
    pprint.pprint(result)
    return


def concept_dev():
    import akshare as ak

    # 获取所有概念板块名称
    concept_df = ak.stock_board_concept_name_em()

    # 查询所属概念板块 445
    for _, row in concept_df.iterrows():
        print(row)


def concept_stock_dev():
    import akshare as ak

    # 获取概念板块内的“中字头”个股
    concept_stocks = ak.stock_board_concept_cons_em(symbol="抖音小店")

    # 逐行打印
    for index, row in concept_stocks.iterrows():
        print(row.to_dict())


def print_concept_latest_buypoints():
    """
    美化打印最新买点查询结果

    Args:
        results (List[Dict]): 最新买点查询结果
    """
    from src.concept.detect import find_concept_stocks_with_latest_buypoints
    concept_code = "BK0892" # 替换为你想查询的板块代码
    start_date = datetime.date(2024, 12, 1)
    end_date = datetime.date(2024, 12, 13)

    results = find_concept_stocks_with_latest_buypoints(
        concept_code,
        start_date,
        end_date
    )

    if not results:
        print("未找到任何买点信息。")
        return

    print(f"共找到 {len(results)} 只股票的最新买点：")
    for stock in results:
        print(f"\n股票代码: {stock['symbol']} - {stock['stock_name']}")
        print(f"  - 日期: {stock['bp_date']}")
        print(f"    信号: {stock['signals']}")
        print(f"    分型强度: {stock['fx_pwr']}")


def concept_radar_examination(n=3):
    from czsc import home_path
    from czsc.data import TsDataCache
    from hjw_examples import concept_radar

    today = datetime.datetime.now()
    sdt = (today - datetime.timedelta(days=n)).strftime("%Y%m%d")
    edt = today.strftime("%Y%m%d")
    trade_dates = TsDataCache(home_path).get_dates_span(sdt, edt, is_open=True)
    for target_day in trade_dates:
        target_day = datetime.datetime.strptime(target_day, "%Y%m%d").strftime("%Y-%m-%d")
        target_day = f"{target_day} 11:30"
        print(f"测试日期{target_day}")
        concept_radar.SUBJ_LV1 = "测试"
        concept_radar.EDT = target_day
        concept_radar.demo(latest_timestamp=target_day)


def rank_chart():
    from src.concept.rank_chart import ConceptRankChart
    # 使用示例
    analyzer = ConceptRankChart()

    # 分析数据
    concept_codes = ['BK1173', 'BK0695', 'BK0519']
    analyzer.analyze(concept_codes)

    # 生成并保存HTML图表
    html_chart = analyzer.generate_chart('html')
    analyzer.save_chart(html_chart, 'concept_ranks.html', 'html')

    # 生成并保存PNG图表
    png_chart = analyzer.generate_chart('png')
    analyzer.save_chart(png_chart, 'concept_ranks.png', 'png')


def peek_concept_buy_points():
    from src.concept import detect, utils
    bp_days_limit = 5
    latest_timestamp = None
    concepts = [
        {"code": "BK0907"},
        {"code": "BK0578"},
        #{"code": "BK1145"},
    ]
    bp_sdt, bp_edt = utils.get_recent_n_trade_dates_boundary(bp_days_limit, latest_timestamp)
    bp_sdt = datetime.datetime.strptime(bp_sdt, '%Y%m%d').date()
    bp_edt = datetime.datetime.strptime(bp_edt, '%Y%m%d').date()
    print(bp_sdt, bp_edt)
    results = detect.get_buypoints_for_multiple_concepts(concepts, bp_sdt, bp_edt)
    pprint.pp(results)


def hot_rank_demo():
    from src.concept.hot_rank import ConceptHotRank, RankType
    START_DATE = datetime.datetime(2025, 1, 13)
    END_DATE = datetime.datetime(2025, 2, 27)
    RANK_THRESHOLD = 10
    N = 5

    # 创建分析器实例
    analyzer = ConceptHotRank()

    # 分析排名靠前的概念
    top_results = analyzer.analyze_concepts(
        start_date=START_DATE,
        end_date=END_DATE,
        rank_threshold=RANK_THRESHOLD,
        limit_n=N,
        rank_type=RankType.TOP
    )

    # 分析排名靠后的概念（假设总共有100个概念）
    bottom_results = analyzer.analyze_concepts(
        start_date=START_DATE,
        end_date=END_DATE,
        rank_threshold=90,  # 假设排名90以后算落后
        limit_n=N,
        rank_type=RankType.BOTTOM
    )
    
    # 打印日期
    print(f"{START_DATE=}, {END_DATE=}")
    # 打印两种结果
    analyzer.print_results(top_results, RankType.TOP)
    analyzer.print_results(bottom_results, RankType.BOTTOM)


if __name__ == '__main__':
    # play_day_trend_reverse()
    # play_pzbc()
    # result = run_single_stock_backtest(ts_code='000415.SZ', edt='20240614', freq="D")
    # pprint.pprint(result.get("sharpe_ratio"))
    # xd_dev()
    # bi_dev()
    # ma_pzbc_dev()
    # us_data_yf()
    # us_raw_bar()
    # us_members()
    # new_stock_break_ipo()
    # play_sw_members()
    # get_hk_hold()
    # get_hsgt()
    # money_flow_global()
    # elevate_dev()
    # concept_dev()
    # concept_radar_examination(15)
    # rank_chart()
    peek_concept_buy_points()
    # hot_rank_demo()
