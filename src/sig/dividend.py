import os
import datetime
import pandas as pd
from tushare import trade_cal

import czsc
from src.concept.hot_rank import ConceptHotRank, RankType
from src.concept.detect import get_stock_top_concepts, get_stock_name_by_symbol

cache_path = os.getenv("TS_CACHE_PATH", os.path.expanduser("~/.ts_data_cache"))
dc = czsc.DataClient(url="http://api.tushare.pro", cache_path=cache_path)

class DividendStockSelector:
    def __init__(self):
        self.pro = dc
        
    def get_dividend_data(self, ex_date=None):
        """
        获取股票的分红数据
        
        参数:
            ex_date (str, 可选): 开始日期，格式为YYYYMMDD

        返回:
            pandas.DataFrame: 分红数据
        """
        # 如果未提供结束日期，则使用当前日期
        if ex_date is None:
            ex_date = datetime.datetime.now().strftime('%Y%m%d')

        # 获取分红数据
        dividend_data = self.pro.dividend(
            ex_date=ex_date,
            fields='ts_code,end_date,ann_date,div_proc,stk_bo_rate,stk_co_rate,cash_div,cash_div_tax,ex_date'
        )

        # 仅筛选已实施的分红
        dividend_data = dividend_data[dividend_data['div_proc'] == '实施']
        return dividend_data


    def get_market_data(self, trade_date=None):
        """
        获取所有股票的最新市场数据
        
        参数:
            trade_date (str, 可选): 交易日期，格式为YYYYMMDD
            
        返回:
            pandas.DataFrame: 市场数据
        """
        # 如果未提供交易日期，则使用当前日期
        if trade_date is None:
            trade_date = datetime.datetime.now().strftime('%Y%m%d')
            
        # 获取每日基本数据
        market_data = self.pro.daily_basic(
            trade_date=trade_date,
            fields='ts_code,trade_date,close,dv_ratio,dv_ttm,total_share,float_share,total_mv,circ_mv'
        )
        
        return market_data
    
    def select_stocks(self, market_cap_threshold=1000000, top_n=20, trade_date=None):
        """
        基于市值和分红比例选择股票
        
        参数:
            market_cap_threshold (float): 最大市值上限（单位：万元）
            top_n (int): 返回的排名靠前的股票数量
            trade_date (str, 可选): 市场数据的交易日期
            start_date (str, 可选): 分红数据的开始日期
            end_date (str, 可选): 分红数据的结束日期
            
        返回:
            pandas.DataFrame: 带有市场和分红信息的选定股票
        """
        # 如果未提供日期，则使用当前日期
        if trade_date is None:
            trade_date = datetime.datetime.now().strftime('%Y%m%d')

        # 获取分红数据
        dividend_data = self.get_dividend_data(trade_date)

        # 获取市场数据
        market_data = self.get_market_data(trade_date)

        # 按市值筛选股票
        filtered_stocks = market_data[market_data['total_mv'] < market_cap_threshold]

        # 与分红数据合并
        merged_data = pd.merge(
            filtered_stocks,
            dividend_data,
            on='ts_code',
            how='inner'
        )

        # 按分红比例（dv_ttm）降序排序
        merged_data = merged_data.sort_values(by='dv_ttm', ascending=False)

        # 选择排名靠前的N只股票
        top_stocks = merged_data.head(top_n)
        
        # 准备最终输出
        result = top_stocks[['ts_code', 'stk_bo_rate', 'stk_co_rate', 'cash_div', 'total_share', 'close', 'total_mv',
                             'circ_mv', 'dv_ttm', 'ann_date', 'ex_date']]
        result = result.reset_index(drop=True)
        
        return result

    @staticmethod
    def email_format(dividend_stocks, top_concepts_codes):
        """
        将热门概念添加到股息股票DataFrame中。

        参数:
            dividend_stocks (pd.DataFrame): 包含股票信息的DataFrame
            top_concepts_codes (List[str]): 热门概念代码列表

        返回:
            pd.DataFrame: 添加了'top_concepts'列和'stock_name'列的dividend_stocks
        """
        # 创建输入DataFrame的副本，避免修改原始数据
        output_stocks = dividend_stocks.copy()

        # 添加新列'top_concepts'，用于存储拼接的概念名称
        output_stocks['top_concepts'] = ''

        # 如果DataFrame中没有stock_name列，添加该列
        if 'stock_name' not in output_stocks.columns:
            output_stocks['stock_name'] = ''

        for index, row in output_stocks.iterrows():
            # 从ts_code中提取股票代码（去掉交易所后缀）
            symbol_format = row['ts_code'].split('.')[0]

            # 调用独立模块获取该股票的热门概念
            concept_names = get_stock_top_concepts(symbol_format, top_concepts_codes)

            # 用逗号连接概念名称
            output_stocks.at[index, 'top_concepts'] = ','.join(concept_names)

            # 计算分红总额
            output_stocks.at[index, 'total_div'] = row['cash_div'] * row['total_share']

            # 如果stock_name为空，调用独立模块获取股票名称
            if not output_stocks.at[index, 'stock_name']:
                output_stocks.at[index, 'stock_name'] = get_stock_name_by_symbol(symbol_format)

        # 确定列的顺序，将stock_name放在第二列
        columns = list(output_stocks.columns)
        if 'stock_name' in columns:
            columns.remove('stock_name')
            columns.insert(1, 'stock_name')

        # 将top_concepts放在第三列
        if 'top_concepts' in columns:
            columns.remove('top_concepts')
            columns.insert(2, 'top_concepts')

        # 将total_dividend放在第四列
        if 'total_div' in columns:
            columns.remove('total_div')
            columns.insert(3, 'total_div')

        # 移除 每股分红（税后）；总股本（万股）；流通市值（万元）
        columns.remove('cash_div')
        columns.remove('total_share')
        columns.remove('circ_mv')
        output_stocks = output_stocks[columns]
        # 按分红总额排序
        output_stocks = output_stocks.sort_values(by='total_div', ascending=False)

        return output_stocks

# 使用示例
if __name__ == "__main__":
    selector = DividendStockSelector()
    concept_top_n, concept_rank_threshold = 30, 10
    start_date = '20250301'
    end_date = None
    # 如果未提供结束日期，则使用当前日期
    if end_date is None:
        end_date = datetime.datetime.now().strftime('%Y%m%d')

    # 如果未提供开始日期，默认为一年前
    if start_date is None:
        start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y%m%d')

    trade_dates = dc.trade_cal(start_date=start_date, end_date=end_date, fields="cal_date")
    for index, row in trade_dates.iterrows():
        trade_date = row.get("cal_date")
        print(trade_date)

        # 选择市值小于50亿元的股票
        selected_stocks = selector.select_stocks(
            market_cap_threshold=100*10000,  # 50亿元
            top_n=30,  # 前30只股票
            trade_date=trade_date  # 最近的交易日期
        )

        # print("市值低且分红高的股票:")
        # print(selected_stocks)

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
        top_concepts_codes_ = [c.get('code') for c in top_concepts]

        # 与主题结合的示例
        combined_results = selector.email_format(selected_stocks, top_concepts_codes_)

        print("\n与主题结合的股票:")
        print(combined_results)
