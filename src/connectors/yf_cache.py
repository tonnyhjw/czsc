# -*- coding: utf-8 -*-
import pprint

import yfinance as yf
import requests
from bs4 import BeautifulSoup

from czsc.data.ts_cache import *
from czsc.objects import RawBar


class YfDataCache:
    """yfinance 数据缓存"""

    def __init__(self, data_path, refresh=False, sdt="20180101", edt=datetime.now()):
        """

        :param data_path: 数据路径
        :param refresh: 是否刷新缓存
        :param sdt: 缓存开始时间
        :param edt: 缓存结束时间
        """
        self.date_fmt = "%Y-%m-%d"
        self.verbose = envs.get_verbose()
        self.refresh = refresh
        self.sdt = pd.to_datetime(sdt).strftime(self.date_fmt)
        self.edt = pd.to_datetime(edt).strftime(self.date_fmt)
        self.data_path = data_path
        self.prefix = "YF_CACHE"
        self.cache_path = os.path.join(self.data_path, self.prefix)
        os.makedirs(self.cache_path, exist_ok=True)
        self.pro = pro
        self.__prepare_api_path()

        self.freq_map = {
            "1min": Freq.F1,
            "5min": Freq.F5,
            "15min": Freq.F15,
            "30min": Freq.F30,
            "60min": Freq.F60,
            "D": Freq.D,
            "W": Freq.W,
            "M": Freq.M,
        }

    def __prepare_api_path(self):
        """给每个tushare数据接口创建一个缓存路径"""
        cache_path = self.cache_path
        self.api_names = [
            "wiki_snp500_member",
            "history",
            "nsdq_100_member"
        ]
        self.api_path_map = {k: os.path.join(cache_path, k) for k in self.api_names}

        for k, path in self.api_path_map.items():
            os.makedirs(path, exist_ok=True)

    def clear(self):
        """清空缓存"""
        for path in os.listdir(self.data_path):
            if path.startswith(self.prefix):
                path = os.path.join(self.data_path, path)
                shutil.rmtree(path)
                if self.verbose:
                    print(f"clear: remove {path}")
                if os.path.exists(path):
                    print(f"yfinance 数据缓存清理失败，请手动删除缓存文件夹：{self.cache_path}")

    # ------------------------------------ 原生接口----------------------------------------------
    def wiki_snp500_member(self):
        """获取标普500成分股
        数据源 https://en.wikipedia.org/wiki/List_of_S%26P_500_companies
        :return:
        """
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        cache_path = self.api_path_map["wiki_snp500_member"]
        file_cache = os.path.join(cache_path, f"wiki_snp500_member.feather")
        if not self.refresh and os.path.exists(file_cache):
            df = pd.read_feather(file_cache)
            if self.verbose:
                print(f"wiki_snp500_member: read cache {file_cache}")
        else:
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table', {'id': 'constituents'})
            df = pd.read_html(str(table))[0]
            df = df.reset_index(drop=True, inplace=False)
            df = df[['Symbol', 'Security', 'GICS Sector', 'GICS Sub-Industry']]
            df.columns = ['symbol', 'name', 'sector', 'industry']
            df['ts_code'] = df['symbol']
            df.to_feather(file_cache)
        return df

    def nsdq_100_member(self):
        """获取纳斯达克100成分股
        数据源 https://api.nasdaq.com/api/quote/list-type/nasdaq100
        :return:
        """
        url = "https://api.nasdaq.com/api/quote/list-type/nasdaq100"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        cache_path = self.api_path_map["nsdq_100_member"]
        file_cache = os.path.join(cache_path, f"nsdq_100_member.feather")
        if not self.refresh and os.path.exists(file_cache):
            df = pd.read_feather(file_cache)
            if self.verbose:
                print(f"wiki_snp500_member: read cache {file_cache}")
        else:
            response = requests.get(url, headers=headers)
            assert response.status_code == 200, f"请求nasdaq100状态码{response.status_code=}"
            data = response.json()
            data = data.get('data')
            assert 'data' in data and 'rows' in data['data'], "请求nasdaq100响应数据结构异常"
            components = data['data']['rows']
            df = pd.DataFrame(components)
            df = df.reset_index(drop=True, inplace=False)
            df = df[['symbol', 'companyName']]
            # 重命名列
            df.columns = ['symbol', 'name']
            df['sector'] = ''
            df['industry'] = ''
            for index, row in df.iterrows():
                _stock_info = self.get_stock_info(row['symbol'])
                df.at[index, 'sector'] = _stock_info.get('sector', '')
                df.at[index, 'industry'] = _stock_info.get('industry', '')
                time.sleep(0.2)  # 在每次请求之间暂停0.5秒，避免请求过于频繁
            df['ts_code'] = df['symbol']
            df.to_feather(file_cache)
        return df

    def get_us_stock_list(self):
        sp500 = self.wiki_snp500_member()
        nd100 = self.nsdq_100_member()
        us_stock_list = pd.concat([nd100, sp500], axis=0, ignore_index=True)
        return us_stock_list

    def history(self, symbol, start_date=None, end_date=None, freq="D", raw_bar=True):
        """获取日线以上数据

        https://tushare.pro/document/2?doc_id=109

        :param symbol:
        :param start_date:
        :param end_date:
        :param freq:
        :param raw_bar:
        :return:
        """
        cache_path = self.api_path_map["history"]
        file_cache = os.path.join(cache_path, f"history_{symbol}#{self.sdt}_{freq}.feather")

        if not self.refresh and os.path.exists(file_cache):
            kline = pd.read_feather(file_cache)
            if self.verbose:
                print(f"history: read cache {file_cache}")
        else:
            start_date_ = (pd.to_datetime(self.sdt) - timedelta(days=1000)).strftime(self.date_fmt)
            ticker = yf.Ticker(symbol)
            kline = ticker.history(start=start_date_, end=self.edt)
            kline = kline.reset_index()
            kline = kline[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
            kline.columns = ['dt', 'open', 'high', 'low', 'close', 'vol']
            kline = kline.assign(symbol=symbol)
            kline["dt"] = pd.to_datetime(kline["dt"], format=self.date_fmt)
            kline["dt"] = kline["dt"].dt.tz_localize(None)
            # update_bars_return(kline)
            kline.to_feather(file_cache)

        if start_date:
            kline = kline[kline["dt"] >= pd.to_datetime(start_date)]
        if end_date:
            kline = kline[kline["dt"] <= pd.to_datetime(end_date)]

        kline.reset_index(drop=True, inplace=True)
        if raw_bar:
            kline = format_kline(kline, freq=self.freq_map[freq])
        return kline

    @staticmethod
    def get_stock_info(symbol):
        stock = yf.Ticker(symbol)
        info = stock.info
        return info


def format_kline(kline: pd.DataFrame, freq: Freq) -> List[RawBar]:
    """yfinance K线数据转换

    :param kline: yfinance 数据接口返回的K线数据
    :param freq: K线周期
    :return: 转换好的K线数据
    """
    bars = []
    kline = kline.sort_values('dt', ascending=True, ignore_index=True)
    records = kline.to_dict("records")

    for i, record in enumerate(records):
        amount = int(record.get("amount", 0))
        # 将每一根K线转换成 RawBar 对象
        bar = RawBar(
            symbol=record["symbol"],
            dt=pd.to_datetime(record['dt']),
            id=i,
            freq=freq,
            open=record["open"],
            close=record["close"],
            high=record["high"],
            low=record["low"],
            vol=record["vol"],  # 成交量，单位：股
            amount=amount,  # 成交额，单位：元
        )
        bars.append(bar)
    return bars


