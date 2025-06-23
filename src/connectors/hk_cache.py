import os
import pprint

import pandas as pd
from typing import List
from datetime import timedelta, datetime

from src.connectors.base_cache import BaseDataCache

import akshare as ak


class HKDataCache(BaseDataCache):
    period_map = {
        "D": "daily",
        "W": "weekly",
        "M": "monthly",
    }

    @property
    def prefix(self) -> str:
        return "HK_CACHE"

    @property
    def api_names(self) -> List[str]:
        return [
            "stock_hk_hist",    # 历史行情数据-东财
            "stock_hk_daily",   # 历史行情数据-新浪
            "stock_hk_ggt_components_em"    # 港股通成份股
        ]

    def history(self, symbol, start_date=None, end_date=None, freq="D", raw_bar=True):
        """获取日线以上数据
        :param symbol:
        :param start_date:
        :param end_date:
        :param freq:
        :param raw_bar:
        :return:
        """
        cache_path = self.api_path_map["stock_hk_hist"]
        file_cache = os.path.join(cache_path, f"stock_hk_hist_{symbol}#{self.sdt}_{freq}.feather")

        if not self.refresh and os.path.exists(file_cache):
            kline = pd.read_feather(file_cache)
            if self.verbose:
                print(f"history: read cache {file_cache}")
        else:
            start_date_ = (pd.to_datetime(self.sdt) - timedelta(days=1000)).strftime(self.date_fmt)
            kline = ak.stock_hk_hist(symbol=symbol, start_date=start_date_, end_date=end_date,
                                      period=self.period_map.get(freq))
            kline = kline.reset_index()
            kline = kline[['日期', '开盘', '最高', '最低', '收盘', '成交量']]
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
            kline = self.format_kline(kline, freq=self.freq_map[freq])
        return kline

    def ggt_components(self):
        """获取港股通成分股名单

        :return:
        """
        cache_path = self.api_path_map["stock_hk_ggt_components_em"]
        file_cache = os.path.join(cache_path, f"stock_hk_ggt_components_em.feather")
        if not self.refresh and os.path.exists(file_cache):
            ggt_components_df = pd.read_feather(file_cache)
            if self.verbose:
                print(f"wiki_snp500_member: read cache {file_cache}")
        else:
            ggt_components_df = ak.stock_hk_ggt_components_em()
            ggt_components_df = ggt_components_df.reset_index()
            ggt_components_df = ggt_components_df[["代码", "名称"]]
            ggt_components_df.columns = ["symbol", "name"]
            ggt_components_df['ts_code'] = ggt_components_df['symbol'] + '.HK'  # 最优方案
            ggt_components_df.to_feather(file_cache)

        return ggt_components_df

if __name__ == '__main__':
    hkdc = HKDataCache('')
    data = hkdc.history('00981', end_date='20250623')
    pprint.pp(data)