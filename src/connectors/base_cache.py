# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod

from czsc.data.ts_cache import *
from czsc.objects import RawBar


class BaseDataCache(ABC):
    """K线数据缓存父类"""

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

    @property
    @abstractmethod
    def prefix(self) -> str:
        """子类必须定义缓存路径目录名"""
        pass

    @property
    @abstractmethod
    def api_names(self) -> List[str]:
        """子类必须定义API名"""
        pass

    def __prepare_api_path(self):
        """给每个tushare数据接口创建一个缓存路径"""
        cache_path = self.cache_path
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
                    print(f"数据缓存清理失败，请手动删除缓存文件夹：{self.cache_path}")

    @abstractmethod
    def history(self, symbol, start_date=None, end_date=None, freq="D", raw_bar=True):
        """获取日线以上数据

        :param symbol:
        :param start_date:
        :param end_date:
        :param freq:
        :param raw_bar:
        :return:
        """
        # cache_path = self.api_path_map["history"]
        # file_cache = os.path.join(cache_path, f"history_{symbol}#{self.sdt}_{freq}.feather")
        #
        # if not self.refresh and os.path.exists(file_cache):
        #     kline = pd.read_feather(file_cache)
        #     if self.verbose:
        #         print(f"history: read cache {file_cache}")
        # else:
        #     start_date_ = (pd.to_datetime(self.sdt) - timedelta(days=1000)).strftime(self.date_fmt)
        #     ticker = yf.Ticker(symbol)
        #     kline = ticker.history(start=start_date_, end=self.edt)
        #     kline = kline.reset_index()
        #     kline = kline[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        #     kline.columns = ['dt', 'open', 'high', 'low', 'close', 'vol']
        #     kline = kline.assign(symbol=symbol)
        #     kline["dt"] = pd.to_datetime(kline["dt"], format=self.date_fmt)
        #     kline["dt"] = kline["dt"].dt.tz_localize(None)
        #     # update_bars_return(kline)
        #     kline.to_feather(file_cache)
        #
        # if start_date:
        #     kline = kline[kline["dt"] >= pd.to_datetime(start_date)]
        # if end_date:
        #     kline = kline[kline["dt"] <= pd.to_datetime(end_date)]
        #
        # kline.reset_index(drop=True, inplace=True)
        # if raw_bar:
        #     kline = format_kline(kline, freq=self.freq_map[freq])
        # return kline
        pass

    @staticmethod
    def format_kline(kline: pd.DataFrame, freq: Freq) -> List[RawBar]:
        """K线数据转换

        :param kline: 数据接口返回的K线数据
        :param freq: K线周期
        :return: 转换好的K线数据
        """
        bars = []
        kline = kline.sort_values(by="dt", ascending=True, ignore_index=True)
        records = kline.to_dict("records")

        for i, record in enumerate(records):
            amount = int(record.get("amount", 0))
            # 将每一根K线转换成 RawBar 对象
            bar = RawBar(
                symbol=record["symbol"],
                dt=pd.to_datetime(record["dt"]),
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


