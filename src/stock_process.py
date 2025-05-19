import gc
import datetime
from loguru import logger
import traceback

from czsc import CZSC, home_path
from czsc.data import TsDataCache
from src.connectors.yf_cache import YfDataCache
from src.sig.trend_reverse import trend_reverse_bi, fake_xd_2nd_buy_point
from src.sig.pzbc import macd_pzbc_bi
from src.sig.powers import long_term_ma_support, long_term_ma_up
from src.sig.zs_elevate import third_buy_bi, third_buy_xd
from src.sig.utils import is_strong_bot_fx


logger.add("statics/logs/stock_process.log", rotation="10MB", encoding="utf-8", enqueue=True, retention="10 days")


def trend_reverse_ubi_entry(row, sdt, edt, freq: str, fx_dt_limit: int = 5):
    dc = TsDataCache(home_path)  # 在每个进程中创建独立的实例
    _ts_code = row.get('ts_code')
    _symbol = row.get('symbol')
    _name = row.get('name')
    _industry = row.get("industry")
    _hs = _ts_code.split(".")[-1]
    _edt = datetime.datetime.strptime(edt, "%Y%m%d")

    output = {}
    try:
        bars = dc.pro_bar(_ts_code, start_date=sdt, end_date=edt, freq=freq, asset="E", adj='qfq', raw_bar=True)
        # if "ST" in _name:
        #     return output
        c = CZSC(bars)
        _signals = trend_reverse_bi(c, edt=_edt, fx_dt_limit=fx_dt_limit, freq=freq, **row)
        logger.debug(_signals)

        for s_value in _signals.values():
            if "买" in s_value:
                s_value_detail = s_value.split("_")
                symbol_link = f'<a href="https://xueqiu.com/S/{_hs}{_symbol}">{_symbol}</a>'
                output = {
                    'name': _name,
                    'symbol': symbol_link,
                    'ts_code': _ts_code,
                    'signals': s_value_detail[0],
                    'fx_pwr': s_value_detail[1],
                    'expect_profit(%)': round(float(s_value_detail[2]) * 100, 2),
                    'industry': _industry
                }
    except Exception as e_msg:
        tb = traceback.format_exc()  # 获取 traceback 信息
        logger.critical(f"{_ts_code} {_name}出现报错，{e_msg}\nTraceback: {tb}")

    finally:
        gc.collect()
        return output


def trend_reverse_ubi_entry_us(row, sdt, edt, freq: str, fx_dt_limit: int = 5):
    ydc = YfDataCache(home_path)  # 在每个进程中创建独立的实例
    _symbol = row.get('symbol')
    _name = row.get('name')
    _industry = row.get("industry")
    _edt = datetime.datetime.strptime(edt, "%Y%m%d")
    _db = "BIUS"

    output = {}
    try:
        bars = ydc.history(_symbol, start_date=sdt, end_date=edt, freq=freq, raw_bar=True)
        c = CZSC(bars)
        _signals = trend_reverse_bi(c, edt=_edt, fx_dt_limit=fx_dt_limit, freq=freq, db=_db, **row)
        logger.debug(_signals)

        for s_value in _signals.values():
            if "买" in s_value:
                s_value_detail = s_value.split("_")
                symbol_link = f'<a href="https://xueqiu.com/S/{_symbol}">{_symbol}</a>'
                output = {
                    'name': _name,
                    'symbol': symbol_link,
                    'signals': s_value_detail[0],
                    'fx_pwr': s_value_detail[1],
                    'expect_profit(%)': round(float(s_value_detail[2]) * 100, 2),
                    'industry': _industry
                }
    except Exception as e_msg:
        tb = traceback.format_exc()  # 获取 traceback 信息
        logger.critical(f"{_symbol} {_name}出现报错，{e_msg}\nTraceback: {tb}")

    finally:
        gc.collect()
        return output


def bot_fx_detect(row, sdt, edt, freq: str = 'W'):
    dc = TsDataCache(home_path)  # 在每个进程中创建独立的实例
    _ts_code = row.get('ts_code')
    _symbol = row.get('symbol')
    _name = row.get('name')
    _industry = row.get("industry")
    _hs = _ts_code.split(".")[-1]
    _edt = datetime.datetime.strptime(edt, "%Y%m%d")
    output = {}
    try:
        bars = dc.pro_bar(_ts_code, start_date=sdt, end_date=edt, freq=freq, asset="E", adj='qfq', raw_bar=True)
        c = CZSC(bars)
        latest_fx = c.ubi_fxs[-1]
        if is_strong_bot_fx(c, latest_fx, _edt):
            symbol_link = f'<a href="https://xueqiu.com/S/{_hs}{_symbol}">{_symbol}</a>'
            output['name'] = _name
            output['symbol_link'] = symbol_link
            output['latest_fx_dt'] = latest_fx.dt
            output['industry'] = _industry
            logger.info(f"输出：{symbol_link} {_name} {latest_fx.dt} {latest_fx.power_str}底分型")

    except Exception as e_msg:
        tb = traceback.format_exc()  # 获取 traceback 信息
        logger.critical(f"{_ts_code} {_name}出现报错，{e_msg}\nTraceback: {tb}")

    finally:
        return output


def bottom_pzbc(row, sdt, edt, freq: str = 'W', fx_dt_limit: int = 30):
    dc = TsDataCache(home_path)  # 在每个进程中创建独立的实例
    _ts_code = row.get('ts_code')
    _symbol = row.get('symbol')
    _name = row.get('name')
    _industry = row.get("industry")
    _hs = _ts_code.split(".")[-1]
    _edt = datetime.datetime.strptime(edt, "%Y%m%d")
    output = {}
    try:
        bars = dc.pro_bar(_ts_code, start_date=sdt, end_date=edt, freq=freq, asset="E", adj='qfq', raw_bar=True)
        c = CZSC(bars)
        _signals = macd_pzbc_bi(c, edt=_edt, fx_dt_limit=fx_dt_limit, freq=freq, **row)
        logger.debug(_signals)
        for s_value in _signals.values():
            if "买" in s_value:
                s_value_detail = s_value.split("_")
                symbol_link = f'<a href="https://xueqiu.com/S/{_hs}{_symbol}">{_symbol}</a>'
                output = {
                    'name': _name,
                    'symbol': symbol_link,
                    'ts_code': _ts_code,
                    'signals': s_value_detail[0],
                    'fx_pwr': s_value_detail[1],
                    'expect_profit(%)': round(float(s_value_detail[2]) * 100, 2),
                    'industry': _industry
                }
    except Exception as e_msg:
        tb = traceback.format_exc()  # 获取 traceback 信息
        logger.critical(f"{_ts_code} {_name}出现报错，{e_msg}\nTraceback: {tb}")

    finally:
        return output


def ma_up_pzbc(row, sdt, edt, freq: str = 'D', fx_dt_limit: int = 5, timeperiod: int = 250, last_n: int = 5):
    dc = TsDataCache(home_path)  # 在每个进程中创建独立的实例
    _ts_code = row.get('ts_code')
    _symbol = row.get('symbol')
    _name = row.get('name')
    _industry = row.get("industry")
    _hs = _ts_code.split(".")[-1]
    _edt = datetime.datetime.strptime(edt, "%Y%m%d")
    output = {}
    try:
        bars = dc.pro_bar(_ts_code, start_date=sdt, end_date=edt, freq=freq, asset="E", adj='qfq', raw_bar=True)
        c = CZSC(bars)
        _signals = long_term_ma_up(c, edt=_edt, fx_dt_limit=fx_dt_limit, freq=freq,
                                   timeperiod=timeperiod, last_n=last_n, **row)
        logger.debug(_signals)
        for s_value in _signals.values():
            if "强势盘整背驰" in s_value:
                s_value_detail = s_value.split("_")
                symbol_link = f'<a href="https://xueqiu.com/S/{_hs}{_symbol}">{_symbol}</a>'
                output = {
                    'name': _name,
                    'symbol': symbol_link,
                    'ts_code': _ts_code,
                    'signals': s_value_detail[0],
                    'fx_pwr': s_value_detail[1],
                    'expect_profit(%)': round(float(s_value_detail[2]) * 100, 2),
                    'industry': _industry
                }
    except Exception as e_msg:
        tb = traceback.format_exc()  # 获取 traceback 信息
        logger.critical(f"{_ts_code} {_name}出现报错，{e_msg}\nTraceback: {tb}")

    finally:
        return output


def ma_up_pzbc_us(row, sdt, edt, freq: str = 'D', fx_dt_limit: int = 5, timeperiod: int = 250, last_n: int = 5):
    ydc = YfDataCache(home_path)  # 在每个进程中创建独立的实例
    _symbol = row.get('symbol')
    _name = row.get('name')
    _industry = row.get("industry")
    _edt = datetime.datetime.strptime(edt, "%Y%m%d")

    _db = "MAUS"

    output = {}
    try:
        bars = ydc.history(_symbol, start_date=sdt, end_date=edt, freq=freq, raw_bar=True)
        c = CZSC(bars)
        _signals = long_term_ma_up(c, edt=_edt, fx_dt_limit=fx_dt_limit, freq=freq, db=_db,
                                   timeperiod=timeperiod, last_n=last_n, **row)
        logger.debug(_signals)

        for s_value in _signals.values():
            if "强势盘整背驰" in s_value:
                s_value_detail = s_value.split("_")
                symbol_link = f'<a href="https://xueqiu.com/S/{_symbol}">{_symbol}</a>'
                output = {
                    'name': _name,
                    'symbol': symbol_link,
                    'signals': s_value_detail[0],
                    'fx_pwr': s_value_detail[1],
                    'expect_profit(%)': round(float(s_value_detail[2]) * 100, 2),
                    'industry': _industry
                }
    except Exception as e_msg:
        tb = traceback.format_exc()  # 获取 traceback 信息
        logger.critical(f"{_symbol} {_name}出现报错，{e_msg}\nTraceback: {tb}")

    finally:
        gc.collect()
        return output


def zs_elevate_3rd_buy_bi(row, sdt, edt, freq: str, ana_type: str = "bi", fx_dt_limit: int = 5):
    dc = TsDataCache(home_path)  # 在每个进程中创建独立的实例
    _ts_code = row.get('ts_code')
    _symbol = row.get('symbol')
    _name = row.get('name')
    _industry = row.get("industry")
    _hs = _ts_code.split(".")[-1]
    _edt = datetime.datetime.strptime(edt, "%Y%m%d")
    ana_strategy = dict(bi=third_buy_bi, xd=third_buy_xd)
    third_buy_strategy = ana_strategy.get(ana_type)

    output = {}
    try:
        bars = dc.pro_bar(_ts_code, start_date=sdt, end_date=edt, freq=freq, asset="E", adj='qfq', raw_bar=True)
        # if "ST" in _name:
        #     return output
        c = CZSC(bars)
        _signals = third_buy_strategy(c, edt=_edt, fx_dt_limit=fx_dt_limit, freq=freq, **row)
        logger.debug(_signals)

        for s_value in _signals.values():
            if "买" in s_value:
                s_value_detail = s_value.split("_")
                symbol_link = f'<a href="https://xueqiu.com/S/{_hs}{_symbol}">{_symbol}</a>'
                output = {
                    'name': _name,
                    'symbol': symbol_link,
                    'ts_code': _ts_code,
                    'signals': s_value_detail[0],
                    'fx_pwr': s_value_detail[1],
                    'expect_profit(%)': round(float(s_value_detail[2]) * 100, 2),
                    'industry': _industry
                }
    except Exception as e_msg:
        tb = traceback.format_exc()  # 获取 traceback 信息
        logger.critical(f"{_ts_code} {_name}出现报错，{e_msg}\nTraceback: {tb}")

    finally:
        gc.collect()
        return output


def zs_elevate_3rd_buy_bi_us(row, sdt, edt, freq: str, ana_type: str = "bi", fx_dt_limit: int = 5):
    ydc = YfDataCache(home_path)  # 在每个进程中创建独立的实例
    _symbol = row.get('symbol')
    _name = row.get('name')
    _industry = row.get("industry")
    _edt = datetime.datetime.strptime(edt, "%Y%m%d")
    ana_strategy = dict(bi=third_buy_bi, xd=third_buy_xd)
    db_choice = dict(bi="BIUS", xd="XDUS")
    third_buy_strategy = ana_strategy.get(ana_type)
    _db = db_choice.get(ana_type)

    output = {}
    try:
        bars = ydc.history(_symbol, start_date=sdt, end_date=edt, freq=freq, raw_bar=True)
        c = CZSC(bars)
        _signals = third_buy_strategy(c, edt=_edt, fx_dt_limit=fx_dt_limit, freq=freq, **row)
        logger.debug(_signals)

        for s_value in _signals.values():
            if "买" in s_value:
                s_value_detail = s_value.split("_")
                symbol_link = f'<a href="https://xueqiu.com/S/{_symbol}">{_symbol}</a>'
                output = {
                    'name': _name,
                    'symbol': symbol_link,
                    'signals': s_value_detail[0],
                    'fx_pwr': s_value_detail[1],
                    'expect_profit(%)': round(float(s_value_detail[2]) * 100, 2),
                    'industry': _industry
                }
    except Exception as e_msg:
        tb = traceback.format_exc()  # 获取 traceback 信息
        logger.critical(f"{_symbol} {_name}出现报错，{e_msg}\nTraceback: {tb}")

    finally:
        gc.collect()
        return output


def row_2_czsc(row, sdt, edt, freq: str = 'W'):
    _ts_code = row.get('ts_code')
    _symbol = row.get('symbol')
    _name = row.get('name')
    _industry = row.get("industry")
    _hs = _ts_code.split(".")[-1]
    _edt = datetime.datetime.strptime(edt, "%Y%m%d")
    dc = TsDataCache(home_path)  # 在每个进程中创建独立的实例
    bars = dc.pro_bar(_ts_code, start_date=sdt, end_date=edt, freq=freq, asset="E", adj='qfq', raw_bar=True)
    return CZSC(bars)


def fake_xd_2nd_buy(row, sdt, edt, freq: str, fx_dt_limit: int = 5):
    dc = TsDataCache(home_path)  # 在每个进程中创建独立的实例
    _ts_code = row.get('ts_code')
    _symbol = row.get('symbol')
    _name = row.get('name')
    _industry = row.get("industry")
    _hs = _ts_code.split(".")[-1]
    _edt = datetime.datetime.strptime(edt, "%Y%m%d")

    output = {}
    try:
        bars = dc.pro_bar(_ts_code, start_date=sdt, end_date=edt, freq=freq, asset="E", adj='qfq', raw_bar=True)
        # if "ST" in _name:
        #     return output
        c = CZSC(bars)
        _signals = fake_xd_2nd_buy_point(c, edt=_edt, fx_dt_limit=fx_dt_limit, freq=freq, **row)
        logger.debug(_signals)

        for s_value in _signals.values():
            if "买" in s_value:
                s_value_detail = s_value.split("_")
                symbol_link = f'<a href="https://xueqiu.com/S/{_hs}{_symbol}">{_symbol}</a>'
                output = {
                    'name': _name,
                    'symbol': symbol_link,
                    'ts_code': _ts_code,
                    'signals': s_value_detail[0],
                    'fx_pwr': s_value_detail[1],
                    'expect_profit(%)': round(float(s_value_detail[2]) * 100, 2),
                    'industry': _industry
                }
    except Exception as e_msg:
        tb = traceback.format_exc()  # 获取 traceback 信息
        logger.critical(f"{_ts_code} {_name}出现报错，{e_msg}\nTraceback: {tb}")

    finally:
        gc.collect()
        return output