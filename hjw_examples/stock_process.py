from czsc import CZSC, home_path
from czsc.data import TsDataCache
from hjw_examples.sig import trend_reverse_ubi


def trend_reverse_ubi_entry(row, sdt, edt):
    dc = TsDataCache(home_path)  # 在每个进程中创建独立的实例
    _ts_code = row.get('ts_code')
    _symbol = row.get('symbol')
    _name = row.get('name')
    _industry = row.get("industry")
    _hs = _ts_code.split(".")[-1]

    output = {}
    try:
        bars = dc.pro_bar(_ts_code, start_date=sdt, end_date=edt, freq='D', asset="E", adj='qfq', raw_bar=True)
        # if "ST" in _name:
        #     return output
        c = CZSC(bars)
        _signals = trend_reverse_ubi(c)
        print(_signals)

        for s_value in _signals.values():
            if "多头" in s_value:
                s_value_detail = s_value.split("_")
                print(s_value_detail)
                symbol_link = f'<a href="https://xueqiu.com/S/{_hs}{_symbol}">{_symbol}</a>'
                output = {
                    'name': _name,
                    'symbol': symbol_link,
                    'ts_code': _ts_code,
                    'signals': s_value_detail[1],
                    'expect_profit(%)': round(float(s_value_detail[2]) * 100, 2),
                    'industry': _industry
                }
    except Exception as e_msg:
        tb = traceback.format_exc()  # 获取 traceback 信息
        logger.error(f"{_ts_code} {_name}出现报错，{e_msg}\nTraceback: {tb}")

    finally:
        return output