import datetime

from czsc.data import TsDataCache
from czsc import home_path, empty_cache_path, RawBar
from czsc.signals.tas import update_macd_cache
from czsc.analyze import CZSC


def play():
    dc = TsDataCache(home_path)
    bars = dc.pro_bar('000001.SH', start_date="20220401", freq='W', asset="I", adj='qfq', raw_bar=True)
    c = CZSC(bars)
    cache_key = update_macd_cache(c)
    print(c.ubi)
    last_bi = c.bi_list[-1]
    print(last_bi)
    fx_raw_bars = []
    for fx in c.fx_list:
        fx_raw_bars += fx.raw_bars

    for x in last_bi.raw_bars:
        try:
            print(x.cache[cache_key])
        except Exception as e_msg:
            print(e_msg)


if __name__ == '__main__':
    output_name = f"statics/3buy_day_result_{datetime.datetime.today().strftime('%Y-%m-%d')}.txt"
    # if not os.path.exists(output_name):
    # empty_cache_path()
    play()
