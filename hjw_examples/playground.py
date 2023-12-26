import datetime

from czsc.data import TsDataCache
from czsc import home_path
from czsc.signals.tas import update_macd_cache
from czsc.analyze import CZSC


def play():
    dc = TsDataCache(home_path)
    bars = dc.pro_bar('603808.SH', start_date="20150101", freq='M', asset="E", adj='qfq', raw_bar=True)
    c = CZSC(bars)
    cache_key = update_macd_cache(c)
    print(c.ubi)

    for bi in c.ubi['raw_bars']:
        try:
            print(f"{bi.dt} {bi.close} {bi.cache[cache_key]['macd']}")
        except Exception as e_msg:
            print(e_msg)


if __name__ == '__main__':
    output_name = f"statics/3buy_day_result_{datetime.datetime.today().strftime('%Y-%m-%d')}.txt"
    # if not os.path.exists(output_name):
    # empty_cache_path()
    play()
