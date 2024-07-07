from src.objects import *

def create_XDs(bi_list: List[BI]):
    fs_bars = []
    for _bi in enumerate(bi_list):
        bar = RawBar(
            symbol=_bi.symbol,
            open=_bi.high,
            close=bi.

        )
        update(_bi, fs_bars)
    return


def update(bi: BI, fs_bars: List[FeatureSequencesNewBar]):
    fs_bars = []

    return
