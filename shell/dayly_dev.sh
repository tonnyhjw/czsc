#!/bin/bash
cd ~/workspace/czsc/
source venv/bin/activate


# 以最新数据分析当天走势
# python -m hjw_examples.day_trend_bc_reverse --sd 20220101 --ed 20221231 -d
python -m hjw_examples.elevate --t bi --sd 20240101 --ed 20240329 -d
python -m hjw_examples.flow_global --n 7 --h 100 --sd 20240101 --ed 20240329 -d
#python -m hjw_examples.ma_support --sd 20240521 --ed 20240716 -d
#python -m hjw_examples.day_trend_bc_reverse_us --sd 20240111 --ed 20240724 -d
#python -m hjw_examples.ma_support_us --sd 20240111 --ed 20240724 -d

# 执行回测
#python -m hjw_examples.all_stocks_backtest


