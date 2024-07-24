#!/bin/bash
cd ~/workspace/czsc/
source venv/bin/activate


# 以最新数据分析当天走势
#python -m hjw_examples.day_trend_bc_reverse_dev
#python -m hjw_examples.ma_support --sd 20240521 --ed 20240716 -d
python -m hjw_examples.day_trend_bc_reverse_us --sd 20240201 --ed 20240723 -d

# 执行回测
#python -m hjw_examples.all_stocks_backtest


