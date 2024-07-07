#!/bin/bash
cd ~/workspace/czsc/
source venv/bin/activate


# 以最新数据分析当天走势
python -m hjw_examples.day_trend_bc_reverse_dev

# 执行回测
python -m hjw_examples.all_stocks_backtest
