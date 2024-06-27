#!/bin/bash
cd ~/workspace/czsc/
source venv/bin/activate

# 测试指定时间运算结果
python -m hjw_examples.all_stocks_backtest
