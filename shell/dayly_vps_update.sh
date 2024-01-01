#!/bin/bash

cd ~/workspace/czsc/
source venv/bin/activate

# 移除缓存
python -m hjw_examples.remove_cache
# 以最新数据分析当天走势
python -m hjw_examples.day_trend_bc_reverse
