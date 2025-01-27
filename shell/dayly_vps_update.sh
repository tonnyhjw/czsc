#!/bin/bash
cd ~/workspace/czsc/
source venv/bin/activate

# 移除缓存
#python -m hjw_examples.remove_cache
# 以最新数据分析当天走势
python -m hjw_examples.ma_support -r

python -m hjw_examples.day_trend_bc_reverse --f D

python -m hjw_examples.elevate --t bi

python -m hjw_examples.flow_global --n 7 --h 100

python -m hjw_examples.concept_liked
