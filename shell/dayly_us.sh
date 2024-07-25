#!/bin/bash
cd ~/workspace/czsc/
source venv/bin/activate


# 以最新数据分析当天走势
python -m hjw_examples.day_trend_bc_reverse_us -r

python -m hjw_examples.ma_support_us
