#!/bin/bash
cd ~/workspace/czsc/
source venv/bin/activate

# 检测本周强势底分型个股
# python -m hjw_examples.week_bot_fx
python -m hjw_examples.week_trend_bc_reverse
