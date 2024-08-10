#!/bin/bash
cd ~/workspace/czsc/
source venv/bin/activate

# 检测本月强势底分型个股
python -m hjw_examples.pzbc --sdt 20120101 --f M
#python -m hjw_examples.ma_support --f W --tp 120 --n 3
