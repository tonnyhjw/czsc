#!/bin/bash

cd ~/workspace/czsc/
source venv/bin/activate

# 移除缓存
python -m hjw_examples.remove_cache
