# Codex 工作规则

## 重点目录
- hjw_examples/market_mood.py

## 默认不要读取
- node_modules/
- dist/
- build/
- logs/
- data/raw/
- .env 
- .venv 
- env/ 
- venv/ 
- ENV/ 
- env.bak/ 
- venv.bak/
- __pycache__/
- *.csv
- *.parquet
- *.db
- hjw_examples/statics/ 
- statics/ 
- hjw_examples/exam.py 
- database/configs.py

## 工作方式
- 修改前先说明会动哪些文件。
- 没有明确要求，不要搜索整个仓库。
- 优先使用 rg 精确搜索，不要全量打开大文件。