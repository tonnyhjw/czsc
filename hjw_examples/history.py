import datetime
import pandas as pd


def read_history(history_file):
    try:
        one_month_ago = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
        history = pd.read_csv(history_file, parse_dates=['date'])
        # 仅保留最近一个月的记录
        history = history[history['date'] >= one_month_ago]
    except FileNotFoundError:
        history = pd.DataFrame(columns=['ts_code', 'date'])
    return history


def update_history(history, ts_code, history_file):
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    new_record = pd.DataFrame({'ts_code': [ts_code], 'date': [today]})

    # 使用 concat 替代 append
    history = pd.concat([history, new_record], ignore_index=True)

    history.to_csv(history_file, index=False)
    return history