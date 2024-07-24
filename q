[1mdiff --git a/hjw_examples/all_stocks_backtest.py b/hjw_examples/all_stocks_backtest.py[m
[1mindex a8734a8..18c9415 100644[m
[1m--- a/hjw_examples/all_stocks_backtest.py[m
[1m+++ b/hjw_examples/all_stocks_backtest.py[m
[36m@@ -126,7 +126,7 @@[m [mif __name__ == '__main__':[m
     stock_basic = TsDataCache(home_path).stock_basic()  # 只用于读取股票基础信息[m
     # stock_basic = stock_basic.head(100)[m
     FX_PWR = ["强", "中", "弱"][m
[31m-    SIGNALS = ["二买", "三买"][m
[32m+[m[32m    SIGNALS = ["一买", "二买", "三买"][m
     for _signals in SIGNALS:[m
         for _fx_pwr in FX_PWR:[m
             run_all_stocks_backtest(stock_basic, fx_pwr=_fx_pwr, signals=_signals, db="MA250")[m
[1mdiff --git a/hjw_examples/day_trend_bc_reverse_dev.py b/hjw_examples/day_trend_bc_reverse_dev.py[m
[1mindex 5eab8df..e1faeda 100644[m
[1m--- a/hjw_examples/day_trend_bc_reverse_dev.py[m
[1m+++ b/hjw_examples/day_trend_bc_reverse_dev.py[m
[36m@@ -62,7 +62,7 @@[m [mif __name__ == '__main__':[m
     # # 生成日期范围，从2024年1月1日到今天[m
     # date_range = pd.date_range(start='2024-01-17', end=today, freq='B')[m
     # formatted_dates = date_range.strftime('%Y%m%d').tolist()[m
[31m-    sdt, edt = '2024-02-01', '2024-07-22'[m
[32m+[m[32m    sdt, edt = '2023-11-03', '2024-01-31'[m
     today = datetime.datetime.now().strftime("%Y%m%d")[m
     trade_dates = TsDataCache(home_path).get_dates_span(sdt, edt, is_open=True)[m
 [m
[1mdiff --git a/hjw_examples/exam.py b/hjw_examples/exam.py[m
[1mindex e8684eb..6c27e24 100644[m
[1m--- a/hjw_examples/exam.py[m
[1m+++ b/hjw_examples/exam.py[m
[36m@@ -54,11 +54,18 @@[m [mdef ma_pzbc_dev():[m
     return[m
 [m
 [m
[32m+[m[32mdef us_data():[m
[32m+[m[32m    import pandas_datareader[m
[32m+[m[32m    pprint.pprint(dir(pandas_datareader.data.get_data_yahoo()))[m
[32m+[m[32m    return[m
[32m+[m
[32m+[m
 if __name__ == '__main__':[m
     # play_day_trend_reverse()[m
     # play_pzbc()[m
     # result = run_single_stock_backtest(ts_code='000415.SZ', edt='20240614', freq="D")[m
     # pprint.pprint(result.get("sharpe_ratio"))[m
     # xd_dev()[m
[31m-    bi_dev()[m
[32m+[m[32m    # bi_dev()[m
     # ma_pzbc_dev()[m
[32m+[m[32m    us_data()[m
