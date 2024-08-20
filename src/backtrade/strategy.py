import backtrader as bt


class NaiveStrategy(bt.Strategy):
    params = (
        ('buy_points', []),
        ('fxs', []),
        ('stop_loss', 0.05),  # 添加止损参数
    )

    def __init__(self):
        self.order = None
        self.buy_signal = False
        self.sell_signal = False
        self.all_buy_points_consumed = False

        # 用于记录买卖点
        self.buy_dates = []
        self.sell_dates = []

        # 记录买入价格
        self.buy_price = None

    def next(self):
        if self.all_buy_points_consumed and self.position.size == 0:
            print("All buy points consumed and position cleared. Ending early.")
            self.env.runstop()
            return

        current_date = bt.num2date(self.datas[0].datetime[0])  # 将当前时间转换为datetime对象

        # 检查是否有买入信号
        if not self.all_buy_points_consumed:
            for buy_point in self.params.buy_points:
                if (current_date - buy_point.date).days == 2 and self.position.size == 0:
                    self.buy_signal = True
                    self.params.buy_points.remove(buy_point)
                    break

            if not self.params.buy_points:
                self.all_buy_points_consumed = True

        # 执行买入操作
        if self.buy_signal and self.order is None:
            available_cash = self.broker.get_cash()
            price = self.data.close[0]
            commission = self.broker.getcommissioninfo(self.data).getcommission(price, 1)
            buy_size = int(available_cash / (price * (1 + commission)))
            if buy_size > 0:
                self.buy_price = price  # 记录买入价格
                stop_loss_price = price * (1 - self.params.stop_loss)
                self.order = self.buy_bracket(size=buy_size,
                                              limitprice=None,
                                              stopprice=stop_loss_price)
                self.buy_dates.append(current_date)
                print(f'BUY ORDER CREATED: {buy_size} shares at {price} with stop loss at {stop_loss_price} on {current_date}')
            else:
                print('Insufficient cash to create buy order')
            self.buy_signal = False

        # 检查是否有卖出信号
        if self.position.size > 0:
            for idx, fx in enumerate(self.params.fxs):
                if fx.dt == current_date:
                    self.sell_signal = True
                    # 截断fxs列表，只保留未处理部分
                    self.params.fxs = self.params.fxs[idx + 1:]
                    break

        # 执行卖出操作
        if self.sell_signal and self.order is None:
            if self.position.size > 0:
                self.order = self.sell(size=self.position.size)
                self.sell_dates.append(current_date)
                print(f'SELL ORDER CREATED: {self.position.size} shares at {self.data.close[0]} on {current_date}')
            else:
                print('No position to sell')
            self.sell_signal = False

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return  # 等待订单被处理

        if order.status in [order.Completed]:
            if order.isbuy():
                print(f'BUY EXECUTED, {order.executed.price}')
            elif order.issell():
                print(f'SELL EXECUTED, {order.executed.price}')

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            print(f'Order Canceled/Margin/Rejected: {order.info}')

        # 订单完成后，将self.order重置为None
        self.order = None

