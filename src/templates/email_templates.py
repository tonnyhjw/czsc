# 添加自定义样式
def daily_email_style(html_table):
    return """
    <html>
    <head>
    <style>
        .table {
            width: 100%;
            margin-bottom: 1rem;
            color: #212529;
        }
        .table-striped tbody tr:nth-of-type(odd) {
            background-color: rgba(0, 0, 0, 0.05);
        }
        .table-hover tbody tr:hover {
            color: #212529;
            background-color: rgba(0, 0, 0, 0.075);
        }
        th {
            vertical-align: bottom;
            border-bottom: 2px solid #dee2e6;
        }
        td, th {
            padding: .75rem;
            vertical-align: top;
            border-top: 1px solid #dee2e6;
            text-align:center;
        }
    </style>
    </head>
    <body>
    """ + html_table + """
    </body>
    </html>
    """


def backtrader_email_body(trade_analysis, sharpe_ratio):
    body = f"""
    <h2>综合交易分析结果</h2>
    <ul>
        <li><strong>总交易数:</strong> {trade_analysis['total']['total']} (所有交易的总数)
            <ul>
                <li><strong>未结交易数:</strong> {trade_analysis['total']['open']} (当前未结算的交易数)</li>
                <li><strong>已结交易数:</strong> {trade_analysis['total']['closed']} (已结算的交易数)</li>
            </ul>
        </li>
        <li><strong>连续交易:</strong>
            <ul>
                <li><strong>连续盈利:</strong>
                    <ul>
                        <li><strong>当前:</strong> {trade_analysis['streak']['won']['current']} (当前连续盈利的次数)</li>
                        <li><strong>最长:</strong> {trade_analysis['streak']['won']['longest']} (最长的连续盈利次数)</li>
                    </ul>
                </li>
                <li><strong>连续亏损:</strong>
                    <ul>
                        <li><strong>当前:</strong> {trade_analysis['streak']['lost']['current']} (当前连续亏损的次数)</li>
                        <li><strong>最长:</strong> {trade_analysis['streak']['lost']['longest']} (最长的连续亏损次数)</li>
                    </ul>
                </li>
            </ul>
        </li>
        <li><strong>利润和损失 (PnL):</strong>
            <ul>
                <li><strong>毛利润:</strong>
                    <ul>
                        <li><strong>总计:</strong> {trade_analysis['pnl']['gross']['total']} (毛利润的总金额)</li>
                        <li><strong>平均:</strong> {trade_analysis['pnl']['gross']['average']} (每笔交易的平均毛利润)</li>
                    </ul>
                </li>
                <li><strong>净利润:</strong>
                    <ul>
                        <li><strong>总计:</strong> {trade_analysis['pnl']['net']['total']} (扣除佣金和费用后的净利润总金额)</li>
                        <li><strong>平均:</strong> {trade_analysis['pnl']['net']['average']} (每笔交易的平均净利润)</li>
                    </ul>
                </li>
            </ul>
        </li>
        <li><strong>盈亏比</strong>
            <ul>
                <li><strong>盈亏次数比:</strong> {trade_analysis['won']['total']} : {trade_analysis['lost']['total']} (盈利的交易总数:亏损的交易总数)</li>
                <li><strong>盈利交易占比:</strong> {trade_analysis['won']['total']/trade_analysis['total']['closed']:.2f} (盈利的交易总数:已结算的交易数)</li>
            </ul>
        </li>
        <li><strong>盈利交易:</strong>
            <ul>
                <li><strong>总计:</strong> {trade_analysis['won']['total']} (盈利的交易总数)</li>
                <li><strong>利润:</strong>
                    <ul>
                        <li><strong>总计:</strong> {trade_analysis['won']['pnl']['total']} (盈利交易的总利润)</li>
                        <li><strong>平均:</strong> {trade_analysis['won']['pnl']['average']} (每笔盈利交易的平均利润)</li>
                        <li><strong>最大:</strong> {trade_analysis['won']['pnl']['max']} (单笔盈利交易的最大利润)</li>
                    </ul>
                </li>
            </ul>
        </li>
        <li><strong>亏损交易:</strong>
            <ul>
                <li><strong>总计:</strong> {trade_analysis['lost']['total']} (亏损的交易总数)</li>
                <li><strong>损失:</strong>
                    <ul>
                        <li><strong>总计:</strong> {trade_analysis['lost']['pnl']['total']} (亏损交易的总损失)</li>
                        <li><strong>平均:</strong> {trade_analysis['lost']['pnl']['average']} (每笔亏损交易的平均损失)</li>
                        <li><strong>最大:</strong> {trade_analysis['lost']['pnl']['max']} (单笔亏损交易的最大损失)</li>
                    </ul>
                </li>
            </ul>
        </li>
        <li><strong>多头交易:</strong>
            <ul>
                <li><strong>总计:</strong> {trade_analysis['long']['total']} (多头交易的总数)
                    <ul>
                        <li><strong>盈利交易数:</strong> {trade_analysis['long']['won']} (多头交易中盈利的交易数)</li>
                        <li><strong>亏损交易数:</strong> {trade_analysis['long']['lost']} (多头交易中亏损的交易数)</li>
                        <li><strong>利润:</strong>
                            <ul>
                                <li><strong>总计:</strong> {trade_analysis['long']['pnl']['total']} (多头交易的总利润)</li>
                                <li><strong>平均:</strong> {trade_analysis['long']['pnl']['average']} (每笔多头交易的平均利润)</li>
                                <li><strong>最大盈利:</strong> {trade_analysis['long']['pnl']['won']['max']} (单笔多头交易的最大盈利)</li>
                                <li><strong>最大亏损:</strong> {trade_analysis['long']['pnl']['lost']['max']} (单笔多头交易的最大亏损)</li>
                            </ul>
                        </li>
                    </ul>
                </li>
            </ul>
        </li>
        <li><strong>空头交易:</strong>
            <ul>
                <li><strong>总计:</strong> {trade_analysis['short']['total']} (空头交易的总数)
                    <ul>
                        <li><strong>盈利交易数:</strong> {trade_analysis['short']['won']} (空头交易中盈利的交易数)</li>
                        <li><strong>亏损交易数:</strong> {trade_analysis['short']['lost']} (空头交易中亏损的交易数)</li>
                        <li><strong>利润:</strong>
                            <ul>
                                <li><strong>总计:</strong> {trade_analysis['short']['pnl']['total']} (空头交易的总利润)</li>
                                <li><strong>平均:</strong> {trade_analysis['short']['pnl']['average']} (每笔空头交易的平均利润)</li>
                                <li><strong>最大盈利:</strong> {trade_analysis['short']['pnl']['won']['max']} (单笔空头交易的最大盈利)</li>
                                <li><strong>最大亏损:</strong> {trade_analysis['short']['pnl']['lost']['max']} (单笔空头交易的最大亏损)</li>
                            </ul>
                        </li>
                    </ul>
                </li>
            </ul>
        </li>
        <li><strong>交易时长:</strong>
            <ul>
                <li><strong>总计:</strong> {trade_analysis['len']['total']} (所有交易的总时长)
                    <ul>
                        <li><strong>平均:</strong> {trade_analysis['len']['average']} (每笔交易的平均时长)</li>
                        <li><strong>最长:</strong> {trade_analysis['len']['max']} (单笔交易的最长时长)</li>
                        <li><strong>最短:</strong> {trade_analysis['len']['min']} (单笔交易的最短时长)</li>
                    </ul>
                </li>
                <li><strong>盈利交易时长:</strong>
                    <ul>
                        <li><strong>总计:</strong> {trade_analysis['len']['won']['total']} (盈利交易的总时长)</li>
                        <li><strong>平均:</strong> {trade_analysis['len']['won']['average']} (每笔盈利交易的平均时长)</li>
                        <li><strong>最长:</strong> {trade_analysis['len']['won']['max']} (单笔盈利交易的最长时长)</li>
                        <li><strong>最短:</strong> {trade_analysis['len']['won']['min']} (单笔盈利交易的最短时长)</li>
                    </ul>
                </li>
                <li><strong>亏损交易时长:</strong>
                    <ul>
                        <li><strong>总计:</strong> {trade_analysis['len']['lost']['total']} (亏损交易的总时长)</li>
                        <li><strong>平均:</strong> {trade_analysis['len']['lost']['average']} (每笔亏损交易的平均时长)</li>
                        <li><strong>最长:</strong> {trade_analysis['len']['lost']['max']} (单笔亏损交易的最长时长)</li>
                        <li><strong>最短:</strong> {trade_analysis['len']['lost']['min']} (单笔亏损交易的最短时长)</li>
                    </ul>
                </li>
            </ul>
        </li>
    </ul>

    <h2>综合夏普比率分析</h2>
    <ul>
        <li><strong>夏普比率:</strong> {sharpe_ratio.get('sharperatio', '暂无数据')} (夏普比率衡量的是投资相对于其风险的回报。一般来说，夏普比率越高越好，0以上是正收益，1以上是优秀，2以上是非常优秀。负数表示投资回报低于无风险利率)</li>
    </ul>
    """

    return body
