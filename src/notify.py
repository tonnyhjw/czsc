import os
import traceback
from loguru import logger
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from src.formatters import *
from src.templates.email_templates import daily_email_style, backtrader_email_body


logger.add("statics/logs/notify.log", rotation="10MB", encoding="utf-8", enqueue=True, retention="10 days")


def send_email(html_content, subject):
    sender_email = os.environ.get('EMAIL_USER')
    sender_password = os.environ.get('EMAIL_PASS')

    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = os.environ.get('RECIPIENT_EMAIL')
    message["Subject"] = subject

    message.attach(MIMEText(html_content, "html"))

    with smtplib.SMTP_SSL('smtp.126.com', 465) as smtp:
        smtp.login(sender_email, sender_password)
        smtp.send_message(message)


def notify_buy_points(results: list, email_subject: str, notify_empty: bool = True):
    html_table = "<h1>没有发现买点</h1>"

    def send_buy_point_style_email(raw_html):
        styled_table = daily_email_style(raw_html)
        # 发送电子邮件
        send_email(styled_table, email_subject)

    try:
        if results:
            # 将结果转换为 DataFrame
            sorted_results = sorted(results, key=sort_by_profit, reverse=True)
            sorted_results = sorted(sorted_results, key=sort_by_industry, reverse=True)
            sorted_results = sorted(sorted_results, key=sort_by_fx_pwr, reverse=True)
            sorted_results = sorted(sorted_results, key=sort_by_signals)
            df_results = pd.DataFrame(sorted_results)
            # 生成 HTML 表格
            html_table = df_results.to_html(classes='table table-striped table-hover', border=0, index=False,
                                            escape=False)
            # 发送电子邮件
            send_buy_point_style_email(html_table)
        elif notify_empty:
            send_buy_point_style_email(html_table)
        else:
            logger.info(html_table)

    except Exception as e_msg:
        tb = traceback.format_exc()  # 获取 traceback 信息
        logger.error(f"发送结果出现报错，{e_msg}\nTraceback: {tb}")


def notify_buy_backtrader(trade_analysis, sharpe_ratio, email_subject: str):
    try:
        # if trade_analysis and sharpe_ratio:
        if trade_analysis:
            email_content = backtrader_email_body(trade_analysis, sharpe_ratio)
            # 发送电子邮件
            send_email(email_content, email_subject)

    except Exception as e_msg:
        tb = traceback.format_exc()  # 获取 traceback 信息
        logger.error(f"发送结果出现报错，{e_msg}\nTraceback: {tb}")


def notify_money_flow(results: list, email_subject: str, notify_empty: bool = True):
    html_table = "<h1>没有发现买点</h1>"

    def send_buy_point_style_email(raw_html):
        styled_table = daily_email_style(raw_html)
        # 发送电子邮件
        send_email(styled_table, email_subject)

    try:
        if results:
            # 将结果转换为 DataFrame
            sorted_results = sorted(results, key=sort_by_fx_pwr, reverse=True)
            sorted_results = sorted(sorted_results, key=sort_by_signals)
            sorted_results = sorted(sorted_results, key=sort_by_industry, reverse=True)

            df_results = pd.DataFrame(sorted_results)
            # 生成 HTML 表格
            html_table = df_results.to_html(classes='table table-striped table-hover', border=0, index=False,
                                            escape=False)
            # 发送电子邮件
            send_buy_point_style_email(html_table)
        elif notify_empty:
            send_buy_point_style_email(html_table)
        else:
            logger.info(html_table)

    except Exception as e_msg:
        tb = traceback.format_exc()  # 获取 traceback 信息
        logger.error(f"发送结果出现报错，{e_msg}\nTraceback: {tb}")


if __name__ == '__main__':
    send_email('hello', 'hello')