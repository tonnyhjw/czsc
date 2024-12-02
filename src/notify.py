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


def notify_buy_backtrader(trade_analysis, sharpe_ratio, trade_detail: list, email_subject: str):
    email_content = "<h1>没有发现买点</h1>"
    try:
        # if trade_analysis and sharpe_ratio:
        if trade_analysis:
            email_content = backtrader_email_body(trade_analysis, sharpe_ratio)
        if trade_detail:
            trade_detail = sorted(trade_detail, key=sort_by_gross_profit, reverse=True)
            df_trade_detail = pd.DataFrame(trade_detail)
            html_table = df_trade_detail.to_html(classes='table table-striped table-hover', border=0, index=False,
                                                 escape=False)
            styled_table = daily_email_style(html_table)
            email_content = email_content + "<br>" + styled_table

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


def notify_new_concept(new_concept_name: pd.DataFrame = None, new_concept_cons: pd.DataFrame = None, email_subject=None):
    concept_content = ""
    if not new_concept_name.empty:
        new_concept_name_table = new_concept_name.to_html(classes='table table-striped table-hover', border=0,
                                                          index=False, escape=False)
        concept_content += f"<h2>发现新的东方财富概念板块</h2>{new_concept_name_table}"
    if not new_concept_cons.empty:
        new_concept_cons_table = new_concept_cons.to_html(classes='table table-striped table-hover', border=0,
                                                          index=False, escape=False)
        concept_content += f"<h2>发现新的概念个股</h2>{new_concept_cons_table}"

    if concept_content:
        styled_table = daily_email_style(concept_content)
        send_email(styled_table, email_subject)


def notify_concept_radar(result_df: pd.DataFrame = None, email_subject=None):
    if result_df.empty:
        logger.info(f"notify_concept_radar receive empty result_df, {email_subject}")
    else:
        result_table = result_df.to_html(classes='table table-striped table-hover', border=0, index=False, escape=False)
        result_table += f'<a href="https://quote.eastmoney.com/center/boardlist.html#concept_board">东财板块概念</a>'
        styled_table = daily_email_style(result_table)
        send_email(styled_table, email_subject)


if __name__ == '__main__':
    send_email('hello', 'hello')
