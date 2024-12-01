import datetime
import argparse

from src.concept import update
from src.notify import notify_new_concept


def update_concept_cons_and_alarm(subj_lv1="自动盯盘"):
    new_concept_name, new_concept_cons = update.fetch_and_store_concept_cons()
    edt: str = datetime.datetime.now().strftime('%Y%m%d')
    if new_concept_name or new_concept_cons:
        email_subject = (f"[{subj_lv1}][概念板块][A股]{edt}发现{len(new_concept_name)}"
                         f"个新板块概念共{len(new_concept_cons)}个个股")

        notify_new_concept(new_concept_name, new_concept_cons, email_subject)


def update_concept_new_stock(subj_lv1="自动盯盘"):
    concept_new_stock = update.fetch_and_store_concept_new_stock()
    edt: str = datetime.datetime.now().strftime('%Y%m%d')
    if concept_new_stock:
        email_subject = f"[{subj_lv1}][概念板块][A股]{edt}发现{len(concept_new_stock)}个个股有板块变动"
        notify_new_concept(new_concept_cons=concept_new_stock, email_subject=email_subject)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="这是一个示例程序")
    # 添加参数
    parser.add_argument("-n", "--name", action="store_true", help="板块概念排名变化")
    parser.add_argument("-c", "--cons", action="store_true", help="新板块概念及个股列表")
    parser.add_argument("-s", "--stocks", action="store_true", help="旧板块概念新个股")

    # 解析参数
    args = parser.parse_args()

    if args.name:
        update.fetch_and_store_concept_name()
    if args.cons:
        update_concept_cons_and_alarm()
    if args.stocks:
        update_concept_new_stock()
