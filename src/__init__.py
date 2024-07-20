from datetime import datetime


def is_friday(date_string):
    date = datetime.strptime(date_string, "%Y%m%d")
    return date.weekday() == 4
