import logging
import os
import sys
from datetime import datetime, timedelta

import pandas as pd

if logging.getLogger().hasHandlers():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
else:
    os.makedirs(os.path.join(os.getcwd(), "logs"), exist_ok=True)
    file_handler = logging.FileHandler(filename="./logs/log.log")
    file_formatter = logging.Formatter(
        "[%(asctime)s] - %(name)s - %(levelname)s - %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_formatter)
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_formatter = logging.Formatter(
        "[%(asctime)s] %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    stdout_handler.setFormatter(stdout_formatter)
    handlers = [file_handler, stdout_handler]
    logging.basicConfig(handlers=handlers)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)


def get_timestamp(string=False):
    """
    Function to get the current timestamp or from a string

    Args:
        string: Timestamp in string format "%Y-%m-%d %H:%M:%S"

    Returns:
        Timestamp
    """
    ts = datetime.today()
    if string:
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    else:
        return ts


def get_date_periods(account_obj):
    """
    Function to split age of account into 3 month periods for use in requests

    Args:
        account_obj: Account object from Monzo API Account endpoint

    Returns:
        List of tuples of date periods
    """
    created_date = account_obj.created.date()
    end = datetime.today().date()

    quarter_end = pd.to_datetime(
        end + pd.tseries.offsets.QuarterEnd(startingMonth=3)
    ).date()

    dates = pd.date_range(start=created_date, end=quarter_end, freq="3M").date.tolist()
    if created_date < dates[0]:
        dates = [created_date] + dates
    if end > dates[-1]:
        dates.append(end)

    date_tuple_lst = []
    while True:
        position = 0
        if len(dates) > 2:
            date_tuple_lst.append(
                (dates[position], dates[position + 1] - timedelta(days=1))
            )
            dates.pop(position)
            position += 2
        else:
            date_tuple_lst.append((dates[-2], dates[-1]))
            break
    return date_tuple_lst
