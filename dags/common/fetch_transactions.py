import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from monzo.endpoints.account import Account
from monzo.endpoints.transaction import Transaction

from .categories import cat_dict
from .utils import get_date_periods

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class FetchTransactions:
    """
    Class to get transactions from Monzo using monzo-api package
    """

    def __init__(self, monzo_auth=None):
        """
        Initialise FetchTransactions

        Args:
            monzo_auth: Monzo authentication object
        """
        self.monzo_auth = monzo_auth

    def fetch_transactions(self, history=False):
        """
        Method to fetch transactions from Monzo

        Args:
            history: Boolean for fetching all transactions, defaults to 'False'

        Returns:
            Transactions dataframe or None
        """
        account_lst = Account.fetch(auth=self.monzo_auth, account_type="uk_retail")
        current_account = account_lst[0]

        if not history:
            fetched_transactions = Transaction.fetch(
                auth=self.monzo_auth,
                account_id=current_account.account_id,
                since=datetime.today() - timedelta(days=89),
                expand=["merchant"],
            )
        else:
            fetched_transactions_lst = []
            periods = get_date_periods(current_account)

            try:
                for (since, before) in periods:

                    fetched_transactions = Transaction.fetch(
                        auth=self.monzo_auth,
                        account_id=current_account.account_id,
                        since=since,
                        before=before,
                        expand=["merchant"],
                    )
                    fetched_transactions_lst.append(fetched_transactions)
                    num_of_transactions = len(fetched_transactions)
                    print(
                        f"Fetched {num_of_transactions} transactions for dates: {since} to {before}"
                    )
            except:
                print(
                    "Failed to fetch transactions for some dates - make sure to trigger DAG immediately after refreshing permissions"
                )

            fetched_transactions = [
                item for sublist in fetched_transactions_lst for item in sublist
            ]

        total_num_of_transactions = len(fetched_transactions)
        print(f"Fetched a total of {total_num_of_transactions} transactions")
        transactions_dict = {}

        for index, trn in enumerate(fetched_transactions):

            id = trn.transaction_id
            date = trn.created
            description = trn.description
            amount = trn.amount
            category = trn.category
            decline_reason = trn.decline_reason
            meta = trn.metadata
            merchant = trn.merchant

            transactions_dict[index] = (
                id,
                date,
                description,
                amount,
                category,
                decline_reason,
                meta,
                merchant,
            )

        transactions_df = pd.DataFrame(transactions_dict).T
        transactions_df.rename(
            columns={
                0: "id",
                1: "date",
                2: "description",
                3: "amount",
                4: "category",
                5: "decline_reason",
                6: "meta",
                7: "merchant",
            },
            inplace=True,
        )

        if len(transactions_df) > 0:

            metadata = pd.json_normalize(transactions_df["meta"])
            transactions_df = transactions_df.merge(
                metadata, left_index=True, right_index=True
            )

            merchant_data = pd.json_normalize(transactions_df["merchant"])
            transactions_df = transactions_df.merge(
                merchant_data, left_index=True, right_index=True
            )

            transactions_df["decline"] = np.where(
                transactions_df["decline_reason"] == "", 0, 1
            )

            columns_dict = {
                "id_x": "id",
                "id_y": "merchant_id",
                "name": "merchant_description",
                "category_x": "category",
                "category_y": "merchant_category",
            }
            transactions_df.rename(columns=columns_dict, inplace=True)
            transactions_df["category"].replace(cat_dict, inplace=True)

            transactions_df["amount"] = (
                transactions_df["amount"].apply(lambda x: -1 * (x / 100)).astype(float)
            )
            transactions_df.reset_index(drop=True, inplace=True)

            transactions_df["amount"] = pd.to_numeric(
                transactions_df["amount"], downcast="float"
            )

            transactions = transactions_df[
                [
                    "id",
                    "date",
                    "description",
                    "emoji",
                    "amount",
                    "category",
                    "decline_reason",
                    "decline",
                    "notes",
                    "merchant_id",
                    "merchant_category",
                    "merchant_description",
                    "suggested_tags",
                ]
            ]

        else:
            transactions = None
        return transactions
