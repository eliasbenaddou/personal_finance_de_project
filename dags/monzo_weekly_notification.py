import datetime
import os
from json import loads

from airflow.decorators import dag, task


class MonzoException(Exception):
    pass


default_args = {
    "owner": "airflow",
}


@dag(
    default_args=default_args,
    schedule_interval="30 9 * * MON",
    start_date=datetime.datetime(2022, 1, 1),
    catchup=False,
    max_active_runs=1,
)
def monzo_weekly_notification():
    """
    DAG to generate Monzo push notification of past week transactions by
    category.
    """

    @task()
    def get_monzo_auth():
        """
        Task to create Monzo authentication object

        Returns:
            Authenticaton object with tokens
        """
        from monzo.authentication import Authentication
        from monzo.handlers.filesystem import FileSystem

        with open(
            os.getenv("MONZO_TOKENS"),
            "r",
        ) as tokens:
            content = loads(tokens.read())

        monzo_auth_obj = Authentication(
            client_id=os.getenv("MONZO_CLIENT_ID"),
            client_secret=os.getenv("MONZO_CLIENT_SECRET"),
            redirect_url=os.getenv("MONZO_REDIRECT_URL"),
            access_token=content["access_token"],
            access_token_expiry=content["expiry"],
            refresh_token=content["refresh_token"],
        )
        handler = FileSystem(os.getenv("MONZO_TOKENS"))
        monzo_auth_obj.register_callback_handler(handler)

        return monzo_auth_obj

    @task()
    def get_past_week_transactions_agg():
        """
        Task to get Monzo transactions made in the past week

        Returns:
            Path to saved transactions JSON file
        """
        from common import sql_templates
        from common.db import Db

        past_week_transactions_agg_path = os.getenv("PAST_WEEK_MONZO_TRANSACTIONS_JSON")

        db = Db()

        past_week_transactions_agg_sql = (
            sql_templates.past_week_transactions_agg.format(
                schema="monzo", table="v_monzo_transactions"
            )
        )

        past_week_transactions_agg = db.query(sql=past_week_transactions_agg_sql)
        past_week_transactions_agg.to_json(
            past_week_transactions_agg_path, orient="records"
        )

        return past_week_transactions_agg_path

    @task()
    def create_feed_items(monzo_auth_obj, past_week_transactions_agg_path):
        """
        Task to create feed item notification using Monzo API package.

        Args:
            monzo_auth_obj: Monzo authentication object containing tokens
            past_week_transactions_agg_path: Path to saved JSON file

        Raises:
            MonzoError upon failure to create feed item
        """
        import pandas as pd
        from monzo.endpoints.account import Account
        from monzo.endpoints.feed_item import FeedItem
        from monzo.exceptions import MonzoError

        account_lst = Account.fetch(auth=monzo_auth_obj)
        current_account = account_lst[1]  # change after PR fullfilled

        with open(past_week_transactions_agg_path, "rb") as past_week_transactions_agg:
            past_week_transactions_agg_json = loads(past_week_transactions_agg.read())

        past_week_transactions_agg_df = pd.DataFrame(past_week_transactions_agg_json)
        past_week_transactions_agg_df.rename(
            columns={0: "category", 1: "frequency", 2: "total_amount"},
            inplace=True,
        )

        past_week_transactions_total = format(
            past_week_transactions_agg_df["total_amount"].sum(), ".2f"
        )
        past_week_transactions_top_category = past_week_transactions_agg_df["category"][
            0
        ]
        past_week_transactions_top_amount = past_week_transactions_agg_df[
            "total_amount"
        ][0]

        params = {
            "title": "💷 Your weekly spending update",
            "image_url": "https://play-lh.googleusercontent.com/aywdO4V_7CiuiggthXUWKy4zKlSt3CODeOEXVuGzQC94EGGi7jkZda5vGMzEFtnwVqFR",
            "body": f"You spent £{past_week_transactions_total} in the past week. \n Your top spending category was {past_week_transactions_top_category}, totalling £{past_week_transactions_top_amount}.",
            "title_color": "#9cb4b3",
        }

        try:
            FeedItem.create(
                auth=monzo_auth_obj,
                account_id=current_account.account_id,
                feed_type="basic",
                params=params,
            )
        except MonzoError:
            print("Failed to create feed item.")

    monzo_api_authentication = get_monzo_auth()
    create_feed_items(monzo_api_authentication, get_past_week_transactions_agg())


dag = monzo_weekly_notification()
