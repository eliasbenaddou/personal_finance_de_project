import datetime
import os
from json import loads

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


class MonzoException(Exception):
    pass


default_args = {
    "owner": "airflow",
}


@dag(
    default_args=default_args,
    schedule_interval="0 9 * * *",
    start_date=datetime.datetime(2022, 1, 1),
    catchup=False,
    max_active_runs=1,
)
def monzo_transactions():
    """
    DAG to pull Monzo transactions in the past 90 days using Monzo API package.
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
    def get_monzo_transactions(monzo_auth_obj):
        """
        Task to pull all transactions from Monzo

        Args:
            monzo_auth_object: Authentication object with tokens

        Returns:
            Path to saved transactions JSON file
        """
        import pandas as pd
        from common.fetch_transactions import FetchTransactions

        transactions_path = os.getenv("MONZO_TRANSACTIONS_JSON")

        trn = FetchTransactions(monzo_auth_obj)

        all_transactions = []
        transactions = trn.fetch_transactions()
        all_transactions.append(transactions)

        final_transactions = pd.concat(all_transactions)
        final_transactions.to_json(transactions_path, orient="records")

        return transactions_path

    @task()
    def upload_monzo_transactions(transactions_path):
        """
        Task to upload transactions to database

        Args:
            transactions_path: path to the transaction JSON file

        Raises:
            MonzoExceptionError upon failure to upload transactions

        """
        import pandas as pd
        from common.upload_transactions import UploadTransactions

        with open(transactions_path, "rb") as transactions:
            final_transactions = loads(transactions.read())

        final_transactions_df = pd.DataFrame(final_transactions)

        final_transactions_df["date"] = pd.to_datetime(
            final_transactions_df["date"], unit="ms"
        )

        upload = UploadTransactions(
            transactions=final_transactions_df,
            schema=os.getenv("MONZO_SCHEMA"),
            table=os.getenv("MONZO_TABLE"),
        )

        new_transactions = upload.get_new_transactions()
        if len(new_transactions) > 0:
            try:
                upload.upload_new_transactions()
            except:
                raise MonzoException(
                    "An error occured while uploading new transactions"
                )

        changed_transactions = upload.get_changed_transactions()
        num_of_changed_trans = len(changed_transactions)

        if num_of_changed_trans > 0:
            try:
                upload.update_changed_transactions()
            except:
                raise MonzoException("No changed transactions to update")

    trigger_ge_checks = TriggerDagRunOperator(
        task_id="trigger_dag",
        trigger_dag_id="monzo_dq_checks",
        execution_date="{{ ds }}",
        reset_dag_run=True,
    )

    monzo_api_authentication = get_monzo_auth()
    pull_transactions = get_monzo_transactions(monzo_api_authentication)
    upload_monzo_transactions(pull_transactions) >> trigger_ge_checks


dag = monzo_transactions()
