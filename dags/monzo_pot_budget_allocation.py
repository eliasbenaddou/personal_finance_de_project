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
    schedule_interval="0 2 1 * *",
    start_date=datetime.datetime(2022, 1, 1),
    catchup=False,
    max_active_runs=1,
)
def monzo_pot_budget_allocation():
    """
    DAG to move money from main current account to Monzo Pots based on budget.
    Requires get_monzo_transactions/pot_budget.py dict populated.
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
    def deposit_into_pots(monzo_auth_obj):
        """
        Task to deposit funds into Monzo pots from main current account

        Args:
            monzo_auth_obj: Monzo Authentication object
        """
        import uuid

        from common.pot_budget import POT_BUDGET, POT_NAME_ID
        from monzo.endpoints.account import Account
        from monzo.endpoints.pot import Pot

        account_lst = Account.fetch(auth=monzo_auth_obj, account_type="uk_retail")
        current_account = account_lst[0]

        for pot_name, pot_id in POT_NAME_ID.items():
            pot_obj = Pot.fetch_single(
                monzo_auth_obj, current_account.account_id, pot_id
            )
            balance = pot_obj.balance

            if balance < POT_BUDGET[pot_name]:
                remainder = POT_BUDGET[pot_name] - balance
                Pot.deposit(
                    auth=monzo_auth_obj,
                    pot=pot_obj,
                    account_id=current_account.account_id,
                    amount=remainder,
                    dedupe_id=uuid.uuid4(),
                )

    monzo_api_authentication = get_monzo_auth()
    deposit_into_pots(monzo_api_authentication)


dag = monzo_pot_budget_allocation()
