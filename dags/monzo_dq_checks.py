import datetime

from airflow import DAG
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}

ge_root_dir = "/opt/airflow/great_expectations"

with DAG(
    "monzo_dq_checks",
    default_args=default_args,
    start_date=datetime.datetime(2022, 1, 1),
    catchup=False,
    max_active_runs=1,
    schedule_interval=None,
) as dag:
    """
    DAG to run Great Expectations Checkpoint against database table for
    data quality checks.
    """

    great_expectations_transactions_dq_checkpoint = GreatExpectationsOperator(
        task_id="great_expectations_transactions_dq_checkpoint",
        data_context_root_dir=ge_root_dir,
        checkpoint_name="monzo_transactions_dq_checks",
        fail_task_on_validation_failure=False,
        checkpoint_kwargs={
            "validations": [
                {
                    "batch_request": {
                        "datasource_name": "monzo_transactions",
                        "data_connector_name": "default_inferred_data_connector_name",
                        "data_asset_name": "monzo.v_monzo_transactions",
                        "data_connector_query": {"index": -1},
                    },
                    "expectation_suite_name": "monzo_transactions_dq_checks",
                },
            ]
        },
    )

great_expectations_transactions_dq_checkpoint
