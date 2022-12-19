from airflow.models import DagBag


def test_no_import_errors():

    dag_bag = DagBag()
    assert len(dag_bag.import_errors) == 0, "No import errors"
    assert dag_bag.size() == 0
