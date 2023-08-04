import pytest

from airflow.exceptions import AirflowSkipException
from airflow.models import DagBag
from dags.get_weather import SQLExecuteQueryOptionalOperator


class MockExecuteReturn:
    @staticmethod
    def execute():
        return None


def test_dag_loaded():
    dagbag = DagBag()
    dag = dagbag.dagbag(dag_id="sandbox_data_pipeline__get_weather")
    assert dagbag.import_errors == {}
    assert dag is not None


def test_execute_skip_false(monkeypatch):
    def mock_execute(*_, **__):
        return MockExecuteReturn.execute()

    monkeypatch.setattr(SQLExecuteQueryOptionalOperator, "execute", mock_execute)

    op = SQLExecuteQueryOptionalOperator(
        task_id="test",
        sql="test",
        skip=False,
    )
    assert op.execute(context={}) is None


def test_execute_skip_true():
    op = SQLExecuteQueryOptionalOperator(
        task_id="test",
        sql="test",
        skip=True,
    )
    with pytest.raises(AirflowSkipException):
        op.execute(context={})
