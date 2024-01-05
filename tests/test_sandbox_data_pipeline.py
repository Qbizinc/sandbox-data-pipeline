import pytest
from airflow.exceptions import AirflowSkipException
from airflow.models import DagBag

from dags.sandbox_data_pipeline import SQLExecuteQueryOptionalOperator, clean_cocktail_json


class MockExecuteReturn:
    @staticmethod
    def execute():
        return None


def test_dag_loaded():
    dagbag = DagBag()
    dag = dagbag.dagbag(dag_id="sandbox_data_pipeline")
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


def test_clean_cocktail_json():
    dirty_json = """{'drinks': [{'idDrink': '11470', 'strAlcoholic': 'Alcoho\rli\n\rc',}]}"""
    clean_json = """{"drinks": [{"idDrink": "11470", "strAlcoholic": "Alcoholic"}]}"""
    assert clean_cocktail_json(dirty_json) == clean_json
