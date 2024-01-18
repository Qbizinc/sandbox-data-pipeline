import ast
from typing import Sequence

from airflow.exceptions import AirflowSkipException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context


class SQLExecuteQueryOptionalOperator(SQLExecuteQueryOperator):
    """
    A SQLExecuteQueryOperator that can be optionally skipped.
    skip: If True, skip the task.
    """
    def __init__(self, skip=False, **kwargs):
        super().__init__(**kwargs)
        self.skip = skip

    def execute(self, context):
        if self.skip:
            self.log.info("received skip=True arg, skipping task")
            raise AirflowSkipException
        else:
            super().execute(context)


class GCSObjectListExistenceSensor(BaseSensorOperator):
    """
    Checks for the existence of a list of objects in a Google Cloud Storage bucket.
    bucket: The name of the GCS bucket.
    prefix: The prefix of the objects to check.
    object_list: A STRING representing a list of objects to check. Note this is a string
        and not a list in order to accommodate xcom_pulls in templating.
    google_cloud_conn_id: The connection ID to use when connecting to Google Cloud.
    """
    template_fields: Sequence[str] = (
        "bucket",
        "prefix",
        "object_list",
    )
    ui_color = "#f0eee4"

    def __init__(
            self,
            *,
            bucket: str,
            prefix: str,
            object_list: str,
            google_cloud_conn_id: str = "google_cloud_default",
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.google_cloud_conn_id = google_cloud_conn_id
        self.object_list = object_list

    def poke(self, context: Context) -> bool:
        # Convert the object list from a string to a list. See docstring for more info.
        _object_list = ast.literal_eval(self.object_list)
        hook = GCSHook(
            gcp_conn_id=self.google_cloud_conn_id,
        )
        for object in _object_list:
            object = object.lower().replace(" ", "_") + ".json"
            self.log.info("Sensor checks existence of : %s, %s", self.bucket, f"{self.prefix}/{object}")
            if not hook.exists(self.bucket, f"{self.prefix}/{object}"):
                return False
        return True
