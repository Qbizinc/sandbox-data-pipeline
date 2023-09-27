from diagrams import Diagram, Cluster, Edge
from diagrams.aws.compute import Lambda
from diagrams.aws.integration import SQS
from diagrams.aws.management import ParameterStore
from diagrams.aws.network import APIGateway
from diagrams.aws.storage import S3
from diagrams.custom import Custom
from diagrams.gcp.analytics import BigQuery
from diagrams.gcp.storage import GCS

# Custom shape for Snowflake
snowflake_icon = "/home/airflow/airflow/diagram_images/snowflake.png"
rapidapi_icon = "/home/airflow/airflow/diagram_images/rapidapi.png"
airflow_icon = "/home/airflow/airflow/diagram_images/airflow.png"
weatherapi_icon = "/home/airflow/airflow/diagram_images/weatherapi.png"
gcs_transfer_icon = "/home/airflow/airflow/diagram_images/google-cloud-data-transfer.png"

if __name__ == "__main__":
    with Diagram("", show=False, filename="diagram_images/sandbox_data_pipeline"):
        with Cluster("AWS"):

            s3 = S3("S3")
            sqs = SQS("SQS")
            lambda_function = Lambda("Lambda")
            api_gateway = APIGateway("API Gateway")
            parameter_store = ParameterStore("Parameter Store")

            parameter_store >> api_gateway >> lambda_function

            lambda_function >> s3
            s3 >> sqs

        with Cluster("GCP"):
            bigquery = BigQuery("BigQuery")
            gcs_transfer = Custom("GCS Transfer", gcs_transfer_icon)
            gcs = GCS("GCS")

            gcs_transfer >> gcs >> bigquery

        with Cluster("Snowflake (optional)"):
            snowflake = Custom("", snowflake_icon)
            s3 >> Edge(style="dotted") >> snowflake

        with Cluster("RapidAPI"):
            rapidapi = Custom("rapidapi.com", weatherapi_icon)
            # lambda_function >> rapidapi
            rapidapi >> Edge(reverse=True) >> lambda_function

        sqs >> gcs_transfer
