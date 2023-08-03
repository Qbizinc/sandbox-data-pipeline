from diagrams import Diagram, Cluster, Edge
from diagrams.aws.compute import EC2
from diagrams.aws.storage import S3
from diagrams.custom import Custom

# Custom shape for Snowflake
snowflake_icon = "/home/airflow/airflow/diagram_images/snowflake.png"
rapidapi_icon = "/home/airflow/airflow/diagram_images/rapidapi.png"
airflow_icon = "/home/airflow/airflow/diagram_images/airflow.png"
weatherapi_icon = "/home/airflow/airflow/diagram_images/weatherapi.png"

if __name__ == "__main__":

    with Diagram("Data Workflow", show=False, filename="diagram_images/sandbox_data_pipeline"):
        with Cluster("AWS"):
            airflow = Custom("Airflow (EC2)", airflow_icon)
            s3 = S3("S3")
        with Cluster("Snowflake"):
            stage_table = Custom("stage table", snowflake_icon)
            weather_table = Custom("weather table", snowflake_icon)

        with Cluster("RapidAPI"):
            rapidapi = Custom("WeatherAPI.com", weatherapi_icon)

        rapidapi >>Edge(style="dotted", label="airflow") >> s3 >> Edge(style="dotted", label="airflow") \
        >> stage_table >> Edge(style="dotted", label="airflow") >> weather_table
        # airflow >> Edge(style="dotted", label="airflow") >> rapidapi
        # airflow >> rapidapi
        # rapidapi >> s3
        # airflow >> s3
        # airflow >> snowflake
        # s3 >> snowflake