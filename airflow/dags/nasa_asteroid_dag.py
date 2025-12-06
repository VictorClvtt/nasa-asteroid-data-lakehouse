from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from include.src.etl.bronze_ingest import bronze_ingest

from include.src.etl.utils.variables import load_env_vars

access_key, secret_key, _, bucket_endpoint = load_env_vars()

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="nasa_asteroid_dag",
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    default_args=default_args,
    tags=["spark"]
)
def nasa_asteroid_dag():
    bronze_ingest_task = PythonOperator(
        task_id="bronze_ingest",
        python_callable=bronze_ingest,
    )

    bronze_to_silver_task = SparkSubmitOperator(
        task_id="bronze_to_silver",
        conn_id="my_spark_conn",
        application="include/src/etl/bronze_to_silver.py",
        conf={
            "spark.hadoop.fs.s3a.endpoint": bucket_endpoint,
            "spark.hadoop.fs.s3a.access.key": access_key,
            "spark.hadoop.fs.s3a.secret.key": secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.671",
        verbose=True,
    )

    silver_to_gold_task = SparkSubmitOperator(
        task_id="silver_to_gold",
        conn_id="my_spark_conn",
        application="include/src/etl/silver_to_gold.py",
        conf={
            "spark.hadoop.fs.s3a.endpoint": bucket_endpoint,
            "spark.hadoop.fs.s3a.access.key": access_key,
            "spark.hadoop.fs.s3a.secret.key": secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.671",
        verbose=True,
    )

    bronze_ingest_task >> bronze_to_silver_task >> silver_to_gold_task


nasa_asteroid_dag()
