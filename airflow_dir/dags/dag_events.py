from datetime import datetime
from airflow import DAG
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
        dag_id="etl_ide_events",
        start_date=datetime(2025, 8, 1),
        schedule_interval="@daily",
        catchup=False,
        tags=["spark", "bronze"],
        params={
            "start_date": Param("auto", type="string", description="Defaults to ds - 5 days"),
            "end_date": Param("auto", type="string", description="Defaults to ds"),
            "as_of": Param("auto", type="string", description="Defaults to ds"),
        },
) as dag:
    bronze = SparkSubmitOperator(
        task_id="ingest_bronze",
        application="/opt/spark/apps/spark_app/events_table/bronze.py",
        name="ingest_bronze",
        conn_id="spark_cluster",
        packages="io.delta:delta-spark_2.12:3.2.0",
        application_args=[
            "--start-date", "{{ params.start_date or macros.ds_add(ds, -5) }}",
            "--end-date", "{{ params.end_date or ds }}",
            "--as-of", "{{ params.as_of or ds }}",
        ],
        verbose=True,
    )

    silver = SparkSubmitOperator(
        task_id="ingest_silver",
        application="/opt/spark/apps/spark_app/events_table/silver.py",
        name="ingest_silver",
        conn_id="spark_cluster",
        packages="io.delta:delta-spark_2.12:3.2.0",
        application_args=[
            "--start-date", "{{ params.start_date or macros.ds_add(ds, -5) }}",
            "--end-date", "{{ params.end_date or ds }}",
            "--as-of", "{{ params.as_of or ds }}",
        ],
        verbose=True,
    )

    gold = SparkSubmitOperator(
        task_id="ingest_gold",
        application="/opt/spark/apps/spark_app/events_table/gold.py",
        name="ingest_gold",
        conn_id="spark_cluster",
        packages="io.delta:delta-spark_2.12:3.2.0",
        application_args=[
            "--start-date", "{{ params.start_date or macros.ds_add(ds, -5) }}",
            "--end-date", "{{ params.end_date or ds }}",
        ],
        verbose=True,
    )

    bronze >> silver >> gold
