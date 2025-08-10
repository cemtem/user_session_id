from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from spark_app.utils.config import WAREHOUSE_DIR


def get_spark_session(app_name: str = "TechTask",
                      configs: dict = None) -> SparkSession:
    spark = SparkSession.getActiveSession()
    if spark:
        return spark

    builder = SparkSession.builder.appName(app_name)
    if configs:
        for key, value in configs.items():
            builder = builder.config(key, value)

    builder = configure_spark_with_delta_pip(builder)

    return builder.getOrCreate()


def get_default_config() -> dict:
    return {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.warehouse.dir": WAREHOUSE_DIR,
    }
