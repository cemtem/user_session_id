from pathlib import Path

from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DateType, TimestampNTZType
)
from delta.tables import DeltaTable
from spark_app.utils.spark_session_utils import get_spark_session, get_default_config
from spark_app.utils.config import SILVER_DIR, GOLD_DIR


def ensure_gold_table():
    spark = get_spark_session(configs=get_default_config())

    db_name = "ide_info"
    table_name = f"{db_name}.events_gold"

    bronze_path = Path(SILVER_DIR).resolve()
    warehouse_root = bronze_path.parents[1]
    db_folder = warehouse_root / f"{db_name}.db"
    db_uri = db_folder.as_uri()

    spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS {db_name}
            LOCATION '{db_uri}'
        """)

    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("timestamp", TimestampNTZType(), False),
        StructField("product_code", StringType(), True),
        StructField("date", DateType(), True),
        StructField("user_session_id", StringType(), False),
        StructField("_source_file", StringType(), False),
        StructField("_ingest_ts", TimestampType(), False),
        StructField("_ingest_date", StringType(), False),
    ])

    (DeltaTable.createIfNotExists(spark)
     .tableName(table_name)
     .addColumns(schema)
     .partitionedBy("date")
     .location(GOLD_DIR.as_uri())
     .execute()
     )
