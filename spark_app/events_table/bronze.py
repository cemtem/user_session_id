import argparse
import datetime as dt
from pathlib import Path

from delta import DeltaTable
from pyspark.sql import functions as F

from spark_app.ddl.bronze_ddl import ensure_bronze_table
from spark_app.utils.spark_session_utils import get_spark_session, get_default_config
from spark_app.utils.config import SOURCE_DIR, BRONZE_DIR


def _add_metadata_columns(df, as_of: str = None):
    if as_of:
        df = df.withColumn("_ingest_ts", F.to_timestamp(F.lit(as_of)))
    else:
        df = df.withColumn("_ingest_ts", F.current_timestamp())
    return (df
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_ingest_date", F.to_date("_ingest_ts")))


def _apply_date_filter(df, start_date: str = None, end_date: str = None):
    df = df.withColumn("date", F.to_date("timestamp"))
    if start_date and end_date:
        filtered_df = df.filter(
            (F.col("date") >= dt.datetime.strptime(start_date, "%Y-%m-%d")) &
            (F.col("date") <= dt.datetime.strptime(end_date, "%Y-%m-%d"))
        )
    else:
        filtered_df = df
    return filtered_df


def _read_source_data(spark, as_of: str = None):
    if as_of:
        source_path = f"{SOURCE_DIR}/batch_{as_of}.parquet"
    else:
        source_path = SOURCE_DIR
    return spark.read.parquet(f"{source_path}")


def _write_delta_table(spark, df):
    (DeltaTable
     .forPath(spark, Path(BRONZE_DIR).resolve().as_uri())
     .alias("t")
     .merge(
        df.alias("s"),
        """t.user_id = s.user_id 
        AND t.event_id = s.event_id 
        AND t.timestamp = s.timestamp
        AND t.product_code = s.product_code""")
     .whenNotMatchedInsertAll()
     .execute())


def ingest(start_date: str = None, end_date: str = None, as_of: str = None):
    ensure_bronze_table()

    spark = get_spark_session(configs=get_default_config())

    raw_df = _read_source_data(spark, as_of=as_of)
    filtered_df = raw_df.transform(_apply_date_filter, start_date=start_date, end_date=end_date)
    enriched_df = filtered_df.transform(_add_metadata_columns, as_of=as_of)

    _write_delta_table(spark, enriched_df)


def _parse_args():
    p = argparse.ArgumentParser(description="Bronze ingest")
    p.add_argument("--start-date", type=str, default=None, help="YYYY-MM-DD")
    p.add_argument("--end-date", type=str, default=None, help="YYYY-MM-DD")
    p.add_argument("--as-of", type=str, default=None, help="YYYY-MM-DD (selects batch_{as_of}.parquet)")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    ingest(start_date=args.start_date, end_date=args.end_date, as_of=args.as_of)
