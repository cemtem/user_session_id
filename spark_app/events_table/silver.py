import argparse
import datetime
from datetime import timedelta
from pathlib import Path

from delta import DeltaTable

import pyspark.sql.functions as F
from spark_app.ddl.silver_ddl import ensure_silver_table
from spark_app.utils.spark_session_utils import get_spark_session, get_default_config
from spark_app.utils.config import BRONZE_DIR, SILVER_DIR

POSSIBLE_USER_ACTIONS = "possible_user_actions.csv"
user_actions = ["a", "b", "c"]


def _read_delta_table_filtered(spark, start_date: str = None, end_date: str = None, as_of: str = None):
    dates = []
    if start_date and end_date:
        s_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        e_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        for day in range((e_date - s_date).days + 1):
            dates.append((s_date + timedelta(days=day)))
        date_filter = (F.col("date").isin(dates))
    else:
        date_filter = (F.lit(True))

    if as_of:
        date_filter = date_filter & (F.col("_ingest_date") == as_of)

    return (spark.read
            .format("delta")
            .load(BRONZE_DIR.resolve().as_uri())
            .where(date_filter)
            )


def _extract_distinct_user_actions(df):
    return (df.alias("l")
            .where(F.col("event_id").isin(user_actions))
            .distinct()
            )


def _write_delta_table(spark, df):
    (DeltaTable
     .forPath(spark, Path(SILVER_DIR).resolve().as_uri())
     .alias("t")
     .merge(
        df.alias("s"),
        """t.user_id = s.user_id 
        AND t.event_id = s.event_id 
        AND t.timestamp = s.timestamp
        AND t.product_code = s.product_code""")
     .whenNotMatchedInsertAll()
     .execute())


def silver_process(start_date: str = None, end_date: str = None, as_of: str = None):
    ensure_silver_table()

    spark = get_spark_session(configs=get_default_config())

    bronze_df = _read_delta_table_filtered(spark, start_date, end_date, as_of=as_of)
    filtered_df = bronze_df.transform(_extract_distinct_user_actions)

    _write_delta_table(spark, filtered_df)


def _parse_args():
    p = argparse.ArgumentParser(description="Bronze ingest")
    p.add_argument("--start-date", type=str, default=None, help="YYYY-MM-DD")
    p.add_argument("--end-date", type=str, default=None, help="YYYY-MM-DD")
    p.add_argument("--as-of", type=str, default=None, help="YYYY-MM-DD (selects batch_{as_of}.parquet)")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    silver_process(start_date=args.start_date, end_date=args.end_date, as_of=args.as_of)
