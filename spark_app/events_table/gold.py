import argparse

import pyspark.sql.functions as F
from delta import DeltaTable
from pyspark.sql import Window

import datetime as dt

from spark_app.ddl.gold_ddl import ensure_gold_table
from spark_app.utils.spark_session_utils import get_spark_session, get_default_config
from spark_app.utils.config import SILVER_DIR, GOLD_DIR

SESSION_DURATION = 300


def _read_silver_delta_table_filtered(spark, start_date: str = None, end_date: str = None):
    dates = []
    if start_date and end_date:
        s_date = dt.datetime.strptime(start_date, "%Y-%m-%d")
        e_date = dt.datetime.strptime(end_date, "%Y-%m-%d")
        for day in range((e_date - s_date).days + 1):
            dates.append((s_date + dt.timedelta(days=day)))
    else:
        dates.append(str(dt.date.today()))

    return (spark
            .read
            .format("delta")
            .load(SILVER_DIR.resolve().as_uri())
            .where(F.col("date").isin(dates))
            .select("user_id",
                    "event_id",
                    "product_code",
                    "timestamp",
                    "date",
                    "_source_file",
                    "_ingest_ts",
                    "_ingest_date")
            )


def _read_gold_delta_table_filtered(spark, affected_dates_df):
    return (spark
            .read
            .format("delta")
            .load(GOLD_DIR.resolve().as_uri())
            .join(affected_dates_df, "date", "left_semi")
            .select("user_id",
                    "event_id",
                    "product_code",
                    "timestamp",
                    "date",
                    "_source_file",
                    "_ingest_ts",
                    "_ingest_date")
            )


def _extract_affected_dates(df):
    return df.select("date").distinct()


def _add_session_start_ts(df):
    window_spec = Window.partitionBy("user_id", "product_code").orderBy("ts_long")
    return (df
            .withColumn("ts_long", F.unix_timestamp("timestamp").cast("long"))
            .withColumn("prev_session", F.lag("ts_long").over(window_spec))
            .withColumn("session_start",
                        F.when(F.col("ts_long") - F.col("prev_session") <= SESSION_DURATION, 0)
                        .otherwise(F.col("ts_long"))
                        )
            .withColumn("session_start_ts", F.max("session_start").over(window_spec))
            )


def _add_user_session_id(df):
    return df.select("user_id",
                     "event_id",
                     "product_code",
                     "timestamp",
                     "date",
                     F.concat(F.col("user_id"),
                              F.lit("#"),
                              F.col("product_code"),
                              F.lit("#"),
                              F.col("session_start_ts"))
                     .alias("user_session_id"),
                     "_source_file",
                     "_ingest_ts",
                     "_ingest_date")


def _write_delta_table(spark, df):
    (DeltaTable
     .forPath(spark, GOLD_DIR.resolve().as_uri())
     .alias("s")
     .merge(
        df.alias("t"),
        """s.user_id = s.user_id
        AND s.event_id = t.event_id
        AND s.timestamp = t.timestamp
        AND s.product_code = t.product_code
        AND s.user_session_id = t.user_session_id""")
     .whenNotMatchedInsertAll()
     .execute())


def gold_process(start_date: str = None, end_date: str = None):
    ensure_gold_table()

    spark = get_spark_session(configs=get_default_config())

    silver_df = _read_silver_delta_table_filtered(spark, start_date, end_date)
    affected_dates_df = silver_df.transform(_extract_affected_dates)
    existing_df = _read_gold_delta_table_filtered(spark, affected_dates_df)

    union_df = silver_df.union(existing_df).distinct()
    with_user_session_id_df = (union_df
                               .transform(_add_session_start_ts)
                               .transform(_add_user_session_id)
                               )

    _write_delta_table(spark, with_user_session_id_df)


def _parse_args():
    p = argparse.ArgumentParser(description="Bronze ingest")
    p.add_argument("--start-date", type=str, default=None, help="YYYY-MM-DD")
    p.add_argument("--end-date", type=str, default=None, help="YYYY-MM-DD")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    gold_process(start_date=args.start_date, end_date=args.end_date)
