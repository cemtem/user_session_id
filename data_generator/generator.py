import os
import hashlib
import random
from datetime import datetime, timedelta

import pandas as pd

START_DATE = datetime.today().date() - timedelta(days=7)  # datetime(2025, 8, 8).date()  #
END_DATE = datetime.today().date() - timedelta(days=7)  # datetime(2025, 8, 8).date() #

NUM_USERS = 100

EVENT_IDS = ['a', 'b', 'c', 'd', 'e', 'f']
USER_EVENT_IDS = ['a', 'b', 'c']
PRODUCT_CODES = ['VS', 'IJ', 'PC']

MIN_EVENTS_PER_DAY = 100
MAX_EVENTS_PER_DAY = 300

os.makedirs("../source_data/events", exist_ok=True)

user_ids = [hashlib.sha256(str(i).encode("utf-8")).hexdigest() for i in range(NUM_USERS)]


def random_timestamp_on(date):
    dt = datetime.combine(date, datetime.min.time())
    seconds = random.randint(43200, 45900)
    return dt + timedelta(seconds=seconds)


for batch_offset in range((END_DATE - START_DATE).days + 1):
    batch_date = START_DATE + timedelta(days=batch_offset)
    days_delta = random.randint(1, 5)
    window_start = batch_date - timedelta(days=days_delta)
    window_end = batch_date

    records = []
    for single_date in pd.date_range(window_start, window_end):
        n = random.randint(MIN_EVENTS_PER_DAY, MAX_EVENTS_PER_DAY)
        for _ in range(n):
            eid = random.choice(EVENT_IDS)
            records.append({
                'user_id': random.choice(user_ids),
                'event_id': eid,
                'timestamp': random_timestamp_on(single_date.date()),
                'product_code': random.choice(PRODUCT_CODES),
            })
    df = pd.DataFrame(records)

    out_path = os.path.join("../source_data/events", f"batch_{batch_date}.parquet")
    df.to_parquet(out_path,
                  engine='pyarrow',
                  index=False,
                  coerce_timestamps="us",
                  allow_truncated_timestamps=True)
    print(f"Written {len(df):,} rows to {out_path}")
