from pathlib import Path
import os

WAREHOUSE_DIR = Path(os.getenv("WAREHOUSE_DIR", "../../data_warehouse")).resolve()
SOURCE_DIR = Path(os.getenv("SOURCE_DIR", "../../source_data/events")).resolve()

BRONZE_DIR = WAREHOUSE_DIR / "bronze" / "events"
SILVER_DIR = WAREHOUSE_DIR / "silver" / "events"
GOLD_DIR = WAREHOUSE_DIR / "gold" / "events"
