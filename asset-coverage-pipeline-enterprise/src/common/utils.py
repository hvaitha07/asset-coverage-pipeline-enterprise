import re
from datetime import datetime
from pyspark.sql import SparkSession

def spark():
    return SparkSession.builder.getOrCreate()

def normalize_hostname(host: str) -> str:
    if not host:
        return ""
    host = host.strip().split(".")[0]
    return re.sub(r"[^A-Za-z0-9\-]", "", host).upper()

def epoch_ms_to_ts(ms):
    try:
        ms = int(float(ms))
        return datetime.utcfromtimestamp(ms/1000.0)
    except Exception:
        return None

def write_delta(df, table):
    s = spark()
    sdf = s.createDataFrame(df)
    sdf.write.mode("overwrite").saveAsTable(table)
    print(f"✅ Data saved to: {table}")
