# Cortex Endpoint Pull Script
# File: cortex_endpoints.py

import requests
import pandas as pd
import time
import requests
import json
import pandas as pd
import time
from pandas import json_normalize
from pyspark.sql import SparkSession

# ==== Auth ====
API_KEY_ID = "Your_API_Key_ID"
API_KEY    = "Your_API_Key"
FQDN       = "api-gentherm.xdr.eu.paloaltonetworks.com"

URL = f"https://{FQDN}/public_api/v1/endpoints/get_endpoint"

headers = {
    "x-xdr-auth-id": API_KEY_ID,
    "Authorization": API_KEY,
    "Content-Type": "application/json"
}

# ==== Pull all pages ====
all_endpoints = []
search_from = 0
batch_size = 100

while True:
    payload = {
        "request_data": {
            "filters": [],
            "search_from": search_from,
            "search_to": search_from + batch_size
        }
    }

    response = requests.post(URL, headers=headers, json=payload)

    if response.status_code != 200:
        print(f"❌ Error (status {response.status_code}): {response.text}")
        break

    endpoints = response.json().get("reply", {}).get("endpoints", [])
    if not endpoints:
        print("✅ All endpoints pulled.")
        break

    all_endpoints.extend(endpoints)
    print(f"📥 Pulled {len(endpoints)}; Total so far: {len(all_endpoints)}")

    search_from += batch_size
    time.sleep(0.3)

# ==== Normalize JSON into table ====
df = pd.json_normalize(all_endpoints)

# ==== Clean list/str columns ====
for col in df.columns:
    df[col] = df[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
    df[col] = df[col].apply(lambda x: str(x).replace('\n', ' ').replace('\r', ' ') if isinstance(x, str) else x)

# ==== Convert last_seen ONLY ====
def fix_epoch_ms(ms):
    try:
        return pd.to_datetime(int(float(ms)), unit='ms').date()
    except:
        return None

if 'last_seen' in df.columns:
    df['last_seen'] = df['last_seen'].apply(fix_epoch_ms)

# ==== Preview ====
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 250)
pd.set_option('display.max_colwidth', 100)

print("👀 PREVIEW: First 10 Endpoint Records (Clean View)")
print(df.head(10))

spark_df = spark.createDataFrame(df)
spark_df.write.mode("overwrite").saveAsTable("security_nprod.db.raw.cortex_assets")
print("✅ Data saved to: security_nprod.db.raw.cortex_assets")
