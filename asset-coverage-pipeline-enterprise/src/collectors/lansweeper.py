import requests
import time
import json
import requests
import json
import pandas as pd
import time
from pandas import json_normalize
from pyspark.sql import SparkSession

# ==== Authentication ====
TOKEN = "your_token"
SITE_ID = "your_site_id"
url = "https://api.lansweeper.com/v2/graphql"

headers = {"Authorization": f"Token {TOKEN}", "Content-Type": "application/json"}

all_assets = []
page = "FIRST"
cursor_value = None

def build_query(page, cursor):
    cursor_block = f', cursor: "{cursor}"' if cursor else ""
    return {
        "query": f"""
        query {{
            site(id: "{SITE_ID}") {{
                assetResources {{
                    items {{
                        assetBasicInfo {{
                            name
                            ipAddress
                            mac
                            lastSeen
                            operatingSystem {{ caption }}
                        }}
                    }}
                    pagination {{ page next limit }}
                }}
            }}
        }}
        """
    }

while True:
    query = build_query(page, cursor_value)
    response = requests.post(url, headers=headers, json=query)
    if response.status_code != 200:
        print("Error:", response.text)
        break
    result = response.json()
    items = result["data"]["site"]["assetResources"]["items"]
    all_assets.extend(items)
    pagination = result["data"]["site"]["assetResources"]["pagination"]
    if not pagination.get("next"):
        break
    cursor_value = pagination["next"]
    time.sleep(0.3)

with open("lansweeper_assets.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["name", "ipAddress", "mac", "lastSeen"])
    writer.writeheader()
    for asset in all_assets:
        writer.writerow(asset["assetBasicInfo"])

After building final pandas df = ...
spark_df = spark.createDataFrame(df)
spark_df.write.mode("overwrite").saveAsTable("security_nprod.db.raw.lansweeper_assets")
print("Data saved to: security_nprod.db.raw.lansweeper_assets")
        
