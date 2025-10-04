import requests
import time
import json
import pandas as pd
from pandas import json_normalize
from pyspark.sql import SparkSession


# ==== Auth ====
AUTH_URL = "https://api.delta.taegis.secureworks.com/auth/token"
GRAPHQL_URL = "https://api.delta.taegis.secureworks.com/graphql"
CLIENT_ID = "your_client_id"
CLIENT_SECRET = "your_client_secret"
TENANT_ID = "your_tenant_id"

def get_token():
    resp = requests.post(AUTH_URL, data={
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    })
    return resp.json()["access_token"]

def fetch_all_assets(token, page_size=100):
    cursor = None
    all_assets = []
    while True:
        query = {
            "query": """
            query ($first: Int, $after: String) {
                assetsV2(first: $first, after: $after) {
                    pageInfo { endCursor hasNextPage }
                    assets {
                        hostnames { hostname }
                        ipAddresses { ip }
                        users { username }
                        operatingSystem
                        createdAt
                        updatedAt
                    }
                }
            }
            """,
            "variables": {"first": page_size, "after": cursor}
        }
        headers = {"Authorization": f"Bearer {token}", "X-Tenant-Context": TENANT_ID}
        resp = requests.post(GRAPHQL_URL, headers=headers, json=query)
        data = resp.json()["data"]["assetsV2"]
        all_assets.extend(data["assets"])
        if not data["pageInfo"]["hasNextPage"]:
            break
        cursor = data["pageInfo"]["endCursor"]
        time.sleep(0.3)
    return all_assets

if __name__ == "__main__":
    token = get_token()
    assets = fetch_all_assets(token)
    keys = ["hostnames", "ipAddresses", "users", "operatingSystem", "createdAt", "updatedAt"]
    with open("secureworks_assets.csv", "w", newline="", encoding="utf-8") as f:
        # After building final pandas df = ...
spark_df = spark.createDataFrame(df)
spark_df.write.mode("overwrite").saveAsTable("security_nprod.db.raw.secureworks_assets")
print("✅ Data saved to: security_nprod.db.raw.secureworks_assets")
