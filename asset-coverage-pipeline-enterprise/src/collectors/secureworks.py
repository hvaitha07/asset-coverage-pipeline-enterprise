import time, requests, pandas as pd
from src.common.utils import write_delta

GQL = r'''
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
'''

def get_token(auth_url, client_id, client_secret):
    resp = requests.post(auth_url, data={"grant_type":"client_credentials","client_id":client_id,"client_secret":client_secret})
    if resp.status_code != 200:
        raise RuntimeError(f"Auth error {resp.status_code}: {resp.text}")
    return resp.json()["access_token"]

def collect_secureworks(auth_url, graphql_url, client_id, client_secret, tenant_id, table="security_nprod.db.raw.secureworks_assets"):
    token = get_token(auth_url, client_id, client_secret)
    cursor, all_rows = None, []
    headers = {"Authorization": f"Bearer {token}", "X-Tenant-Context": tenant_id}
    while True:
        query = {"query": GQL, "variables": {"first": 100, "after": cursor}}
        resp = requests.post(graphql_url, headers=headers, json=query)
        if resp.status_code != 200:
            raise RuntimeError(f"GraphQL error {resp.status_code}: {resp.text}")
        data = resp.json()["data"]["assetsV2"]
        all_rows.extend(data["assets"])
        if not data["pageInfo"]["hasNextPage"]: break
        cursor = data["pageInfo"]["endCursor"]
        time.sleep(0.2)
    df = pd.json_normalize(all_rows)
    write_delta(df, table)
