import time, requests, pandas as pd
from src.common.utils import write_delta

GQL = r'''
query($sid: ID!, $cursor: String) {
  site(id: $sid) {
    assetResources {
      items {
        assetBasicInfo {
          name
          ipAddress
          mac
          lastSeen
          operatingSystem { caption }
        }
      }
      pagination { page next limit }
    }
  }
}
'''

def collect_lansweeper(token, site_id, url="https://api.lansweeper.com/v2/graphql", table="security_nprod.db.raw.lansweeper_assets"):
    headers = {"Authorization": f"Token {token}", "Content-Type": "application/json"}
    cursor, all_assets = None, []
    while True:
        q = {"query": GQL, "variables": {"sid": site_id, "cursor": cursor}}
        resp = requests.post(url, headers=headers, json=q)
        if resp.status_code != 200:
            raise RuntimeError(f"Lansweeper error {resp.status_code}: {resp.text}")
        data = resp.json()["data"]["site"]["assetResources"]
        items = data["items"]
        all_assets.extend([x["assetBasicInfo"] for x in items])
        nxt = data["pagination"].get("next")
        if not nxt: break
        cursor = nxt
        time.sleep(0.2)
    df = pd.json_normalize(all_assets)
    write_delta(df, table)
