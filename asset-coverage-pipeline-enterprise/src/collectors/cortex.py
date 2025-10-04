import time, requests, pandas as pd
from pandas import json_normalize
from src.common.utils import write_delta

def collect_cortex(api_key_id, api_key, fqdn, table="security_nprod.db.raw.cortex_assets"):
    url = f"https://{fqdn}/public_api/v1/endpoints/get_endpoint"
    headers = {"x-xdr-auth-id": api_key_id, "Authorization": api_key, "Content-Type": "application/json"}
    all_endpoints, search_from, batch = [], 0, 100
    while True:
        payload = {"request_data": {"filters": [], "search_from": search_from, "search_to": search_from + batch}}
        resp = requests.post(url, headers=headers, json=payload)
        if resp.status_code != 200:
            raise RuntimeError(f"Cortex error {resp.status_code}: {resp.text}")
        endpoints = resp.json().get("reply", {}).get("endpoints", [])
        if not endpoints: break
        all_endpoints.extend(endpoints); search_from += batch; time.sleep(0.2)
    df = json_normalize(all_endpoints)
    for c in df.columns:
        df[c] = df[c].apply(lambda x: ", ".join(map(str, x)) if isinstance(x, list) else x)
        df[c] = df[c].apply(lambda x: str(x).replace("\n", " ").replace("\r", " ") if isinstance(x, str) else x)
    write_delta(df, table)
