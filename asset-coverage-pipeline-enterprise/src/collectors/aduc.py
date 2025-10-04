Active Directory Users and Computers
# fetch_assets.py
# ADUC Assets Pulling Script

from ldap3 import Server, Connection, NTLM, ALL, SUBTREE
import pandas as pd
import logging
import requests
import json
import pandas as pd
import time
from pandas import json_normalize
from pyspark.sql import SparkSession


# === AD Configuration ===
AD_SERVER   = "1*******"
AD_USER     = "*******"
AD_PASSWORD = "*******"

# === Logging Setup ===
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# === Region Data ===
continents = {
    "AS": {
        "name": "Asia",
        "regions": ["Hanam", "Jiangmen", "Langfang", "Seoul", "Shanghai", "ShanghaiK1",
                    "Shenzhen", "ShenzhenX", "Tianjin", "Tokyo"]
    },
    "EU": {
        "name": "Europe",
        "regions": ["Azure", "Mila", "Odelzhausen", "Pilsenzitivam", "Plzen",
                    "Prilep", "Stuttgart", "Tangier", "Treuchtlingen", "Vyhnova"]
    },
    "NA": {
        "name": "North America",
        "regions": ["Acuna", "Azure East US", "Burlington", "Celaya", "Cincinnati",
                    "Del Rio", "Greenville", "Monterrey", "Northville"]
    }
}

# === Attributes to Fetch ===
ATTRIBUTES = ["cn", "description", "operatingSystem", "lastLogonTimestamp"]

# === Connect to AD ===
def connect_to_ad():
    try:
        server = Server(AD_SERVER, get_info=ALL)
        conn = Connection(server,
                          user=AD_USER,
                          password=AD_PASSWORD,
                          authentication=NTLM,
                          auto_bind=True)
        logging.info("Connected to Active Directory.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to AD: {e}")
        return None

# === Parse description into name, date, time ===
def parse_description(desc):
    if not desc:
        return "", "", ""
    try:
        parts = desc.split()
        name_part = " ".join(parts[:3])
        date_part = parts[-2].replace("-", "").replace(".", "")
        time_part = parts[-1].replace("-", "").replace(".", "")
        return name_part, date_part, time_part
    except Exception:
        return "", "", ""

# === Fetch Region-Based Assets ===
def fetch_region_assets(conn):
    all_results = []
    for code, data in continents.items():
        continent = data["name"]
        for region in data["regions"]:
            ou_paths = [
                f"OU=Laptops,OU=Computers,OU={region},OU={code},OU=Regions,DC=Domain,DC=GlobalADS,DC=priv",
                f"OU=Workstations,OU=Computers,OU={region},OU={code},OU=Regions,DC=Domain,DC=GlobalADS,DC=priv",
                f"OU=Domain Controllers,OU={code},OU=Domain Controllers,DC=Domain,DC=GlobalADS,DC=priv"
            ]
            for asset_type, base_dn in zip(["Laptop", "Workstation", "Domain Controller"], ou_paths):
                logging.info(f"Searching in {continent}/{region} for {asset_type}...")
                try:
                    conn.search(
                        search_base=base_dn,
                        search_filter="(objectClass=computer)",
                        search_scope=SUBTREE,
                        attributes=ATTRIBUTES
                    )
                    if not conn.entries:
                        logging.info(f"No entries found in {base_dn}")
                        continue
                    for entry in conn.entries:
                        desc = str(entry.description) if entry.description else ""
                        name_part, date_part, time_part = parse_description(desc)
                        all_results.append({
                            "Continent": continent,
                            "Region": region,
                            "Asset Type": asset_type,
                            "Computer Name": str(entry.cn),
                            "Operating System": str(entry.operatingSystem),
                            "Raw Description": desc,
                            "Name from Desc": name_part,
                            "Date from Desc": date_part,
                            "Time from Desc": time_part,
                            "Last Logon Timestamp": str(entry.lastLogonTimestamp)
                        })
                except Exception as e:
                    logging.warning(f"Failed to pull from {base_dn}: {e}")
                    continue
    return all_results

# === Fetch Global Servers ===
def fetch_global_servers(conn):
    results = []
    server_dn = "OU=Servers,DC=Domain,DC=GlobalADS,DC=priv"
    logging.info("Searching for Global Servers...")
    try:
        conn.search(
            search_base=server_dn,
            search_filter="(objectClass=computer)",
            search_scope=SUBTREE,
            attributes=ATTRIBUTES
        )
        for entry in conn.entries:
            desc = str(entry.description) if entry.description else ""
            name_part, date_part, time_part = parse_description(desc)
            results.append({
                "Continent": "Global",
                "Region": "Global Servers",
                "Asset Type": "Server",
                "Computer Name": str(entry.cn),
                "Operating System": str(entry.operatingSystem),
                "Raw Description": desc,
                "Name from Desc": name_part,
                "Date from Desc": date_part,
                "Time from Desc": time_part,
                "Last Logon Timestamp": str(entry.lastLogonTimestamp)
            })
        logging.info("Global Servers fetched successfully.")
    except Exception as e:
        logging.warning(f"Failed to fetch Servers: {e}")
    return results

# === Run Script ===
if __name__ == "__main__":
    conn = connect_to_ad()
    if conn:
        region_assets = fetch_region_assets(conn)
        global_servers = fetch_global_servers(conn)
        conn.unbind()

        all_assets = region_assets + global_servers
        df = pd.DataFrame(all_assets)
        logging.info(f"Total records fetched: {len(df)}")
      # After building final pandas df = ...
spark_df = spark.createDataFrame(df)
spark_df.write.mode("overwrite").saveAsTable("security_nprod.db.raw.aduc_assets")
print("✅ Data saved to: security_nprod.db.raw.aduc_assets")

