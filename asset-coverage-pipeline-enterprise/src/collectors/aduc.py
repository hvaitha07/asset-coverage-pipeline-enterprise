import logging, pandas as pd
from ldap3 import Server, Connection, NTLM, ALL, SUBTREE
from src.common.utils import write_delta

ATTRIBUTES = ["cn", "description", "operatingSystem", "lastLogonTimestamp"]

def connect(server, user, password):
    srv = Server(server, get_info=ALL)
    return Connection(srv, user=user, password=password, authentication=NTLM, auto_bind=True)

def parse_description(desc: str):
    if not desc: return "", "", ""
    try:
        parts = desc.split()
        name_part = " ".join(parts[:3])
        date_part = parts[-2].replace("-", "").replace(".", "")
        time_part = parts[-1].replace("-", "").replace(".", "")
        return name_part, date_part, time_part
    except Exception:
        return "", "", ""

def collect_aduc(server, user, password, table="security_nprod.db.raw.aduc_assets"):
    continents = {"NA":{"name":"North America","regions":["Acuna","Burlington","Celaya","Cincinnati","Greenville","Monterrey","Northville"]}}
    ou_tpl = [
        ("Laptop",           "OU=Laptops,OU=Computers,OU={region},OU={code},OU=Regions,DC=Domain,DC=GlobalADS,DC=priv"),
        ("Workstation",      "OU=Workstations,OU=Computers,OU={region},OU={code},OU=Regions,DC=Domain,DC=GlobalADS,DC=priv"),
        ("Domain Controller","OU=Domain Controllers,OU={code},OU=Domain Controllers,DC=Domain,DC=GlobalADS,DC=priv")
    ]
    conn = connect(server, user, password)
    all_rows = []
    for code, data in continents.items():
        for region in data["regions"]:
            for asset_type, dn_tpl in ou_tpl:
                base_dn = dn_tpl.format(region=region, code=code)
                try:
                    conn.search(base_dn, "(objectClass=computer)", SUBTREE, attributes=ATTRIBUTES)
                    for e in conn.entries:
                        desc = str(e.description) if e.description else ""
                        name_part, date_part, time_part = parse_description(desc)
                        all_rows.append({
                            "Continent": data["name"], "Region": region, "Asset Type": asset_type,
                            "Computer Name": str(e.cn), "Operating System": str(e.operatingSystem),
                            "Raw Description": desc, "Name from Desc": name_part,
                            "Date from Desc": date_part, "Time from Desc": time_part,
                            "Last Logon Timestamp": str(e.lastLogonTimestamp)
                        })
                except Exception as ex:
                    logging.warning(f"LDAP miss: {base_dn} -> {ex}")
    conn.unbind()
    df = pd.DataFrame(all_rows)
    write_delta(df, table)
