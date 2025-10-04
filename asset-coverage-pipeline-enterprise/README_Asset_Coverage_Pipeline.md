# Asset Coverage Pipeline (Databricks Lakehouse → Power BI)

Unifies endpoint inventories from **Cortex XDR, Active Directory (ADUC), Lansweeper, and Secureworks Taegis** into **Delta Lake** on **Databricks**, computes **coverage gaps**, and powers **self‑serve KPIs** in **Power BI**.

> **Goal:** give SecOps/IT an authoritative view of **Fully / Partially / Not Covered** assets, reduce blind spots, and eliminate weekly manual reconciliation.

---

## 🧭 Architecture (ELT)

```
[Cortex][ADUC][Lansweeper][Secureworks]
          │  Python Collectors (REST / LDAP / GraphQL)
          ▼
      Delta Lake  (security_nprod.db.raw.*)
          │  PySpark join + rules
          ▼
  raw.gap_data (coverage flags & category)
          │  Databricks SQL / View
          ▼
        Power BI  (Live dashboards)
```

- **Secrets** via **Databricks Secret Scopes** — no plaintext credentials.
- **Delta** tables for reproducibility, history, and BI performance.

---

## 📦 Repository Layout

```
src/
  collectors/
    cortex.py         # paginated REST -> Delta
    aduc.py           # LDAP OU scan -> Delta
    lansweeper.py     # GraphQL (cursor) -> Delta
    secureworks.py    # GraphQL (cursor + token) -> Delta
  common/
    utils.py          # Spark helper, hostname normalization, write_delta()
notebooks/
  gap_analysis_databricks.py  # joins + flags + category -> raw.gap_data
infra/
  requirements.txt    # requests, pandas, ldap3 (Spark provided by DBR)
  secrets_template.md # keys/endpoints you set in a secret scope
docs/
  screenshots/        # Power BI & notebook screenshots
tests/
  test_utils.py
.github/workflows/
  ci.yml              # optional CI for tests
```

---

## 🔐 Secrets (example scope: `security-ingest`)

Create a **Secret Scope** in Databricks and add:

```
# Cortex XDR
CORTEX_API_KEY_ID
CORTEX_API_KEY
CORTEX_FQDN  # e.g., api-tenant.xdr.eu.paloaltonetworks.com

# ADUC (LDAP)
AD_SERVER     # ldaps://ad.domain.example
AD_USER       # DOMAIN\svc.reader
AD_PASSWORD

# Lansweeper
LS_TOKEN
LS_SITE_ID
LS_URL        # https://api.lansweeper.com/v2/graphql

# Secureworks Taegis
SW_AUTH_URL       # https://api.delta.taegis.secureworks.com/auth/token
SW_GRAPHQL_URL    # https://api.delta.taegis.secureworks.com/graphql
SW_CLIENT_ID
SW_CLIENT_SECRET
SW_TENANT_ID
```

Use in code:
```python
api_key = dbutils.secrets.get("security-ingest", "CORTEX_API_KEY")
```

---

## ⚙️ Setup

1. **Cluster:** DBR 13–14.x, Python 3.10+.  
2. **Install deps:**  
   ```
   %pip install -r infra/requirements.txt
   ```
3. **Tables naming (default):**
   - `security_nprod.db.raw.cortex_assets`
   - `security_nprod.db.raw.aduc_assets`
   - `security_nprod.db.raw.lansweeper_assets`
   - `security_nprod.db.raw.secureworks_assets`
   - Output: `security_nprod.db.raw.gap_data`

> Adjust database names once in the scripts if your environment differs.

---

## 🚚 Ingest (Run Collectors)

Run each script as a Databricks **notebook** or **job**:

- `src/collectors/cortex.py`  
  - REST pagination (`search_from`, `search_to`), list→CSV cleanup, write Delta.
- `src/collectors/aduc.py`  
  - LDAP bind (NTLM), region OU iteration, description parsing, write Delta.
- `src/collectors/lansweeper.py`  
  - GraphQL with cursor pagination, normalize, write Delta.
- `src/collectors/secureworks.py`  
  - Client‑credentials → GraphQL pagination, normalize, write Delta.

---

## 🔎 Gap Analysis (PySpark)

Open `notebooks/gap_analysis_databricks.py` and run:

- **Normalize hostnames:** strip domain, uppercase, sanitize.
- **Join** all sources to a base set (Cortex by default) on normalized hostname.
- **Flags:** `in_aduc`, `in_lansweeper`, `in_secureworks`, `in_baramundi` (if applicable).  
- **`tools_covered`** = sum of flags.  
- **`coverage_category`** =  
  - `Fully Covered` (all flags present)  
  - `Not Covered` (0 flags)  
  - `Partially Covered` (otherwise)

Writes → `security_nprod.db.raw.gap_data`.

> Optional: publish a view `security_nprod.db.curated.gap_coverage_v` for BI.

---

## 📊 Power BI Wiring

1. Connect Power BI to **Databricks SQL** (catalog/schema of your tables).  
2. Use `raw.gap_data` (or curated view) and build visuals:
   - **Slicers:** Region, Site, Coverage Category, Asset Type  
   - **Cards:** Total Asset Count; Has ADUC / Lansweeper / Cortex / Secureworks (Yes/No)  
   - **Table:** Computer Name, coverage flags, last‑seen columns  
   - **Donuts:** Fully vs Partially vs Not Covered by tool  
3. (Optional) Add freshness and trend pages if you schedule daily jobs.

---

## 📈 KPIs

- **Coverage %** by tool / region / site / asset type  
- **Unmanaged assets** (assets covered by 0 tools)  
- **Last‑seen freshness** distribution per source  
- **Trend** of coverage over time (if scheduled)

---

## 🧮 Example SQL View (for BI)

```sql
CREATE OR REPLACE VIEW security_nprod.db.curated.gap_coverage_v AS
SELECT
  hostname_norm,
  in_aduc, in_lansweeper, in_secureworks, in_baramundi,
  tools_covered,
  coverage_category
FROM security_nprod.db.raw.gap_data;
```

---

## 🛡️ Governance & Security

- No secrets in code; **all credentials** come from Secret Scopes.  
- Restrict workspace access to least privilege.  
- Consider separate **prod/nonprod** schemas (e.g., `security_prod`, `security_nprod`).  
- Use **Delta history**/time‑travel for audits.

---

## ⏱️ Scheduling (Databricks Jobs)

- **Task 1–4:** run each collector (parallel).  
- **Task 5:** run `gap_analysis_databricks.py`.  
- **Task 6 (optional):** create/refresh curated view.  
- Set notifications and retries; schedule daily or hourly.

---

## 🧪 Tests

Run unit tests (example included for hostname normalization):
```
pytest -q
```

---

## 🧰 Troubleshooting

- **Empty joins?** Ensure hostname normalization is applied the same way in all sources.  
- **Slow queries in BI?** Create a curated table/view, add Z‑Ordering on hostname, and reduce wide columns.  
- **Auth failures?** Re‑check secret names/values in the secret scope and token TTLs.

---

## 📌 Roadmap

- Promote to `curated.gap_coverage` (Delta Live Tables or scheduled ETL).  
- Add freshness SLA metrics & incident drill‑downs.  
- Add Tenable (if used) and other EDR/CMDB sources.  
- CI: linting/type checks for collectors.

---

## 👤 Credits

Authored by Harsha Vardhan • Master’s project (Asset Coverage) — Databricks Lakehouse + Power BI.
