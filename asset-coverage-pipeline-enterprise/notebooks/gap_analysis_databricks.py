# ===============================================================
# Gap Analysis in PySpark (Databricks)
# ===============================================================

# Step 0: Libraries / Imports
from pyspark.sql.functions import (
    col, upper, trim, substring_index, get_json_object,
    to_timestamp, when
)

# ===============================================================
# Step 1: Load raw source tables
# ===============================================================
baramundi_df   = spark.table("security_nprod.db.raw.baramundi_assets_data")
cortex_df      = spark.table("security_nprod.db.raw.cortex_assets_data")
lansweeper_df  = spark.table("security_nprod.db.raw.lansweeper_assets_data")
secureworks_df = spark.table("security_nprod.db.raw.secureworks_assets_data")
aduc_df        = spark.table("security_nprod.db.raw.aduc_assets_data")

# ===============================================================
# Step 2: Quick System Counts (raw distinct hostnames per tool)
# ===============================================================
print("🔢 Unique Systems per Tool (raw load):")
print(f"Baramundi: {baramundi_df.select('Name').distinct().count()}")
print(f"Cortex: {cortex_df.select('endpoint.name').distinct().count()}")
print(f"Lansweeper: {lansweeper_df.select('assetBasicInfo.name').distinct().count()}")
print(f"SecureWorks: {secureworks_df.select(get_json_object(col('hostnames'), '$.hostname')).distinct().count()}")
print(f"ADUC: {aduc_df.select('Computer Name').distinct().count()}")

# ===============================================================
# Step 3: Normalize Hostnames
# ===============================================================
def normalize_hostname(df, col_expr):
    return df.withColumn("hostname_norm", upper(substring_index(trim(col_expr), ".", 1)))

baramundi_df   = normalize_hostname(baramundi_df, col("Name"))
cortex_df      = normalize_hostname(cortex_df, col("endpoint.name"))
lansweeper_df  = normalize_hostname(lansweeper_df, col("assetBasicInfo.name"))
secureworks_df = normalize_hostname(secureworks_df, get_json_object(col("hostnames"), "$.hostname"))
aduc_df        = normalize_hostname(aduc_df, col("Computer Name"))

# ===============================================================
# Step 4: Parse Dates / Timestamps
# ===============================================================
baramundi_df   = baramundi_df.withColumn("last_contact_ts",
                        to_timestamp(col("Last contact"), "M/d/yyyy H:mm:ss a"))
cortex_df      = cortex_df.withColumn("last_seen_ts",
                        (col("last_seen")/1000).cast("timestamp"))
lansweeper_df  = lansweeper_df.withColumn("last_seen_ts",
                        to_timestamp(col("assetBasicInfo.lastSeen")))
secureworks_df = secureworks_df.withColumn("updated_ts",
                        to_timestamp(col("updatedAt")))
aduc_df        = aduc_df.withColumn("last_logon_ts",
                        to_timestamp(col("Last Logon Timestamp")))

# ===============================================================
# Step 5: Cortex Superset Base
# ===============================================================
cortex_base = cortex_df.select(
    col("hostname_norm"),
    col("Computer Name"),
    col("endpoint_status").alias("Connection Status"),
    col("last_seen_ts").alias("Cortex Last Seen")
).dropDuplicates()

# ===============================================================
# Step 6: Prepare Join DFs
# ===============================================================
baramundi_seen  = baramundi_df.select("hostname_norm", col("last_contact_ts").alias("Baramundi Last Seen")).dropDuplicates()
lansweeper_seen = lansweeper_df.select("hostname_norm", col("last_seen_ts").alias("Lansweeper Last Seen")).dropDuplicates()
secureworks_seen= secureworks_df.select("hostname_norm", col("updated_ts").alias("SecureWorks Last Seen")).dropDuplicates()
aduc_seen       = aduc_df.select(
                        "hostname_norm",
                        col("last_logon_ts").alias("ADUC Last Logon"),
                        "Asset Type","Operating System","Continent","Region"
                ).dropDuplicates()

# ===============================================================
# Step 7: Join Everything into Cortex Superset
# ===============================================================
df = (cortex_base
    .join(baramundi_seen,  "hostname_norm", "left")
    .join(lansweeper_seen, "hostname_norm", "left")
    .join(secureworks_seen,"hostname_norm", "left")
    .join(aduc_seen,       "hostname_norm", "left")
)

# ===============================================================
# Step 8: Coverage Flags + Tools Covered
# ===============================================================
df = df.withColumn("Covered in Cortex",     when(col("Cortex Last Seen").isNotNull(), "Yes").otherwise("No")) \
       .withColumn("Covered in Baramundi",  when(col("Baramundi Last Seen").isNotNull(), "Yes").otherwise("No")) \
       .withColumn("Covered in Lansweeper", when(col("Lansweeper Last Seen").isNotNull(), "Yes").otherwise("No")) \
       .withColumn("Covered in SecureWorks",when(col("SecureWorks Last Seen").isNotNull(), "Yes").otherwise("No")) \
       .withColumn("Covered in ADUC",       when(col("ADUC Last Logon").isNotNull(), "Yes").otherwise("No"))

df = df.withColumn("Tools Covered",
          (col("Covered in Cortex")=="Yes").cast("int") +
          (col("Covered in Baramundi")=="Yes").cast("int") +
          (col("Covered in Lansweeper")=="Yes").cast("int") +
          (col("Covered in SecureWorks")=="Yes").cast("int") +
          (col("Covered in ADUC")=="Yes").cast("int")
)

# ===============================================================
# Step 9: Coverage Category
# ===============================================================
df = df.withColumn("Coverage Category",
          when(col("Tools Covered") == 5, "Fully Covered")
         .when(col("Tools Covered") == 0, "Not Covered")
         .otherwise("Partially Covered")
)

# ===============================================================
# Step 10: Final Column Selection
# ===============================================================
final_cols = [
    "Computer Name","Connection Status","Asset Type","Operating System",
    "Continent","Region",
    "Covered in Cortex","Covered in Baramundi","Covered in Lansweeper",
    "Covered in SecureWorks","Covered in ADUC",
    "Tools Covered","Coverage Category",
    "Cortex Last Seen","Baramundi Last Seen","Lansweeper Last Seen",
    "SecureWorks Last Seen","ADUC Last Logon"
]

df = df.select(final_cols)

# ===============================================================
# Step 11: Write Results to Delta Table
# ===============================================================
df.write.mode("overwrite").saveAsTable("security_nprod.db.raw.gap_data")
print("✅ Data written to security_nprod.db.raw.gap_data")

# ===============================================================
# Step 12: Coverage Summary + Post-checks
# ===============================================================
print("📊 Coverage Summary:")
df.groupBy("Coverage Category").count().show()

print("🔢 Unique Systems per Tool (normalized hostnames):")
print(f"Baramundi: {baramundi_df.select('hostname_norm').distinct().count()}")
print(f"Cortex: {cortex_df.select('hostname_norm').distinct().count()}")
print(f"Lansweeper: {lansweeper_df.select('hostname_norm').distinct().count()}")
print(f"SecureWorks: {secureworks_df.select('hostname_norm').distinct().count()}")
print(f"ADUC: {aduc_df.select('hostname_norm').distinct().count()}")
