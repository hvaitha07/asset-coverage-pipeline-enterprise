from pyspark.sql import functions as F, SparkSession
spark = SparkSession.builder.getOrCreate()

baramundi   = spark.table("security_nprod.db.raw.baramundi_assets_data")
cortex      = spark.table("security_nprod.db.raw.cortex_assets")
lansweeper  = spark.table("security_nprod.db.raw.lansweeper_assets")
secureworks = spark.table("security_nprod.db.raw.secureworks_assets")
aduc        = spark.table("security_nprod.db.raw.aduc_assets")

def norm(expr): return F.upper(F.split(F.trim(expr), "\.")[0])

base = cortex.select(norm(F.col("endpoint.name")).alias("hostname_norm")).distinct()

joined = base \
  .join(lansweeper.select(norm(F.col("name")).alias("hostname_norm")).distinct().withColumn("in_lansweeper", F.lit(1)),"hostname_norm","left") \
  .join(baramundi.select(norm(F.col("Name")).alias("hostname_norm")).distinct().withColumn("in_baramundi", F.lit(1)),"hostname_norm","left") \
  .join(secureworks.select(norm(F.get_json_object(F.col("hostnames"), "$.hostname")).alias("hostname_norm")).distinct().withColumn("in_secureworks", F.lit(1)),"hostname_norm","left") \
  .join(aduc.select(norm(F.col("Computer Name")).alias("hostname_norm")).distinct().withColumn("in_aduc", F.lit(1)),"hostname_norm","left")

out = joined.fillna(0)
out = out.withColumn("tools_covered", F.col("in_baramundi")+F.col("in_lansweeper")+F.col("in_secureworks")+F.col("in_aduc"))
out = out.withColumn("coverage_category",
    F.when(F.col("tools_covered")==4, F.lit("Fully Covered"))
     .when(F.col("tools_covered")==0, F.lit("Not Covered"))
     .otherwise(F.lit("Partially Covered"))
)

out.write.mode("overwrite").saveAsTable("security_nprod.db.raw.gap_data")
print("✅ Wrote: security_nprod.db.raw.gap_data")
