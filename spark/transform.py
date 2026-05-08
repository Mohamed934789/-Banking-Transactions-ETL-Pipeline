import os
os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

# ── Spark Session ──────────────────────────────────────────
spark = SparkSession.builder \
    .appName("BankingETL-Transform") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✓ Spark ready")


HDFS_RAW    = "hdfs://hadoop-namenode:9000/banking/raw"
HDFS_STAGED = "hdfs://hadoop-namenode:9000/banking/staged"

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{HDFS_RAW}/*/*")

print(f"✓ Raw records: {df.count()}")



from pyspark.sql.types import DoubleType, IntegerType

# ── Clean ──────────────────────────────────────────────────
df = df.dropDuplicates()
df = df.dropna(subset=["TransactionID", "AccountID",
                        "TransactionAmount", "TransactionDate"])

# FIX النهائي: try_to_timestamp على String مباشرة
df = df.withColumn("TransactionDate",
    F.coalesce(
        F.expr("try_to_timestamp(TransactionDate, 'M/d/yyyy H:mm')"),
        F.expr("try_to_timestamp(TransactionDate, 'M/d/yyyy')"),
        F.expr("try_to_timestamp(TransactionDate, 'MM/dd/yyyy HH:mm')"),
        F.expr("try_to_timestamp(TransactionDate, 'MM/dd/yyyy')"),
        F.expr("try_to_timestamp(TransactionDate, 'yyyy-MM-dd HH:mm:ss')"),
        F.expr("try_to_timestamp(TransactionDate, 'yyyy-MM-dd')"),
    ))

df = df.withColumn("TransactionAmount",
        F.abs(F.col("TransactionAmount").cast(DoubleType()))) \
       .withColumn("AccountBalance",
        F.col("AccountBalance").cast(DoubleType())) \
       .withColumn("CustomerAge",
        F.col("CustomerAge").cast(IntegerType())) \
       .withColumn("TransactionDuration",
        F.col("TransactionDuration").cast(IntegerType())) \
       .withColumn("LoginAttempts",
        F.col("LoginAttempts").cast(IntegerType()))

df = df.withColumn("is_suspicious",
    F.when((F.col("TransactionAmount") > 10000) |
           (F.col("LoginAttempts") > 3), True).otherwise(False))

df = df.withColumn("txn_year",  F.year("TransactionDate")) \
       .withColumn("txn_month", F.month("TransactionDate")) \
       .withColumn("txn_day",   F.dayofmonth("TransactionDate")) \
       .withColumn("txn_hour",  F.hour("TransactionDate")) \
       .withColumn("txn_date",  F.to_date("TransactionDate"))

df = df.filter(F.col("TransactionDate").isNotNull())
df.cache()

total = df.count()
nulls = df.filter(F.col("TransactionDate").isNull()).count()
print(f"✓ Clean records: {total}")
print(f"  Null dates:    {nulls}")



# ── Star Schema ────────────────────────────────────────────
dim_date = df.select(
    F.col("txn_date").alias("date"),
    F.col("txn_year").alias("year"),
    F.col("txn_month").alias("month"),
    F.col("txn_day").alias("day"),
    F.dayofweek("TransactionDate").alias("day_of_week"),
    F.date_format("TransactionDate", "EEEE").alias("day_name"),
    F.date_format("TransactionDate", "MMMM").alias("month_name"),
    F.quarter("TransactionDate").alias("quarter"),
).dropDuplicates(["date"]) \
 .withColumn("date_id", F.monotonically_increasing_id())
print(f"✓ dim_date:     {dim_date.count()} rows")

dim_account = df.select(
    F.col("AccountID").alias("account_id"),
    F.col("CustomerAge").alias("customer_age"),
    F.col("CustomerOccupation").alias("occupation"),
    F.col("AccountBalance").alias("account_balance"),
).dropDuplicates(["account_id"])
print(f"✓ dim_account:  {dim_account.count()} rows")

dim_merchant = df.select(
    F.col("MerchantID").alias("merchant_id"),
    F.col("Location").alias("location"),
    F.col("Channel").alias("channel"),
).dropDuplicates(["merchant_id"])
print(f"✓ dim_merchant: {dim_merchant.count()} rows")

dim_txn_type = df.select(
    F.col("TransactionType").alias("transaction_type"),
).dropDuplicates(["transaction_type"]) \
 .withColumn("type_id", F.monotonically_increasing_id())
print(f"✓ dim_type:     {dim_txn_type.count()} rows")

fact = df.join(
    dim_date.select("date","date_id"),
    df["txn_date"] == dim_date["date"], "left"
).join(
    dim_txn_type.select("transaction_type","type_id"),
    df["TransactionType"] == dim_txn_type["transaction_type"], "left"
).select(
    F.col("TransactionID").alias("transaction_id"),
    F.col("AccountID").alias("account_id"),
    F.col("MerchantID").alias("merchant_id"),
    F.col("date_id"),
    F.col("type_id"),
    F.col("TransactionAmount").alias("amount"),
    F.col("AccountBalance").alias("balance"),
    F.col("TransactionDuration").alias("duration_seconds"),
    F.col("LoginAttempts").alias("login_attempts"),
    F.col("DeviceID").alias("device_id"),
    F.col("IP Address").alias("ip_address"),
    F.col("TransactionDate").alias("transaction_date"),
    F.col("is_suspicious"),
)
print(f"✓ fact:         {fact.count()} rows")

# ── Write to HDFS ──────────────────────────────────────────
def write_parquet(df, name):
    path = f"{HDFS_STAGED}/{name}"
    df.write.mode("overwrite").parquet(path)
    print(f"✓ Written: {name}")

write_parquet(dim_date,     "dim_date")
write_parquet(dim_account,  "dim_account")
write_parquet(dim_merchant, "dim_merchant")
write_parquet(dim_txn_type, "dim_transaction_type")
write_parquet(fact,         "fact_transactions")

print("\n✓✓✓ Transform complete!")
spark.stop()