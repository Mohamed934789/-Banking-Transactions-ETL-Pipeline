import os
import pandas as pd
os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

spark = SparkSession.builder \
    .appName("BankingETL-Load") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✓ Spark ready")

HDFS_STAGED = "hdfs://hadoop-namenode:9000/banking/staged"

conn = snowflake.connector.connect(
    account  = "ZNMDVIU-VY47245",
    user     = "MOHAMEDKASSAB",
    password = "Mm123456789Mm@",
    database = "BANKING_DWH",
    schema   = "STAR_SCHEMA",
    warehouse= "COMPUTE_WH",
    role     = "ACCOUNTADMIN",
)
print("✓ Snowflake connected")

def load_to_snowflake(table_name):
    print(f"\nLoading {table_name}...")
    df_spark  = spark.read.parquet(f"{HDFS_STAGED}/{table_name}")
    df_pandas = df_spark.toPandas()
    df_pandas.columns = [col.upper() for col in df_pandas.columns]

    # FIX: حوّل كل datetime columns لـ string
    for col in df_pandas.columns:
        if df_pandas[col].dtype == "datetime64[ns]" or \
           str(df_pandas[col].dtype).startswith("datetime"):
            df_pandas[col] = df_pandas[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    print(f"  Rows: {len(df_pandas)}")
    success, nchunks, nrows, _ = write_pandas(
        conn,
        df_pandas,
        table_name.upper(),
        auto_create_table=False,
        overwrite=True,
        quote_identifiers=False,
    )
    print(f"  ✓ Done: {nrows} rows")

load_to_snowflake("dim_date")
load_to_snowflake("dim_account")
load_to_snowflake("dim_merchant")
load_to_snowflake("dim_transaction_type")
load_to_snowflake("fact_transactions")

conn.close()
print("\n✓✓✓ Load complete!")