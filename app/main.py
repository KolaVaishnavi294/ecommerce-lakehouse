import os
import boto3
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.types import *
from delta.tables import DeltaTable

# ==============================
# 1️⃣ CREATE MINIO BUCKET
# ==============================

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)

buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]

if "data" not in buckets:
    s3.create_bucket(Bucket="data")
    print("Bucket 'data' created")
else:
    print("Bucket already exists")

# ==============================
# 2️⃣ CREATE SPARK SESSION
# ⚠️ DO NOT SET master() HERE
# ==============================

builder = SparkSession.builder \
    .appName("Ecommerce Lakehouse") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

print("Spark Session Created")

# ==============================
# 3️⃣ PRODUCTS SCHEMA ENFORCEMENT
# ==============================

product_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("stock_quantity", IntegerType(), True),
])

products_df = spark.read.csv(
    "/data/products.csv",
    header=True,
    schema=product_schema
)

# Write partitioned Delta table
products_path = "s3a://data/warehouse/products"

products_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("category") \
    .save(products_path)

print("Products Delta table created")

# Add CHECK constraint
spark.sql(f"""
ALTER TABLE delta.`{products_path}`
ADD CONSTRAINT price_positive CHECK (price > 0)
""")

print("Constraint added")

# ==============================
# 4️⃣ CUSTOMERS + MERGE
# ==============================

customers_df = spark.read.option("header", True).csv("/data/customers.csv")
updates_df = spark.read.option("header", True).csv("/data/updates.csv")

customers_path = "s3a://data/warehouse/customers"

customers_df.write.format("delta").mode("overwrite").save(customers_path)

delta_table = DeltaTable.forPath(spark, customers_path)

delta_table.alias("t").merge(
    updates_df.alias("s"),
    "t.customer_id = s.customer_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print("Merge completed")

# ==============================
# 5️⃣ TIME TRAVEL DEMO
# ==============================

spark.sql(f"""
SELECT * FROM delta.`{customers_path}` VERSION AS OF 0
""").show()

print("Time travel verified")

# ==============================
# 6️⃣ OPTIMIZE + ZORDER
# ==============================

spark.sql(f"""
OPTIMIZE delta.`{products_path}`
ZORDER BY (product_id)
""")

print("OPTIMIZE completed")

# ==============================
# 7️⃣ VACUUM
# ==============================

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

spark.sql(f"""
VACUUM delta.`{customers_path}` RETAIN 0 HOURS
""")

print("VACUUM completed")

# ==============================
# 8️⃣ STREAMING INGESTION
# ==============================

sales_path = "s3a://data/warehouse/sales"

sales_stream = spark.readStream \
    .option("header", True) \
    .schema("sale_id INT, product_id INT, customer_id INT, quantity INT, sale_date STRING") \
    .csv("/data")

query = sales_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://data/warehouse/checkpoints/sales") \
    .trigger(once=True) \
    .start(sales_path)

query.awaitTermination()

print("Streaming ingestion completed")

spark.stop()
print("ALL TASKS COMPLETED SUCCESSFULLY")
