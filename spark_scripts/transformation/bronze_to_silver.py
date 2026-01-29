from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

spark = SparkSession.builder \
    .appName("Bronze_to_Silver_Optimization") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

def transform_to_silver(table_name):
    bronze_path = f"/opt/airflow/data/bronze/{table_name}"
    silver_path = f"/opt/airflow/data/silver/{table_name}"
    
    if not os.path.exists(bronze_path): 
        print(f"Path not found: {bronze_path}")
        return

    print(f"✨ Processing Silver for: {table_name}")
    df = spark.read.format("delta").load(bronze_path)

    # Làm sạch & Ép kiểu chuẩn
    if table_name == "products":
        # Dùng double cho weight để khớp với dữ liệu nguồn
        df = df.withColumn("base_price", col("base_price").cast("decimal(10,2)")) \
               .withColumn("weight", col("weight").cast("double")) \
               .filter(col("product_id").isNotNull())
    
    elif table_name == "clickstream":
        df = df.withColumn("timestamp", col("timestamp").cast("timestamp")) \
               .withColumn("stay_duration", col("stay_duration").cast("int")) \
               .filter(col("user_id").isNotNull())

    # Ghi dữ liệu với option mergeSchema
    df.write.format("delta").mode("overwrite") \
      .option("mergeSchema", "true") \
      .save(silver_path)

    # Tối ưu hóa Z-Ordering 
    try:
        if table_name == "clickstream":
            spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY (user_id, product_id)")
        elif table_name == "products":
            spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY (category_id)")
    except Exception as e:
        print(f" Optimize skipped for {table_name}: {e}")

    print(f" Silver table {table_name} is READY!")

tables = ["products", "orders", "users", "categories", "order_items", "clickstream"]
for t in tables:
    transform_to_silver(t)

spark.stop()