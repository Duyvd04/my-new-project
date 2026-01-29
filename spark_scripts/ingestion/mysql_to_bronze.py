from pyspark.sql import SparkSession
from airflow.hooks.base import BaseHook
import os

# 1. Kết nối Airflow để lấy bảo mật (Yêu cầu I.3)
conn = BaseHook.get_connection("mysql_conn")
mysql_url = f"jdbc:mysql://{conn.host}:{conn.port}/{conn.schema}"

spark = SparkSession.builder \
    .appName("MySQL_to_Bronze_Incremental") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,mysql:mysql-connector-java:8.0.28") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

def ingest_table(table_name):
    bronze_path = f"/opt/airflow/data/bronze/{table_name}"
    
    # 2. Logic High-Watermark (Yêu cầu I.2)
    last_timestamp = "1900-01-01 00:00:00"
    if os.path.exists(bronze_path):
        try:
            # Đọc mốc thời gian mới nhất từ dữ liệu Delta hiện có
            last_timestamp = spark.read.format("delta").load(bronze_path) \
                .selectExpr("max(updated_at)").collect()[0][0] or "1900-01-01 00:00:00"
        except: pass

    # 3. Chỉ lấy phần chênh lệch (Delta)
    query = f"(SELECT * FROM {table_name} WHERE updated_at > '{last_timestamp}') as incremental_df"
    
    df = spark.read.format("jdbc") \
        .option("url", mysql_url) \
        .option("dbtable", query) \
        .option("user", conn.login) \
        .option("password", conn.password) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()

    if df.count() > 0:
        # THÊM mergeSchema để tự động cập nhật nếu Duy thay đổi số cột (ví dụ từ 13 lên 20 cột)
        df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(bronze_path)
        print(f"✅ Ingested {df.count()} new rows for {table_name}")
    else:
        print(f"☕ No new data for {table_name}")

# Chạy cho 10 bảng
tables = ["products", "orders", "users", "categories", "order_items", "promotions", "order_promotions", "warehouses", "inventory", "marketing_channels"]
for t in tables:
    ingest_table(t)

spark.stop()