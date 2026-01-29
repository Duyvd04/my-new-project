from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import sys

# Khởi tạo Spark với cấu hình Delta Catalog (Bắt buộc để chạy SQL/Optimize)
spark = SparkSession.builder \
    .appName("Gold_Analytics_Full_5_Tasks") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def load_delta(table):
    return spark.read.format("delta").load(f"/opt/airflow/data/silver/{table}")

try:
    # Load các bảng chính đã có dữ liệu Silver
    df_click = load_delta("clickstream")
    df_prod = load_delta("products")
    
    # --- CÂU 1: CONVERSION RATE ---
    print("\n[BÀI 1] CONVERSION RATE BY CATEGORY")
    cvr = df_click.join(df_prod, "product_id") \
        .groupBy("category_id") \
        .agg(
            count(when(col("action") == "view", 1)).alias("Views"),
            count(when(col("action") == "purchase", 1)).alias("Purchases")
        ) \
        .withColumn("CVR_%", round((col("Purchases") / col("Views")) * 100, 2))
    cvr.orderBy(desc("CVR_%")).show(5)

    # --- CÂU 2: RFM SEGMENTATION (Dùng dữ liệu Clickstream để mô phỏng nếu Orders chưa nạp) ---
    print("\n[BÀI 2] CUSTOMER SEGMENTATION (RFM)")
    rfm = df_click.filter(col("action") == "purchase") \
        .groupBy("user_id").agg(
            count("session_id").alias("Frequency"),
            sum(lit(100)).alias("Monetary") # Giả lập mỗi đơn 100$
        ).withColumn("Segment", when(col("Frequency") > 3, "VIP").otherwise("Standard"))
    rfm.show(5)

    # --- CÂU 3: PROMOTION IMPACT ---
    print("\n[BÀI 3] PROMOTION IMPACT ANALYSIS")
    promo = df_prod.select("product_name", "base_price", "cost_price") \
        .withColumn("Profit", col("base_price") - col("cost_price")) \
        .withColumn("Is_Promo", when(col("base_price") < 500, "Discounted").otherwise("Normal")) \
        .groupBy("Is_Promo").agg(avg("Profit").alias("Avg_Profit"))
    promo.show()

    # --- CÂU 4: DSI & LOW STOCK ---
    print("\n[BÀI 4] DAYS SALES OF INVENTORY (DSI)")
    dsi = df_prod.withColumn("COGS", col("cost_price") * 5) \
        .withColumn("DSI", round((col("base_price") / col("COGS")) * 365, 0)) \
        .withColumn("Status", when(col("DSI") > 100, "Slow-moving").otherwise("Healthy"))
    dsi.select("product_name", "DSI", "Status").orderBy(desc("DSI")).show(5)

    # --- CÂU 5: MARKETING ATTRIBUTION ---
    print("\n[BÀI 5] FIRST-TOUCH VS LAST-TOUCH")
    w = Window.partitionBy("user_id").orderBy("timestamp")
    attr = df_click.filter(col("channel_id").isNotNull()) \
        .withColumn("First_Touch", first("channel_id").over(w)) \
        .withColumn("Last_Touch", last("channel_id").over(w)) \
        .select("user_id", "First_Touch", "Last_Touch").distinct()
    attr.show(5)

except Exception as e:
    print(f"❌ LỖI: {e}")
    sys.exit(1)

spark.stop()