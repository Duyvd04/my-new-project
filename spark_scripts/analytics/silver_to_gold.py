# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, count, sum, max, datediff, lit, current_date, when

# spark = SparkSession.builder \
#     .appName("Silver_to_Gold_Analytics") \
#     .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#     .getOrCreate()

# # Đọc dữ liệu từ Silver
# products = spark.read.format("delta").load("/opt/airflow/data/silver/products")
# clickstream = spark.read.format("delta").load("/opt/airflow/data/silver/clickstream")
# # Sử dụng try-except phòng trường hợp bảng orders/users trống
# try:
#     orders = spark.read.format("delta").load("/opt/airflow/data/silver/orders")
# except: orders = None

# # --- BÁO CÁO 1: CONVERSION RATE THEO CATEGORY ---
# print("📊 Calculating Conversion Rate...")
# cv_report = clickstream.join(products, "product_id") \
#     .groupBy("category_id") \
#     .agg(
#         count(when(col("action") == "view", 1)).alias("total_views"),
#         count(when(col("action") == "purchase", 1)).alias("total_purchases")
#     ) \
#     .withColumn("conversion_rate", (col("total_purchases") / col("total_views")) * 100)

# cv_report.write.format("delta").mode("overwrite").save("/opt/airflow/data/gold/conversion_rate")

# # --- BÁO CÁO 2: RFM SEGMENTATION (VIP CUSTOMERS) ---
# if orders:
#     print("💎 Calculating RFM Segmentation...")
#     rfm = orders.groupBy("user_id").agg(
#         datediff(current_date(), max("order_date")).alias("recency"),
#         count("order_id").alias("frequency"),
#         sum("total_amount").alias("monetary")
#     ).withColumn("customer_segment", 
#         when((col("frequency") > 5) & (col("monetary") > 1000), "VIP")
#         .when(col("frequency") > 2, "Loyal")
#         .otherwise("Standard")
#     )
#     rfm.write.format("delta").mode("overwrite").save("/opt/airflow/data/gold/rfm_segmentation")

# print("✅ Gold Layer Analytics Completed!")
# spark.stop()