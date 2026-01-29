from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Cập nhật SparkSession đầy đủ cấu hình Delta
spark = SparkSession.builder \
    .appName("Stream_to_Bronze") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Giữ nguyên phần Schema và readStream phía dưới...
schema = StructType([
    StructField("timestamp", TimestampType()), StructField("user_id", IntegerType()),
    StructField("product_id", IntegerType()), StructField("action", StringType()),
    StructField("channel_id", IntegerType()), StructField("session_id", StringType()),
    StructField("ip", StringType()), StructField("device", StringType()),
    StructField("os", StringType()), StructField("browser", StringType()),
    StructField("region", StringType()), StructField("referrer", StringType()),
    StructField("stay_duration", IntegerType()), StructField("is_login", BooleanType()),
    StructField("res", StringType()), StructField("url", StringType()),
    StructField("campaign", StringType()), StructField("lat", FloatType()),
    StructField("long", FloatType()), StructField("error", StringType())
])

df = spark.readStream.option("header", "true").schema(schema).csv("/opt/airflow/data/input/")

# Thêm option mergeSchema để tránh lỗi giống MySQL lúc nãy
query = df.writeStream.format("delta") \
    .outputMode("append") \
    .option("mergeSchema", "true") \
    .option("checkpointLocation", "/opt/airflow/data/bronze/_checkpoints/clickstream") \
    .start("/opt/airflow/data/bronze/clickstream")

query.awaitTermination(60)