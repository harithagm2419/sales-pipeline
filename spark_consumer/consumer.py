from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("SalesConsumer") \
    .getOrCreate()

schema = StructType([
    StructField("event_id", StringType()),
    StructField("product", StringType()),
    StructField("quantity", IntegerType()),
    StructField("price", DoubleType()),
    StructField("timestamp", StringType()),
    StructField("region", StringType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales_events") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

query = parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()