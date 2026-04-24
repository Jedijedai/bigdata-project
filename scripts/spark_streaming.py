from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType

# 1. Buat Spark session
spark = SparkSession.builder \
    .appName("KafkaToParquet") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Schema data Kafka
schema = StructType() \
    .add("nama", StringType()) \
    .add("rekening", StringType()) \
    .add("jumlah", IntegerType()) \
    .add("lokasi", StringType())

# 3. Baca dari Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bank_topic") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Convert value ke JSON
df_value = df_kafka.selectExpr("CAST(value AS STRING)")

df_json = df_value.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# 5. Simpan ke Parquet
query = df_json.writeStream \
    .format("parquet") \
    .option("path", "stream_data/realtime_output") \
    .option("checkpointLocation", "stream_data/checkpoint") \
    .outputMode("append") \
    .start()

# 6. Biar jalan terus
query.awaitTermination()
