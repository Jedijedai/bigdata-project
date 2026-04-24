from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("FraudDetection").getOrCreate()

# Membaca stream dari Kafka
df_kafka = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bank_topic") \
    .load()

# Definisi Schema JSON
schema = StructType([
    StructField("nama", StringType()),
    StructField("rekening", StringType()),
    StructField("jumlah", IntegerType()),
    StructField("lokasi", StringType())
])

# Parsing data JSON dari Kafka
df = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Masking (Menyembunyikan nomor rekening kecuali 2 angka terakhir)
df = df.withColumn("rekening_masked", 
    concat(lit("*****"), col("rekening").substr(-2, 2)))

# Fraud Detection Logic
df = df.withColumn("status",
    when(col("jumlah") > 50000000, "FRAUD")
    .when(col("lokasi") == "Luar Negeri", "FRAUD")
    .otherwise("NORMAL"))

# Encryption (Mengubah jumlah menjadi Base64 string)
df = df.withColumn("jumlah_encrypted",
    base64(col("jumlah").cast("string")))

# Menulis hasil ke format Parquet
query = df.writeStream \
    .format("parquet") \
    .option("path", "stream_data/realtime_output/") \
    .option("checkpointLocation", "data/checkpoints/") \
    .start()

query.awaitTermination()