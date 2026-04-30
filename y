from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, sum as _sum, hour
import random
from datetime import datetime, timedelta
import os
import shutil

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

spark = SparkSession.builder \
    .appName("UTS BigData Processing") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("🚀 Spark Ready...")

# Hapus output lama
if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Dummy data
locations = ["AreaA", "AreaB", "AreaC"]
start_time = datetime(2026, 1, 1, 7, 0)

sensor_data = []
for i in range(100):
    for loc in locations:
        sensor_data.append((
            start_time + timedelta(minutes=i),
            loc,
            random.randint(10, 100)
        ))

sensor_df = spark.createDataFrame(sensor_data, ["timestamp", "location", "vehicle_count"])

# Processing
traffic_df = sensor_df.groupBy("location") \
    .agg(_sum("vehicle_count").alias("total_vehicle"))

traffic_time_df = sensor_df.groupBy(
    window(col("timestamp"), "10 minutes"),
    col("location")
).agg(_sum("vehicle_count").alias("total_vehicle"))

ml_df = sensor_df.withColumn("hour", hour(col("timestamp")))

# Save parquet
def save_data(df, name):
    path = os.path.join(OUTPUT_DIR, name)
    df.write.mode("overwrite").parquet(path)

save_data(traffic_df, "traffic")
save_data(traffic_time_df, "traffic_time")
save_data(ml_df, "ml_data")

print("✅ SELESAI - DATA TERSIMPAN")

spark.stop()
