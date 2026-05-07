
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, sum as _sum, hour
from datetime import datetime, timedelta
import random
import os
import shutil

# =========================================================
# ABSOLUTE PATH
# =========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

# =========================================================
# INIT SPARK
# =========================================================

spark = SparkSession.builder \
    .appName("UTS_Attendance_Analytics") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("🚀 Spark Ready - Memulai Pemrosesan...")

# =========================================================
# BERSIHKAN OUTPUT LAMA
# =========================================================

if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =========================================================
# GENERATE DATA
# =========================================================

buildings = [
    "Fakultas Sains dan Teknologi",
    "Perpustakaan",
    "Auditorium"
]

start_time = datetime(2026, 5, 7, 7, 0)

attendance_data = []

for i in range(100):
    for building in buildings:
        attendance_data.append((
            start_time + timedelta(minutes=i),
            building,
            random.randint(20, 300)
        ))

# =========================================================
# CREATE DATAFRAME
# =========================================================

attendance_df = spark.createDataFrame(
    attendance_data,
    ["timestamp", "building", "attendance_count"]
)

print("✅ DataFrame berhasil dibuat")

# =========================================================
# SPARK TRANSFORMATION
# =========================================================

# 1. Total mahasiswa per gedung
attendance_total = attendance_df.groupBy("building") \
    .agg(
        _sum("attendance_count").alias("total_attendance")
    )

# 2. Tren kehadiran per 20 menit
attendance_time = attendance_df.groupBy(
    window(col("timestamp"), "20 minutes"),
    col("building")
).agg(
    _sum("attendance_count").alias("total_attendance")
)

# 3. Dataset AI berbasis jam
ml_df = attendance_df.withColumn(
    "hour",
    hour(col("timestamp"))
)

print("✅ Spark Transformation selesai")

# =========================================================
# SAVE TO PARQUET
# =========================================================

attendance_total.write.mode("overwrite").parquet(
    os.path.join(OUTPUT_DIR, "attendance_total")
)

attendance_time.write.mode("overwrite").parquet(
    os.path.join(OUTPUT_DIR, "attendance_time")
)

ml_df.write.mode("overwrite").parquet(
    os.path.join(OUTPUT_DIR, "ml_attendance")
)

print("\n✅ SEMUA DATA BERHASIL DISIMPAN KE PARQUET")

# =========================================================
# STOP SPARK
# =========================================================

spark.stop()

print("🛑 Spark Session Closed")