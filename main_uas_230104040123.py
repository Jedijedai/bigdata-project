from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import random

# Membuat Spark Session
spark = (
    SparkSession.builder
    .appName("UAS_BIGDATA_ENERGY")
    .getOrCreate()
)

# Generate data dummy
start_time = datetime.now()

data = []

sector_list = [
    "Industrial_A",
    "Industrial_B",
    "Residential_C"
]

for i in range(150):

    timestamp = start_time + timedelta(minutes=i)

    sector = random.choice(sector_list)

    power_usage = random.randint(100, 1000)

    data.append(
        (
            timestamp,
            sector,
            power_usage
        )
    )

# Nama kolom
columns = [
    "timestamp",
    "sector",
    "power_usage"
]

# Buat DataFrame Spark
df = spark.createDataFrame(
    data,
    columns
)

print("DATA BERHASIL DIBUAT")

df.show(10)
from pyspark.sql.functions import sum, avg, hour, window
import os


# ======================
# TOTAL KONSUMSI ENERGI
# ======================

energy_total = (
    df.groupBy(
        "sector"
    )
    .agg(
        sum(
            "power_usage"
        ).alias(
            "total_energy"
        )
    )
)

print("\nTOTAL ENERGI\n")

energy_total.show()



# ======================
# AGREGASI TIAP 10 MENIT
# ======================

energy_time = (
    df.groupBy(
        window(
            "timestamp",
            "10 minutes"
        ),
        "sector"
    )
    .agg(
        avg(
            "power_usage"
        ).alias(
            "avg_power"
        )
    )
)

print("\nAGREGASI 10 MENIT\n")

energy_time.show()



# ======================
# DATASET MACHINE LEARNING
# ======================

ml_energy = (
    df.withColumn(
        "hour",
        hour(
            "timestamp"
        )
    )
    .groupBy(
        "hour"
    )
    .agg(
        avg(
            "power_usage"
        ).alias(
            "power_usage"
        )
    )
    .orderBy(
        "hour"
    )
)

print("\nDATA ML\n")

ml_energy.show()



# ======================
# SAVE PARQUET
# ======================

base = os.path.abspath(
    "./output"
)

energy_total.write.mode(
    "overwrite"
).parquet(
    f"{base}/energy_total"
)

energy_time.write.mode(
    "overwrite"
).parquet(
    f"{base}/energy_time"
)

ml_energy.write.mode(
    "overwrite"
).parquet(
    f"{base}/ml_energy"
)

print("\nPARQUET BERHASIL DIBUAT\n")
pdf = ml_energy.toPandas()

pdf.to_csv(
    "ml_result.csv",
    index=False
)

print("CSV ML BERHASIL")
spark.stop()
