# =====================================
# VISUALIZATION LAYER
# =====================================

from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt

# ============================
# INIT SPARK
# ============================
spark = SparkSession.builder \
    .appName("VisualizationLayer") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ============================
# LOAD DATA
# ============================
df = spark.read.parquet("data/clean/parquet/")

# ============================
# REVENUE PER CATEGORY
# ============================
category_df = df.groupBy("category") \
    .sum("total_amount") \
    .toPandas()

# Sort data
category_df = category_df.sort_values("sum(total_amount)", ascending=False)

# ============================
# CREATE BAR CHART
# ============================
plt.figure(figsize=(8, 5))
plt.bar(category_df["category"], category_df["sum(total_amount)"])

plt.xticks(rotation=45)
plt.title("Revenue per Category")
plt.ylabel("Total Revenue")

plt.tight_layout()

# ============================
# SAVE VISUALIZATION
# ============================
plt.savefig("reports/category_revenue.png")

print("Visualization saved to reports/category_revenue.png")

# ============================
# STOP SPARK
# ============================
spark.stop()