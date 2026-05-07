
import streamlit as st
from pyspark.sql import SparkSession
import plotly.express as px
from sklearn.linear_model import LinearRegression
import pandas as pd
import os

# =========================================================
# PAGE CONFIG
# =========================================================

st.set_page_config(
    page_title="Smart Campus Dashboard",
    layout="wide"
)

st.title("🎓 Smart Campus Attendance Analytics")

# =========================================================
# ABSOLUTE PATH
# =========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

# =========================================================
# INIT SPARK
# =========================================================

@st.cache_resource
def get_spark():
    return SparkSession.builder \
        .appName("Dashboard_App") \
        .getOrCreate()

spark = get_spark()

# =========================================================
# LOAD PARQUET
# =========================================================

try:
    attendance_pdf = spark.read.parquet(
        os.path.join(OUTPUT_DIR, "attendance_total")
    ).toPandas()

    time_pdf = spark.read.parquet(
        os.path.join(OUTPUT_DIR, "attendance_time")
    ).toPandas()

    ml_pdf = spark.read.parquet(
        os.path.join(OUTPUT_DIR, "ml_attendance")
    ).toPandas()

except Exception as e:
    st.error(f"Gagal membaca data parquet: {e}")
    st.stop()

# =========================================================
# SIDEBAR FILTER
# =========================================================

st.sidebar.title("📌 Filter Gedung")

buildings = attendance_pdf["building"].unique()

selected_building = st.sidebar.selectbox(
    "Pilih Gedung",
    buildings
)

# =========================================================
# FILTER DATA
# =========================================================

filtered_attendance = attendance_pdf[
    attendance_pdf["building"] == selected_building
]

# =========================================================
# KPI METRICS
# =========================================================

st.subheader("📊 Key Performance Indicators")

col1, col2 = st.columns(2)

with col1:
    st.metric(
        "Total Mahasiswa Semua Gedung",
        int(attendance_pdf["total_attendance"].sum())
    )

with col2:
    st.metric(
        f"Total Mahasiswa {selected_building}",
        int(filtered_attendance["total_attendance"].sum())
    )

# =========================================================
# VISUALIZATION
# =========================================================

st.subheader("📈 Grafik Tren Kehadiran")

time_pdf["start_time"] = time_pdf["window"].apply(
    lambda x: x[0] if isinstance(x, tuple) else x.start
)

fig = px.line(
    time_pdf,
    x="start_time",
    y="total_attendance",
    color="building",
    title="Tren Kehadiran Mahasiswa"
)

st.plotly_chart(fig, use_container_width=True)

# =========================================================
# MACHINE LEARNING
# =========================================================

st.subheader("🤖 AI Prediction (Linear Regression)")

X = ml_pdf[["hour"]]
y = ml_pdf["attendance_count"]

model = LinearRegression()
model.fit(X, y)

hour_input = st.slider(
    "Pilih Jam Prediksi",
    0,
    23,
    12
)

prediction = model.predict([[hour_input]])

st.success(
    f"Prediksi jumlah mahasiswa pada jam {hour_input}:00 adalah {int(prediction[0])} mahasiswa"
)

# =========================================================
# DATA PREVIEW
# =========================================================

st.subheader("🗂 Preview Dataset")

st.dataframe(ml_pdf.head(20))