import streamlit as st
from pyspark.sql import SparkSession
import plotly.express as px
from sklearn.linear_model import LinearRegression
import os

# ==============================
# PATH CONFIG (FIX KE SCRIPTS)
# ==============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "scripts/output")

# ==============================
# PAGE CONFIG
# ==============================
st.set_page_config(page_title="Traffic Dashboard", layout="wide")
st.title("🚦 Smart City AI Traffic Dashboard")

# ==============================
# INIT SPARK
# ==============================
@st.cache_resource
def get_spark():
    return SparkSession.builder.appName("Dashboard_App").getOrCreate()

spark = get_spark()

# ==============================
# LOAD DATA
# ==============================
def load_parquet(folder_name):
    path = os.path.join(OUTPUT_DIR, folder_name)
    if not os.path.exists(path):
        st.error(f"⚠️ Folder data '{folder_name}' tidak ditemukan! Jalankan engine dulu.")
        st.stop()
    return spark.read.parquet(path).toPandas()

pdf = load_parquet("traffic")
pdf_time = load_parquet("traffic_time")
pdf_ml = load_parquet("ml_data")

# ==============================
# SIDEBAR
# ==============================
locations = pdf["location"].unique()
selected_loc = st.sidebar.selectbox("Pilih Lokasi", locations)

filtered_pdf = pdf[pdf["location"] == selected_loc]

# ==============================
# KPI
# ==============================
col1, col2 = st.columns(2)

with col1:
    st.metric("Total Kendaraan", int(pdf["total_vehicle"].sum()))

with col2:
    st.metric(f"Total di {selected_loc}", int(filtered_pdf["total_vehicle"].sum()))

# ==============================
# VISUALISASI
# ==============================
pdf_time["start_time"] = pdf_time["window"].apply(
    lambda x: x[0] if isinstance(x, tuple) else x.start
)

fig = px.line(pdf_time, x="start_time", y="total_vehicle", color="location")
st.plotly_chart(fig, use_container_width=True)

# ==============================
# AI PREDICTION
# ==============================
st.subheader("Prediksi AI")

X = pdf_ml[["hour"]]
y = pdf_ml["vehicle_count"]

model = LinearRegression()
model.fit(X, y)

hour_input = st.slider("Pilih Jam", 0, 23, 12)
pred = model.predict([[hour_input]])

st.success(f"Prediksi kendaraan jam {hour_input}:00 = {int(pred[0])}")
