import streamlit as st
import pandas as pd
import plotly.express as px
from sklearn.linear_model import LinearRegression
import os


st.set_page_config(
    page_title="Smart Energy Analytics",
    layout="wide"
)

st.title(
    "Smart Energy Consumption Analytics"
)


# ======================
# LOAD DATA ML
# ======================

base = os.path.abspath(
    "./ml_result.csv"
)

df = pd.read_csv(
    base
)


# ======================
# FILTER
# ======================

sector = st.sidebar.selectbox(

    "Pilih Sector",

    [
        "Industrial_A",
        "Industrial_B",
        "Residential_C"
    ]

)


# ======================
# KPI
# ======================

total = df[
    "power_usage"
].sum()


st.metric(
    "Total Konsumsi",
    f"{total:.0f} kWh"
)



# ======================
# GRAFIK
# ======================

fig = px.line(

    df,

    x="hour",

    y="power_usage",

    title="Trend Konsumsi Energi",

    markers=True

)

st.plotly_chart(
    fig,
    use_container_width=True
)



# ======================
# PREDIKSI
# ======================

X = df[
    ["hour"]
]

y = df[
    "power_usage"
]

model = LinearRegression()

model.fit(
    X,
    y
)


jam = st.slider(

    "Pilih Jam Prediksi",

    0,

    23,

    12

)


hasil = model.predict(
    [[jam]]
)[0]


st.subheader(
    "Prediksi Konsumsi"
)

st.success(
    f"{hasil:.2f} kWh"
)
