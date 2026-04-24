import streamlit as st
import pandas as pd
import os
import time

st.set_page_config(page_title="Fraud Dashboard", layout="wide")
st.title("🚨 Real-Time Fraud Detection Dashboard")

# Folder tempat Spark menyimpan data
path = "stream_data/realtime_output/"

# 1. Cek apakah folder output sudah diciptakan oleh Spark
if not os.path.exists(path) or len(os.listdir(path)) == 0:
    st.info("Menunggu data dari Spark Streaming... (Pastikan Spark sudah berjalan)")
    time.sleep(2)
    st.rerun()
else:
    try:
        # 2. Coba baca data
        df = pd.read_parquet(path)

        if len(df) > 0:
            # Layout Metrik
            col1, col2 = st.columns(2)
            col1.metric("Total Transaksi", len(df))
            col2.metric("Total Fraud", len(df[df["status"]=="FRAUD"]))

            # Layout Tabel & Chart
            st.subheader("10 Transaksi Terakhir")
            st.dataframe(df.tail(10), use_container_width=True)

            st.subheader("Statistik Status Transaksi")
            st.bar_chart(df["status"].value_counts())
            
            # Auto-refresh setiap 5 detik
            time.sleep(5)
            st.rerun()
        else:
            st.warning("File ditemukan, tapi belum ada data transaksi masuk.")
            time.sleep(2)
            st.rerun()

    except Exception as e:
        # Jika kena error '0 bytes' atau file sedang terkunci, jangan crash, tapi tunggu sebentar
        st.warning("Sedang sinkronisasi data dengan Spark... Mohon tunggu.")
        time.sleep(2)
        st.rerun()