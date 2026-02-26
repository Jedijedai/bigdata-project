📌 Deskripsi
Nama : Husni Majedi
NIM :230104040123
Repositori ini berisi implementasi dari Praktikum 2 mengenai pemrosesan data skala besar (Batch Processing) menggunakan Apache Spark. Praktikum ini mensimulasikan pemrosesan data transaksi harian dari sistem e-commerce dengan volume besar yang perlu dibersihkan, dihitung metrik bisnisnya, dan disimpan ke Data Lake agar siap untuk analisis atau Business Intelligence (BI).
+1

🎯 Tujuan Pembelajaran
Melalui modul ini, mahasiswa diharapkan mampu:

Menggunakan Linux environment (WSL) untuk data engineering.

Mengintegrasikan VS Code dengan WSL.

Mengelola virtual environment Python.

Mengimplementasikan batch data ingestion dengan Spark.

Menerapkan data cleaning & transformation.

Mendesain struktur Data Lake (Medallion Architecture: Raw, Clean, Curated).
+1

Menggunakan format penyimpanan Parquet (Columnar Storage).
+1

Menerapkan strategi partisi (partitioning strategy/partition pruning).
+1

Menghasilkan pipeline enterprise-ready.

🛠️ Teknologi & Environment

Programming Language: Python 


Development Environment: Linux Server Environment (WSL Ubuntu) 
+1


Code Editor: VS Code (Remote WSL) 


CLI: Bash 


Distributed Data Processing Engine: PySpark 


Dependencies Tambahan: Java 17 (OpenJDK) 

📂 Struktur Project Enterprise
Sistem penyimpanan disusun menggunakan konsep Medallion Architecture:

Plaintext
bigdata-project/ [cite: 184]
│
├── data/ [cite: 185]
│   ├── raw/                  # Data mentah (format CSV) [cite: 186, 248]
│   ├── clean/                # Data tervalidasi (format Parquet & terpartisi) [cite: 188, 406, 414]
│   └── curated/              # Data siap analitik/agregasi bisnis [cite: 189, 410, 411, 412]
│
├── logs/                     # Folder untuk menyimpan file log [cite: 191, 285]
└── scripts/                  # Folder untuk menyimpan script Python (batch_pipeline_enterprise.py) [cite: 192, 256]
🚀 Cara Menjalankan Pipeline
Persiapan Environment:

Pastikan Anda telah masuk ke dalam OS Ubuntu via WSL dan berada di direktori project ~/bigdata-project.
+1

Letakkan dataset mentah di data/raw/ecommerce_raw.csv (minimal 120.000 baris).

Aktifkan Virtual Environment:
Jalankan perintah berikut di terminal:

Bash
source venv/bin/activate [cite: 439]
Eksekusi Pipeline:
Jalankan script utama Spark:

Bash
python scripts/batch_pipeline_enterprise.py [cite: 441]
📊 Hasil Output
Setelah pipeline selesai dieksekusi tanpa error, direktori berikut akan berisi data hasil pemrosesan:


data/clean/parquet/: Data bersih dalam format Parquet.


data/clean/partitioned_by_category/: Data bersih yang dipartisi berdasarkan kategori.


data/curated/top_products/: Data agregasi 5 produk terlaris.


data/curated/category_revenue/: Data agregasi pendapatan per kategori.

Anda dapat membandingkan ukuran efisiensi file hasil ekstraksi menggunakan perintah du -sh data/clean/parquet/.