import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import joblib

# Memuat data yang telah dibersihkan
df = pd.read_csv('data/clean/traffic_smartcity_clean_v1.csv')

# Preprocessing dan Feature Engineering
df['datetime'] = pd.to_datetime(df['datetime'])
df['hour'] = df['datetime'].dt.hour
df['day'] = df['datetime'].dt.dayofweek

# Membuat fitur lag (data sebelumnya)
df['lag1'] = df['traffic'].shift(1)
df = df.dropna()

# Menentukan fitur (X) dan target (y)
X = df[['hour', 'day', 'lag1']]
y = df['traffic']

# Melatih model RandomForest
model = RandomForestRegressor()
model.fit(X, y)

# Menyimpan model ke dalam file .pkl
joblib.dump(model, 'models/traffic_model_v1.pkl')

print("Model berhasil disimpan")
