import pandas as pd

# Legge il file Parquet
df = pd.read_parquet("merged.parquet")

# Stampa le prime righe (facoltativo)
print(df.head())

# Stampa i nomi delle colonne
print("Nomi delle colonne:")
print(df.columns.tolist())
