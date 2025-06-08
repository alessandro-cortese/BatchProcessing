import pandas as pd

df = pd.read_parquet("merged.parquet")

print(df.head())

print("Nomi delle colonne:")
print(df.columns.tolist())
