import pandas as pd
import matplotlib.pyplot as plt
import glob
import os

# Percorso dei CSV e dei grafici
csv_folder = "../../performance"
chart_folder = "../charts"
os.makedirs(chart_folder, exist_ok=True)

# Leggi tutti i file CSV
csv_files = glob.glob(os.path.join(csv_folder, "*.csv"))

df_list = []

for file_path in csv_files:
    try:
        df = pd.read_csv(file_path)

        # Estrai il formato (es: CSV, AVRO, PARQUET) dal nome file
        filename = os.path.basename(file_path)
        file_format = filename.split("_")[-1].split(".")[0].strip().lower()

        df["file_format"] = file_format
        df_list.append(df)

    except Exception as e:
        print(f"Errore nel leggere {file_path}: {e}")

if not df_list:
    raise ValueError("Nessun CSV valido trovato nella cartella specificata.")

# Unisci tutti i dati
df_all = pd.concat(df_list, ignore_index=True)

# Normalizza nomi
df_all["query_type"] = df_all["query_type"].str.strip().str.lower()
df_all["query_name"] = df_all["query_name"].str.strip().str.lower()

# Crea grafici per ogni combinazione query_type + formato file
grouped = df_all.groupby(["query_type", "file_format"])

for (qtype, fmt), subset in grouped:
    plt.figure()

    for query_name in subset["query_name"].unique():
        query_data = (
            subset[subset["query_name"] == query_name]
            .groupby("num_executors")["execution_time_sec"]
            .mean()
            .reset_index()
            .sort_values("num_executors")
        )
        plt.plot(
            query_data["num_executors"],
            query_data["execution_time_sec"],
            marker='o',
            label=query_name
        )

    plt.title(f"Execution Time vs Executors\nQuery Type: {qtype.capitalize()}, Format: {fmt.upper()}")
    plt.xlabel("Number of Executors")
    plt.ylabel("Execution Time (sec)")
    plt.grid(True)
    plt.legend(title="Query Name")
    plt.tight_layout()

    # Salva grafico
    chart_filename = f"execution_time_{qtype}_{fmt}.png"
    plt.savefig(os.path.join(chart_folder, chart_filename))
    plt.close()

# Grafici EXTRA per dataframe, senza query4
df_filtered = df_all[(df_all["query_type"] == "dataframe") & (df_all["query_name"] != "query4")]

for fmt in df_filtered["file_format"].unique():
    subset = df_filtered[df_filtered["file_format"] == fmt]
    plt.figure()

    for query_name in subset["query_name"].unique():
        query_data = (
            subset[subset["query_name"] == query_name]
            .groupby("num_executors")["execution_time_sec"]
            .mean()
            .reset_index()
            .sort_values("num_executors")
        )
        plt.plot(
            query_data["num_executors"],
            query_data["execution_time_sec"],
            marker='o',
            label=query_name
        )

    plt.title(f"Execution Time vs Executors (No query4)\nQuery Type: Dataframe, Format: {fmt.upper()}")
    plt.xlabel("Number of Executors")
    plt.ylabel("Execution Time (sec)")
    plt.grid(True)
    plt.legend(title="Query Name")
    plt.tight_layout()

    # Salva grafico
    chart_filename = f"execution_time_dataframe_{fmt}_noquery4.png"
    plt.savefig(os.path.join(chart_folder, chart_filename))
    plt.close()

print("Tutti i grafici (standard + no-query4) salvati in:", os.path.abspath(chart_folder))
