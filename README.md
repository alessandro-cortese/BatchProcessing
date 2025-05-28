# 🔌 Energy and Emissions Analysis with Apache Spark

## 📘 Project Overview

This project aims to process and analyze historical electricity production and CO₂ emissions data using the **Apache Spark** big data framework. The data is provided by [Electricity Maps](https://www.electricitymaps.com/) and covers hourly statistics for **Italy** and **Sweden** from **January 1st, 2021 to December 31st, 2024**.

The goal is to answer a set of analytical queries using Spark (optionally Spark SQL), visualize the results using a dashboard tool (e.g., **Grafana**), and evaluate the processing performance.  



## 🧾 Dataset Description

- Source: [Electricity Maps Datasets Portal](https://portal.electricitymaps.com/datasets)
- Format: CSV
- Countries: **Italy (IT)** and **Sweden (SE)**
- Time Range: **2021–2024**
- Each row (event) contains:
  - **Carbon Intensity (direct):** gCO₂ eq/kWh
  - **Carbon-Free Energy Percentage (CFE%):** % of electricity from low/no CO₂ sources
  - **Renewable Energy Percentage:** (not used for this project)

Only the **direct carbon intensity** and **carbon-free energy percentage (CFE%)** fields are considered in the analysis.


## 📊 Queries and Visualizations

### **Q1 - Annual Aggregation (IT & SE)**
- For each country and each year:
  - Compute **mean**, **min**, and **max** of:
    - Carbon Intensity (gCO₂/kWh)
    - CFE%
- Generate **2 comparative plots** for Italy vs. Sweden showing annual trends.

### **Q2 - Monthly Aggregation (IT only)**
- Aggregate data by **(year, month)** pairs.
- Compute average carbon intensity and CFE%.
- Produce **top-5 and bottom-5** months based on both metrics.
- Generate **2 line plots** showing monthly evolution of carbon intensity and CFE%.

### **Q3 - Hour-of-Day Aggregation (IT & SE)**
- Aggregate by **hour of the day** (0–23).
- Compute **average** carbon intensity and CFE%.
- Report **min, 25th, 50th, 75th percentile, and max** values of each metric.
- Generate **2 comparative plots** showing average values per hour for each country.

### **Q4 - Clustering Analysis**

- Perform a **clustering analysis** on the average annual **carbon intensity** for **2024** across **30 countries** using the **k-means** algorithm.  
- 15 European countries + 15 extra-European countries  
- Determine optimal `k` using:
  - Elbow method
  - Silhouette score  
- Visualize the clustering results.


## 💾 Data Ingestion & Output

- The ingestion pipeline is built with **Apache NiFi** and follows this process:

1. **Merge all CSVs** into a single file.
2. **Drop unnecessary columns** (e.g., renewable energy share).
3. **Convert to Parquet format** and save as `merged.parquet` on HDFS using `PutParquet`.


## ⚙️ System Architecture

- **Processing Engine:** Apache Spark (via Docker Compose)
- **Data Flow:** Apache NiFi
- **Storage:**
  - Input: HDFS
  - Output: HDFS + Redis
- **Visualization:** Grafana
- **Deployment:** Fully containerized via Docker Compose


## 🐳 Build & Run the Architecture (Docker)
The system is designed to run fully in Docker containers. Scripts to manage the full pipeline:
```bash
  cd docker/
```
```bash
├── setup.sh # Builds and starts entire architecture
├── start_spark_client.sh # Launch Spark client to run queries
├── stop_architecture.sh # Stops and removes all containers
└── stop_spark_client.sh # Stops and removes Spark client container
```

## 📚 References

[1] [Electricity Maps - Carbon Intensity Data](https://www.electricitymaps.com/)  
[2] [Grafana - Open Source Visualization Tool](https://grafana.com/grafana/)  
[3] [Elbow Method (Wikipedia)](https://en.wikipedia.org/wiki/Elbow_method_(clustering))  
[4] [Silhouette Score (Wikipedia)](https://en.wikipedia.org/wiki/Silhouette_(clustering))


## 👥 Group Info

- Group Members:  
  - Alessandro Cortese
  - Chiara Iurato
  - Luca Martorelli


