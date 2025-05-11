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

### Ingestion
- The data ingestion pipeline is implemented using Apache NiFi, which handles:
    - Fetching hourly datasets for Italy and Sweden
    - Basic preprocessing (e.g., column renaming or filtering, if needed)
    - Loading the cleaned data into HDFS for further processing by Spark


## ⚙️ System Architecture

- **Processing Engine:** Apache Spark (Standalone or via Docker Compose)
- **Storage:**
  - Input: HDFS
  - Output: HDFS + optional external storage (Redis, HBase, etc.)
- **Visualization:** Grafana
- **Optional Cloud Deployment:** Amazon EMR or other cloud platforms


## ⏱️ Performance Evaluation

- Measure and report **execution times** for each query.
- If using both Spark and Spark SQL:
  - Compare performance between both approaches.
  - Include comparison results in the final report.


## 📦 Project Structure
TBD


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


