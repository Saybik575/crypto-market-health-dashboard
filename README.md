# Crypto Market Health Pipeline & Dashboard

## Overview

This project implements a complete, end-to-end data engineering pipeline to monitor the top assets in the cryptocurrency market. It demonstrates skills in cloud-native ETL (Extract, Transform, Load), data warehousing, and business intelligence visualization.

The pipeline is designed to:
* Extract real-time crypto market data from the **CoinGecko API**.
* Process and orchestrate the data flow using **Apache Airflow**.
* Utilize **Google Cloud Storage (GCS)** as a reliable staging layer, avoiding local file system dependencies.
* Load structured data into **Google BigQuery** for scalable analysis.
* Visualize key insights in a professional **Looker Studio Dashboard** for market health assessment.

## Tech Stack

| Category | Tool / Service | Purpose |
| :--- | :--- | :--- |
| **Orchestration** | `Apache Airflow` | Scheduling, monitoring, and managing the ETL workflow. |
| **Cloud** | `Google Cloud Storage (GCS)` | Raw data landing zone and transformed data staging. |
| **Data Warehouse** | `Google BigQuery` | Fast, serverless data storage and query engine. |
| **Language** | `Python` | Fetching data (requests) and transforming data (Pandas). |
| **Source** | `CoinGecko API` | Real-time market data extraction. |
| **Visualization** | `Looker Studio` | Interactive business intelligence dashboarding. |

## 🛠 Airflow DAG: ETL Workflow

The `crypto_exchange_pipeline.py` DAG executes the daily extraction and loading process, ensuring data is ready for analysis.

**DAG code here → `crypto_exchange_pipeline.py`**

The pipeline steps are:

* **`fetch_data_and_upload_to_gcs`**:
    * Fetches the latest Top 10 crypto market data from the CoinGecko API.
    * Uploads the raw JSON data directly to GCS. (Uses `GCSHook` to avoid local storage).
* **`transform_data_from_gcs_and_upload_to_gcs`**:
    * Reads the raw JSON from GCS into memory.
    * Transforms the data into a flattened, structured CSV format (e.g., calculates timestamps, cleans fields).
    * Uploads the transformed CSV back to a different GCS path.
* **`create_bigquery_dataset` / `create_bigquery_table`**:
    * Ensures the target BigQuery environment is set up.
* **`load_data_to_bigquery`**:
    * Loads the final transformed CSV data directly from GCS into the BigQuery table (`tbl_crypto`).
    * Uses XComs to dynamically reference the exact GCS file path for reliability.

## Dashboard Preview: Market Health Dashboard

The Looker Studio dashboard provides an executive overview focusing on **Market Health and Dominance**. It demonstrates the ability to translate technical data into actionable business insights.

(Charts taken from the PDF report ● see Crypto_Market_Health_Report.pdf)

Key Dashboard Visualizations include:

* **Total Market Cap / Total Volume (KPIs):** High-level health and liquidity indicators.
* **Bitcoin Dominance %:** Calculated field showing Bitcoin's share of the total market, a key risk metric.
* **Market Cap Share % (Donut Chart):** Visual breakdown of asset concentration.
* **Volume vs. Market Cap (Combo Chart):** Compares trading velocity against market size (liquidity analysis).
* **Market Health Heatmap (Table):** Detailed asset ranking table featuring conditional formatting for quick identification of price and volume outliers.
