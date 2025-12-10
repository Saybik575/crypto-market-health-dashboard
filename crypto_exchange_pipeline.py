# crypto_exchange_pipeline.py
import json
import requests
from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# Import Google Storage Hook for GCS file operations
from airflow.providers.google.cloud.hooks.gcs import GCSHook 

# Replaced deprecated operator imports with supported ones:
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)

# === Config ===
GCP_PROJECT = "learn-aiflow"
GCS_BUCKET = "crypto-exchange-pipeline-satvik"
GCS_RAW_DATA_PATH = "raw_data/crypto_raw_data"
GCS_TRANSFORMED_DATA_PATH = "transformed_data/crypto_transformed_data"
BIGQUERY_DATASET = "crypto_db"
BIGQUERY_TABLE = "tbl_crypto"
GCP_CONN_ID = 'google_cloud_default' # Define connection ID once

BQ_SCHEMA = [
    {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'symbol', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'name', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'current_price', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'market_cap', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'total_volume', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'last_updated', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
]

# === Tasks' Python callables ===
def _fetch_data_and_upload_to_gcs(ti, gcs_bucket, gcs_raw_data_path, gcp_conn_id):
    """Fetches data and directly uploads the raw JSON to GCS."""
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        'x_cg_demo_api_key': 'COINGECKO_API_KEY',
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 10,
        'page': 1,
        'sparkline': 'false'
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    # Generate the GCS object name (key) for the raw data
    execution_date_nodash = ti.execution_date.strftime('%Y%m%dT%H%M%S')
    raw_gcs_key = f"{gcs_raw_data_path}_{execution_date_nodash}.json"
    
    # Initialize GCS Hook
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)

    # Convert JSON to string and upload directly to GCS
    gcs_hook.upload(
        bucket_name=gcs_bucket,
        object_name=raw_gcs_key,
        data=json.dumps(data),
        mime_type='application/json'
    )
    
    # Push the GCS path for the raw data to XCom for the next task to use
    ti.xcom_push(key='raw_gcs_path', value=raw_gcs_key)


def _transform_data_from_gcs_and_upload_to_gcs(ti, gcs_bucket, gcs_transformed_data_path, gcp_conn_id):
    """Reads raw JSON from GCS, transforms it, and uploads CSV directly to GCS."""
    
    # Pull the GCS path for the raw data from XCom
    raw_gcs_key = ti.xcom_pull(task_ids='fetch_data_and_upload_to_gcs', key='raw_gcs_path')
    
    if not raw_gcs_key:
        raise ValueError("Could not retrieve raw GCS path from XCom.")
        
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    
    # 1. Read Raw JSON from GCS
    raw_data_content = gcs_hook.download(
        bucket_name=gcs_bucket,
        object_name=raw_gcs_key
    )
    data = json.loads(raw_data_content.decode('utf-8'))


    # 2. Transform Data
    transformed_data = []
    current_utc_timestamp = datetime.utcnow().isoformat() + "Z"

    for item in data:
        transformed_data.append({
            'id': item.get('id'),
            'symbol': item.get('symbol'),
            'name': item.get('name'),
            'current_price': item.get('current_price'),
            'market_cap': item.get('market_cap'),
            'total_volume': item.get('total_volume'),
            'last_updated': item.get('last_updated'),
            'timestamp': current_utc_timestamp # Use RFC3339-like timestamp
        })

    df = pd.DataFrame(transformed_data)
    
    # 3. Upload Transformed CSV to GCS
    
    # Generate the GCS object name (key) for the transformed data
    execution_date_nodash = ti.execution_date.strftime('%Y%m%dT%H%M%S')
    transformed_gcs_key = f"{gcs_transformed_data_path}_{execution_date_nodash}.csv"

    # Convert DataFrame to CSV string (buffer)
    csv_buffer = df.to_csv(index=False)
    
    gcs_hook.upload(
        bucket_name=gcs_bucket,
        object_name=transformed_gcs_key,
        data=csv_buffer.encode('utf-8'),
        mime_type='text/csv'
    )
    
    # Push the GCS path for the transformed data to XCom for the BigQuery load task
    ti.xcom_push(key='transformed_gcs_path', value=transformed_gcs_key)


# === DAG ===
default_args = {
    'owner': 'satvik',
    'depends_on_past': False,
}

dag = DAG(
    dag_id='crypto_exchange_pipeline_gcs_only',
    default_args=default_args,
    description='Fetch data, transform, and load to BigQuery using GCS as staging, avoiding local storage.',
    schedule=timedelta(days=1),
    start_date=datetime(2025, 12, 8),
    catchup=False,
    max_active_runs=1,
)

# === Operators / Tasks ===

# 1. Create Bucket
create_bucket_task = GCSCreateBucketOperator(
    task_id='create_bucket',
    bucket_name=GCS_BUCKET,
    storage_class='MULTI_REGIONAL',
    location='US',
    gcp_conn_id=GCP_CONN_ID,
    dag=dag,
)

# 2. Fetch Data and Upload to GCS
# This replaces fetch_data_task and upload_raw_data_to_gcs_task
fetch_and_upload_task = PythonOperator(
    task_id='fetch_data_and_upload_to_gcs',
    python_callable=_fetch_data_and_upload_to_gcs,
    op_kwargs={
        'gcs_bucket': GCS_BUCKET, 
        'gcs_raw_data_path': GCS_RAW_DATA_PATH,
        'gcp_conn_id': GCP_CONN_ID,
    },
    dag=dag,
)

# 3. Transform Data from GCS and Upload back to GCS
# This replaces transform_data_task and upload_transformed_data_to_gcs_task
transform_and_upload_task = PythonOperator(
    task_id='transform_data_from_gcs_and_upload_to_gcs',
    python_callable=_transform_data_from_gcs_and_upload_to_gcs,
    op_kwargs={
        'gcs_bucket': GCS_BUCKET, 
        'gcs_transformed_data_path': GCS_TRANSFORMED_DATA_PATH,
        'gcp_conn_id': GCP_CONN_ID,
    },
    dag=dag,
)

# 4. BigQuery: create dataset if not exists
create_bq_dataset_task = BigQueryCreateEmptyDatasetOperator(
    task_id='create_bigquery_dataset',
    dataset_id=BIGQUERY_DATASET,
    project_id=GCP_PROJECT,
    gcp_conn_id=GCP_CONN_ID,
    dag=dag,
)

# 5. BigQuery: create table using DDL (idempotent)
create_bq_table_task = BigQueryInsertJobOperator(
    task_id='create_bigquery_table',
    configuration={
        "query": {
            "query": f"""
                CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}` (
                    id STRING,
                    symbol STRING,
                    name STRING,
                    current_price FLOAT64,
                    market_cap INT64,
                    total_volume INT64,
                    last_updated TIMESTAMP,
                    timestamp TIMESTAMP
                );
            """,
            "useLegacySql": False,
        }
    },
    gcp_conn_id=GCP_CONN_ID,
    dag=dag,
)

# 6. Load CSV from GCS to BigQuery
load_data_to_bq_task = GCSToBigQueryOperator(
    task_id='load_data_to_bigquery',
    bucket=GCS_BUCKET,
    # Dynamically pull the GCS key from XCom
    source_objects=[
        "{{ task_instance.xcom_pull(task_ids='transform_data_from_gcs_and_upload_to_gcs', key='transformed_gcs_path') }}"
    ],
    destination_project_dataset_table=f"{GCP_PROJECT}:{BIGQUERY_DATASET}.{BIGQUERY_TABLE}",
    source_format='CSV',
    schema_fields=BQ_SCHEMA,
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    gcp_conn_id=GCP_CONN_ID,
    dag=dag,
)

# === Task ordering ===
create_bucket_task >> fetch_and_upload_task >> transform_and_upload_task
transform_and_upload_task >> create_bq_dataset_task >> create_bq_table_task >> load_data_to_bq_task