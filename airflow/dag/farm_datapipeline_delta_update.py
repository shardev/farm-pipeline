from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import os

token = os.getenv("DBT_CLOUD_API_TOKEN")
dbt_account_id = os.getenv("DBT_ACCOUNT_ID")


# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='farm_datapipeline_delta_update',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    upload_to_bigquery = GCSToBigQueryOperator(
        task_id='load_new_wialondump_to_bigquery',
        bucket='farm-source', 
        source_objects=['wialon/wialon_dump_18082024.csv'], 
        destination_project_dataset_table='farm-datapipeline.dbt_asarovic.stg_wialon__telemetry',
        source_format='CSV',  
        write_disposition='WRITE_APPEND',  
        skip_leading_rows=1,  
        autodetect=False,  
        schema_fields=[
            {"name": "unit_id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "dateTime", "type": "STRING", "mode": "NULLABLE"},
            {"name": "driverId", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "gpsLongitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "gpsLatitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "speed", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "altitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "course", "type": "FLOAT", "mode": "NULLABLE"}
        ]
    )

    # Function to trigger the dbt Cloud model
    def trigger_dbt_job(job_id):
        dbt_cloud_token = token
        account_id = dbt_account_id
        headers = {
            "Authorization": f"Token {dbt_cloud_token}",
            "Content-Type": "application/json"
        }
        response = requests.post(
            f"https://aw621.us1.dbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/",
            headers=headers
        )
        if response.status_code != 200:
            raise Exception(f"Failed to trigger dbt job {job_id}: {response.text}")

    # Trigger stg wialon in dbt Cloud
    trigger_dbt_wialon_stg_task = PythonOperator(
        task_id='trigger_dbt_wialon_stg_task',
        python_callable=trigger_dbt_job,
        op_args=['70403104041026'],  # Wialon staging
    )

    trigger_dbt_telematics_stg_task = PythonOperator(
        task_id='trigger_dbt_telematics_stg_task',
        python_callable=trigger_dbt_job,
        op_args=['70403104041027'],  # Telematics staging
    )

    trigger_dbt_fendt_stg_task = PythonOperator(
        task_id='trigger_dbt_fendt_stg_task',
        python_callable=trigger_dbt_job,
        op_args=['70403104041028'],  # Fendt staging
    )

    trigger_dbt_fendt_gps_stg_task = PythonOperator(
        task_id='trigger_dbt_fendt_gps_stg_task',
        python_callable=trigger_dbt_job,
        op_args=['70403104041029'],  # Fendt gps staging
    )

    trigger_dbt_fendt_join_int_task = PythonOperator(
        task_id='trigger_dbt_fendt_join_int_task',
        python_callable=trigger_dbt_job,
        op_args=['70403104041030'],  # Fendt int join 
    )
    
    trigger_dbt_common_structure_int_task = PythonOperator(
        task_id='trigger_dbt_common_structure_int_task',
        python_callable=trigger_dbt_job,
        op_args=['70403104041031'],  # Common structure int
    )

    trigger_dbt_vehicle_runs_int_task = PythonOperator(
        task_id='trigger_dbt_vehicle_runs_int_task',
        python_callable=trigger_dbt_job,
        op_args=['70403104041032'],  # Vehicle runs int
    )
    
    trigger_dbt_coverage_map_mart_task = PythonOperator(
        task_id='trigger_dbt_coverage_map_mart_task',
        python_callable=trigger_dbt_job,
        op_args=['70403104041033'],  # Coverage map mart
    )

    # Task dependencies
    upload_to_bigquery >> [
        trigger_dbt_wialon_stg_task,
        trigger_dbt_telematics_stg_task,
        trigger_dbt_fendt_stg_task,
        trigger_dbt_fendt_gps_stg_task
    ] >> trigger_dbt_fendt_join_int_task >> \
    trigger_dbt_common_structure_int_task >> \
    trigger_dbt_vehicle_runs_int_task >> \
    trigger_dbt_coverage_map_mart_task

