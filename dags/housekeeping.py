from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateExternalTableOperator
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectUpdateSensor
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator
)

PROJECT_ID= '<project_id>'
LOCATION= 'europe-west2'

input_bucket='cmpsr-input-bucket'
dataset_name= 'dataset_external'
external_table_name='external'
external_table_bucket='extnl-table-bucket'

dag = DAG('housekeeping', description='object update example', schedule_interval='0 12 * * *', start_date=datetime(2017, 3, 20), catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries = 3, dag=dag)

create_bucket = GCSCreateBucketOperator(task_id="create_input_bucket1", bucket_name=input_bucket, project_id=PROJECT_ID, dag=dag)

create_external_table_bucket = GCSCreateBucketOperator(task_id="create_external_bucket", bucket_name=external_table_bucket, project_id=PROJECT_ID, dag=dag)

create_new_dataset = BigQueryCreateEmptyDatasetOperator(
    dataset_id=dataset_name,
    project_id=PROJECT_ID,
    location=LOCATION,
    task_id='newDatasetCreator', dag=dag)

create_external_table = BigQueryCreateExternalTableOperator(
    task_id="create_external_table",destination_project_dataset_table=f"{dataset_name}.{external_table_name}",
    bucket=external_table_bucket,source_objects=['registers.csv'],
    schema_fields=[
        {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "age", "type": "INTEGER", "mode": "NULLABLE"},
    ],
)

dummy_operator >> create_bucket >> create_external_table_bucket >> create_new_dataset >> create_external_table