from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectUpdateSensor

def print_hello():
    return 'Hello Wolrd'

def obj_upd(Callable):
    return 'object updated'

dag = DAG('object_update', description='object update example', schedule_interval='0 12 * * *', start_date=datetime(2017, 3, 20), catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries = 3, dag=dag)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

object_update = GCSObjectUpdateSensor(task_id='obj_update', bucket='my-test-bu123', object='composer')



dummy_operator >> hello_operator >> object_update