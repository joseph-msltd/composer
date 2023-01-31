from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

def obj_upd(Callable):
    return 'object updated'

dag = DAG("composer_sample_trigger_response_dag", description='object update example', schedule_interval='0 12 * * *', start_date=datetime(2017, 3, 20), catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries = 3, dag=dag)


dummy_operator