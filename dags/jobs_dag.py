from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
import datetime

def get_current_date():
    return datetime.datetime.now()


config_for_all_dag = {
    'dag_id_1': {'start_date': get_current_date()},
    'dag_id_2': {'start_date': get_current_date()},
    'dag_id_3': {'start_date': get_current_date()}
}

for value in config_for_all_dag:
    obj_conf_param = config_for_all_dag[value]
    start_date = obj_conf_param['start_date']
    unique_dag_id = value
    dag = DAG(
        start_date=start_date,
        dag_id=unique_dag_id,
    )
    op = DummyOperator(task_id='dummy', dag=dag)




