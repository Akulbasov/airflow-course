from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
import datetime

from airflow.operators.python_operator import PythonOperator


def get_current_date():
    return datetime.datetime.now()


config_for_all_dag = {
    'dag_id_1': {
        'start_date': get_current_date(),
        'table_name': "table_name_1"
    },
    'dag_id_2': {
        'start_date': get_current_date(),
        'table_name': "table_name_2"
    },
    'dag_id_3': {
        'start_date': get_current_date(),
        'table_name': "table_name_3"
    }
}

# [START howto_operator_python]
def print_context(ds, **kwargs):
    """Print the Airflow context and ds variable from the context."""
    print(kwargs)
    print(ds)
    return 'start processing tables in database: {mock_database}'


config_dag_1 = config_for_all_dag["dag_id_1"]
config_dag_2 = config_for_all_dag["dag_id_3"]
config_dag_3 = config_for_all_dag["dag_id_3"]


default_args = {
    "owner": "akulbasov"
}
dag_1 = DAG(
    start_date=config_dag_1["start_date"],
    dag_id="dag_id_1",
    default_args=default_args
)

task_1 = PythonOperator(
    op_kwargs="dag_id_1",
    task_id='print_in_the_log',
    provide_context=True,
    python_callable=print_context,
    dag=dag_1
)

# dag_2 = DAG(
#     start_date=config_dag_2["start_date"],
#     dag_id="dag_id_2",
# )

task_2 = DummyOperator(task_id='insert_new_row', dag=dag_1)


# dag_3 = DAG(
#     start_date=config_dag_3["start_date"],
#     dag_id="dag_id_3",
# )

task_3 = DummyOperator(task_id='query_the_table', dag=dag_1)


task_1 >> task_2 >> task_3




