from airflow import DAG
from datetime import datetime

from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

list_of_table_names = ["table_1", "table_2", "table_3", "table_4"]

def get_current_date():
    return datetime.now()


# [START howto_operator_python]
def print_process_start(ds, **kwargs):
    """Print the Airflow context and ds variable from the context."""
    return 'start processing tables in database: {mock_database}'


def create_table():
    return 'Created table in db'


default_args = {
    "owner": "akulbasov"
}

dag = DAG(
    start_date=get_current_date(),
    dag_id="processing_with_database",
    default_args=default_args,
    schedule_interval=timedelta(microseconds=1)
)

start_task = PythonOperator(
    task_id='print_process_start',
    provide_context=True,
    python_callable=print_process_start,
    dag=dag
)


create_table = PythonOperator(
    task_id='create_table',
    provide_context=True,
    python_callable=create_table,
    dag=dag
)


bash_command = """
echo "$USER"
"""

echo_user = BashOperator(
    task_id='execute_bash_command',
    bash_command='echo $USER',
    dag=dag
)



dummy = DummyOperator(task_id='dummy_task', dag=dag, depends_on_past=False)


def check_table_exist(**kwargs):
    import random
    if random.sample(range(1, 3), 1) == 1:
        return 'dummy_task'
    else:
        return 'create_table'

check_table_exist = BranchPythonOperator(
    task_id='check_table_exist',
    python_callable=check_table_exist,
    provide_context=True,
    dag=dag
)

insert_new_row = DummyOperator(task_id='insert_new_row', dag=dag, trigger_rule=TriggerRule.ALL_DONE)
query_the_table = DummyOperator(task_id='query_the_table', dag=dag)


start_task >> echo_user >> check_table_exist >> [dummy, create_table] >> insert_new_row >> query_the_table



