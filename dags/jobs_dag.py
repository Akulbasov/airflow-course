from airflow import DAG
from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

import logging



# [START howto_operator_python]
def print_process_start(ds, **kwargs):
    """Print the Airflow context and ds variable from the context."""
    return 'start processing tables in database: {mock_database}'

hook = PostgresHook()

def create_table(**context):
    table_name = context["task_instance"].xcom_pull(key="username", task_ids="push_username_to_xcom")
    cur = hook.get_conn().cursor()
    sql = """
    CREATE TABLE {}(
    id TEXT NOT NULL,
    username VARCHAR (50) NOT NULL,
    timestamp TEXT NOT NULL);
    """.format(table_name)
    logging.info(sql)
    cur.close()
    return 'Created table in db'


default_args = {
    'owner': 'akulbasov',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 17),
    'provide_context': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
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


def push_username_to_xcom(**context):
    import os
    username = os.getlogin()
    context['task_instance'].xcom_push('username', username)


push_username_to_xcom = PythonOperator(
    task_id='push_username_to_xcom',
    python_callable=push_username_to_xcom,
    dag=dag, provide_context=True
)


dummy = DummyOperator(task_id='dummy_task', dag=dag, depends_on_past=False)


def check_table_exist(**context):
    from sqlite3 import OperationalError
    get_current_user_name = context["task_instance"].xcom_pull(key="username", task_ids="push_username_to_xcom")
    logging.info("I took data from db with key username and value {}".format(get_current_user_name))
    """ callable function to get schema name and after that check if table exist """

    # check table exist
    sql = """
    SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='{}');
    """.format(get_current_user_name)

    logging.info(
        str(hook.get_records("""
        SELECT 1 FROM information_schema.tables WHERE table_name='{}';
        """.format(get_current_user_name)))
    )
    logging.info(bool(hook.get_first(sql=sql)[0]))
    if bool(hook.get_first(sql=sql)[0]):
        return "dummy_task"
    else:
        return "create_table"


# will success
table_name_success = "dag"
get_table_name = BranchPythonOperator(task_id="check_table_success", python_callable=check_table_exist)


check_table_exist = BranchPythonOperator(
    task_id='check_table_exist',
    python_callable=check_table_exist,
    provide_context=True,
    dag=dag, retries=1
)

def insert_new_row(**context):
    import uuid
    table_name = context["task_instance"].xcom_pull(key="username", task_ids="push_username_to_xcom")
    id = uuid.uuid4()
    time = str(datetime.utcnow())
    sql = """
    INSERT INTO {}(id, username, timestamp)
    VALUES('{}','{}','{}');
    """.format(table_name, id, table_name, time)
    hook.get_conn().cursor().execute(query=sql)
    return "Pasted new data in tableName {} with id {} and username {} and timestamp {}"\
        .format(table_name, id, table_name, time)

from airflow.utils.trigger_rule import TriggerRule

insert_new_row = PythonOperator( task_id='insert_new_row',
    provide_context=True,
    python_callable=insert_new_row,
    dag=dag, trigger_rule=TriggerRule.ONE_SUCCESS, retries=1
)

def query_the_table(**context):
    table_name = context["task_instance"].xcom_pull(key="username", task_ids="push_username_to_xcom")
    return str(hook.get_records("""
            SELECT count(*) FROM '{}';
            """.format(table_name)))

query_the_table = PythonOperator(
    provide_context=True,
    python_callable=query_the_table,
    task_id='query_the_table', dag=dag
)


start_task >> push_username_to_xcom >> check_table_exist >> [dummy, create_table] >> insert_new_row >> query_the_table



