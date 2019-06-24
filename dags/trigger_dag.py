import os
import re

import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.operators.sensors import BaseSensorOperator
from airflow import DAG
from airflow.operators import *
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.models import BaseOperator
from airflow.models import DagBag

import logging

execution_date = '{{ execution_date.strftime("%d-%m-%Y") }}'
task_name = 'file_sensor_task'
filepath = Variable.get("filepath") or "/Users/akulbasov/Documents/GridU/airflow-course/dags_data"
filepattern = Variable.get("filepattern") or "run"
archivepath = Variable.get("archivepath") or "/Users/akulbasov/Documents/GridU/airflow-course/archive_dags_data/"

def rename_sub_dag(parent_dag_name, child_dag_name, new_name, file_path):
  dag = DAG(
    '%s.%s' % (parent_dag_name, child_dag_name),
    schedule_interval="@once",
    start_date=datetime(2019, 6, 17),
  )
  rename = RenameFileOperator(task_id=parent_dag_name,
    filepath=file_path,
    task_name="rename_file_task",
    new_name=new_name, dag=dag)

  printResult = BashOperator(bash_command="echo all_success", task_id="print_result", dag=dag)
  rename >> printResult
  return dag


class RenameFileOperator(BaseOperator):
    @apply_defaults
    def __init__(self, filepath, task_name, new_name, *args, **kwargs):
        super(RenameFileOperator, self).__init__(*args, **kwargs)
        self.filepath = filepath
        self.task_name = task_name
        self.new_name = new_name

    def execute(self, context):
        file_name = "run"
        os.rename(self.filepath + file_name, self.filepath + file_name + "_" + str(context.get("execution_date")))


class ArchiveFileOperator(BaseOperator):
    @apply_defaults
    def __init__(self, filepath, archivepath, task_name, *args, **kwargs):
        super(ArchiveFileOperator, self).__init__(*args, **kwargs)
        self.filepath = filepath
        self.archivepath = archivepath
        self.task_name = task_name

    def execute(self, context):
        file_name = context['task_instance'].xcom_pull(self.task_name, key='file_name')
        os.rename(self.filepath + file_name, self.archivepath + file_name)


class OmegaFileSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, filepath, filepattern, *args, **kwargs):
        super(OmegaFileSensor, self).__init__(*args, **kwargs)
        self.filepath = filepath
        self.filepattern = filepattern

    def poke(self, context):
        logging.info('I am in poke section')
        full_path = self.filepath
        file_pattern = re.compile(self.filepattern)

        directory_with_file = os.listdir(full_path)

        for file in directory_with_file:
            if not re.match(file_pattern, file):
                logging.info("File with name {} doesn't matched file pattern {}".format(file, file_pattern))
            else:
                context['task_instance'].xcom_push('file_name', file)
                return True
        return False

class OmegaPlugin(AirflowPlugin):
    name = "omega_plugin"
    operators = [OmegaFileSensor, ArchiveFileOperator, RenameFileOperator]


seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())

default_args = {
    'owner': 'akulbasov',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 17),
    'provide_context': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}



dag = DAG(
    dag_id=task_name,
    default_args=default_args,
    schedule_interval="@once")


sensor_task = OmegaFileSensor(
    task_id=task_name,
    filepath=filepath,
    filepattern=filepattern,
    poke_interval=1,
    dag=dag)


def process_file(**context):
    file_to_process = context['task_instance'].xcom_pull(
        key='file_name', task_ids=task_name)
    file = open(filepath + file_to_process, 'w')
    file.write('This is a test\n')
    file.write('of processing the file')
    file.close()


process_task = PythonOperator(
    task_id='process_the_file', python_callable=process_file, dag=dag)

archive_task = ArchiveFileOperator(
    task_id='archive_file',
    filepath=filepath,
    task_name=task_name,
    archivepath=archivepath,
    dag=dag)


sub_dag = SubDagOperator(
    dag=dag,
    subdag=rename_sub_dag(task_name, 'process_result', "finished_" + execution_date, archivepath),
    task_id="process_result"
)


sensor_task >> process_task >> archive_task >> sub_dag