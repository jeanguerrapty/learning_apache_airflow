# ----------------------------------------------------
# Description: Airflow Python Operator Demo 2
# Operator: Python
# ----------------------------------------------------
# imports
# ----------------------------------------------------
import time

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator
# ----------------------------------------------------
# default args
# ----------------------------------------------------
default_args = {
    'owner' : 'jeanguerrapty',
}
# ----------------------------------------------------
# python functions
# ----------------------------------------------------
def task_a():
    print('task_a completed!')

def task_b():
    time.sleep(5)
    print('task_b completed!')

def task_c():
    print('task_c completed!')

def task_d():
    print('task_d completed!')
# ----------------------------------------------------
# dag and tasks
# ----------------------------------------------------
with DAG (
    dag_id = 'execute_python_operator_2',
    description = 'Multiple tasks with Python Operators in DAGs',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily',
    tags = ['dependencies', 'python'],
) as dag:

    taskA = PythonOperator ( 
        task_id = 'taskA',
        python_callable = task_a
    )
    taskB = PythonOperator ( 
        task_id = 'taskB',
        python_callable = task_b
    )
    taskC = PythonOperator ( 
        task_id = 'taskC',
        python_callable = task_c
    )
    taskD = PythonOperator ( 
        task_id = 'taskD',
        python_callable = task_d
    )
# ----------------------------------------------------
# order of operations
# ----------------------------------------------------
taskA >> [taskB, taskC]
[taskB, taskC] >> taskD
# ----------------------------------------------------
