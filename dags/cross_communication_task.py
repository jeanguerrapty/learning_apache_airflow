# ----------------------------------------------------
# Description: Airflow Cross Communication Task Demo
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
def increment_by_one(counter):
    print('Count {counter}'.format(counter = counter))
    return counter + 1

def multiply_by_hundred(counter):
    print('Count {counter}'.format(counter = counter))
    return counter * 100
# ----------------------------------------------------
# dag and tasks
# ----------------------------------------------------
with DAG (
    dag_id = 'cross_task_communication',
    description = 'Cross task communication with XCom',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily',
    tags = ['xcom', 'python'],
) as dag:

    taskA = PythonOperator ( 
        task_id = 'increment_by_one',
        python_callable = increment_by_one,
        op_kwargs = {'counter': 100}
    )

    taskB = PythonOperator ( 
        task_id = 'multiply_by_hundred',
        python_callable = multiply_by_hundred,
        op_kwargs = {'counter': 9}
    )
# ----------------------------------------------------
# order of operations
# ----------------------------------------------------
taskA >> taskB
# ----------------------------------------------------
