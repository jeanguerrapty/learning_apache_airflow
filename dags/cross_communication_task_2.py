# ----------------------------------------------------
# Description: Airflow Cross Communication Task Demo 2
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
def increment_by_one(value):
    print('Value {value}'.format(value = value))
    return value + 1

def multiply_by_hundred(ti):
    value = ti.xcom_pull(task_ids = 'increment_by_one')
    print('Value {value}'.format(value = value))
    return value * 100

def subtract_nine(ti):
    value = ti.xcom_pull(task_ids = 'multiply_by_hundred')
    print('Value {value}'.format(value = value))
    return value - 9

def print_value(ti):
    value = ti.xcom_pull(task_ids = 'subtract_nine')
    print('Value {value}'.format(value = value))
# ----------------------------------------------------
# dag and tasks
# ----------------------------------------------------
with DAG (
    dag_id = 'cross_task_communication_2',
    description = 'Cross task communication with XCom Example 2',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily',
    tags = ['xcom', 'python'],
) as dag:

    increment_by_one = PythonOperator ( 
        task_id = 'increment_by_one',
        python_callable = increment_by_one,
        op_kwargs = {'value': 1}
    )

    multiply_by_hundred = PythonOperator ( 
        task_id = 'multiply_by_hundred',
        python_callable = multiply_by_hundred
    )

    subtract_nine = PythonOperator (
        task_id = 'subtract_nine',
        python_callable = subtract_nine
    )

    print_value = PythonOperator (
        task_id = 'print_value',
        python_callable = print_value
    )
# ----------------------------------------------------
# order of operations
# ----------------------------------------------------
increment_by_one >> multiply_by_hundred >> subtract_nine >> print_value
# ----------------------------------------------------
