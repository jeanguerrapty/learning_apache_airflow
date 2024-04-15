# ----------------------------------------------------
# Description: Airflow Python Operator Demo
# Operator: Python
# ----------------------------------------------------
# imports
# ----------------------------------------------------
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
def print_function():
    print('The simplest possible Python operator')
# ----------------------------------------------------
# dag and tasks
# ----------------------------------------------------
with DAG (
    dag_id = 'execute_python_operator',
    description = 'Pytnon Operators in DAGs',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily',
    tags = ['simple', 'python'],
) as dag:

    task = PythonOperator ( 
        task_id = 'python_task',
        python_callable = print_function
    )
# ----------------------------------------------------
# order of operations
# ----------------------------------------------------
task
# ----------------------------------------------------
