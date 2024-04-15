# ----------------------------------------------------
# Description: Execute Simple Task - Hello World
# Operator: Bash
# ----------------------------------------------------
# imports
# ----------------------------------------------------
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator
# ----------------------------------------------------
# default args
# ----------------------------------------------------
default_args = {
    'owner' : 'jeanguerrapty',
}
# ----------------------------------------------------
# dag and tasks
# ----------------------------------------------------
with DAG (
    dag_id = 'hello_world',
    description = 'Our First Hello World DAG',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily',
) as dag:

    task = BashOperator( 
        task_id = 'hello_world_task',
        bash_command= 'echo Hello World once again!',
    )
# ----------------------------------------------------
# order of operations
# ----------------------------------------------------
task
# ----------------------------------------------------