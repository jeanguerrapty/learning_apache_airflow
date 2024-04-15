# ----------------------------------------------------
# Description: Airflow Execute Multiple Tasks
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
    dag_id = 'executing_multiple_tasks',
    description = 'DAGs with multiple tasks and dependencies',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
) as dag:

    taskA = BashOperator ( 
        task_id = 'taskA',
        bash_command= 'echo taskA has executed!',
    )

    taskB = BashOperator (
        task_id = 'taskB',
        bash_command= 'echo taskB has executed!',
    )
# ----------------------------------------------------
# order of operations
# ----------------------------------------------------
taskA.set_upstream(taskB)
# ----------------------------------------------------
