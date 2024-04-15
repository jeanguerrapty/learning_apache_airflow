# ----------------------------------------------------
# Description: Airflow Execute Multiple Tasks 3
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
    dag_id = 'executing_multiple_tasks_3',
    description = 'DAGs with multiple tasks and dependencies using bash_scripts from a folder',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = timedelta(days = 1),
    tags = ['upstream', 'downstream'],
    template_searchpath = 'dags/bash_scripts/'
) as dag:

    taskA = BashOperator ( 
        task_id = 'taskA',
        bash_command = 'taskA.sh'
    )

    taskB = BashOperator (
        task_id = 'taskB',
        bash_command = 'taskB.sh'
    )

    taskC = BashOperator (
        task_id = 'taskC',
        bash_command = 'taskC.sh'
    )

    taskD = BashOperator ( 
        task_id = 'taskD',
        bash_command = 'taskD.sh'
    )

    taskE = BashOperator ( 
        task_id = 'taskE',
        bash_command = 'taskE.sh'
    )

    taskF = BashOperator ( 
        task_id = 'taskF',
        bash_command = 'taskF.sh'
    )

    taskG = BashOperator ( 
        task_id = 'taskG',
        bash_command = 'taskG.sh'
    )
# ----------------------------------------------------
# order of operations
# >> means set downstream dependency
# << means set upstream dependency
# ----------------------------------------------------
taskA >> taskB >> taskE
taskA >> taskC >> taskF
taskA >> taskD >> taskG