# ----------------------------------------------------
# Description: Airflow Execute Multiple Tasks 2
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
    dag_id = 'executing_multiple_tasks_2',
    description = 'DAGs with multiple tasks and dependencies',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = timedelta(days = 1),
    tags = ['upstream', 'downstream']
) as dag:

    taskA = BashOperator ( 
        task_id = 'taskA',
        bash_command = '''
            echo taskA has started!

            for i in {1..10}
            do
                echo taskA printing $i
            done

            echo taskA has finished!=
        '''
    )

    taskB = BashOperator (
        task_id = 'taskB',
        bash_command = '''
            echo taskB has started!
            sleep 4
            echo taskB has finished!
        '''
    )

    taskC = BashOperator (
        task_id = 'taskC',
        bash_command = '''
            echo taskC has started!
            sleep 15
            echo taskC has finished!
        '''
    )

    taskD = BashOperator ( 
        task_id = 'taskD',
        bash_command = 'echo taskD has finished!'
    )
# ----------------------------------------------------
# order of operations
# >> means set downstream dependency
# << means set upstream dependency
# ----------------------------------------------------
# taskB and taskC depend on taskA
taskA >> [ taskB, taskC ]
# task D depends on taskC and taskC
taskD << [ taskB, taskC ]
