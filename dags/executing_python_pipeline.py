# ----------------------------------------------------
# Description: Airflow Python Pipeline Demo 1
# Operator: Python
# ----------------------------------------------------
# imports
# ----------------------------------------------------
import pandas as pd

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
def read_csv_file():
    src_file_path = 'datasets/insurance.csv'
    df = pd.read_csv(src_file_path)
    print(df)
    return df.to_json()

def remove_null_values(ti):
    json_data = ti.xcom_pull(task_ids = 'read_csv_file')
    df = pd.read_json(json_data)
    df = df.dropna()
    print(df)
    return df.to_json()

def group_by_smoker(ti):
    json_data = ti.xcom_pull(task_ids = 'remove_null_values')
    df = pd.read_json(json_data)
    smoker_df = df.groupby('smoker').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()
    smoker_df.to_csv('output/grouped_by_smoker.csv', index = False)

def group_by_region(ti):
    json_data = ti.xcom_pull(task_ids = 'remove_null_values')
    df = pd.read_json(json_data)
    region_df = df.groupby('region').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()
    region_df.to_csv('output/grouped_by_region.csv', index = False)
    

# ----------------------------------------------------
# dag and tasks
# ----------------------------------------------------
with DAG (
    dag_id = 'python_pipeline',
    description = 'Running a Python Pipeline',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['python', 'transfrom', 'pipeline'],
) as dag:

    read_csv_file = PythonOperator ( 
        task_id = 'read_csv_file',
        python_callable = read_csv_file,
    )

    remove_null_values = PythonOperator (
        task_id = 'remove_null_values',
        python_callable = remove_null_values,
    )

    group_by_smoker = PythonOperator (
        task_id = 'group_by_smoker',
        python_callable = group_by_smoker
    )

    group_by_region = PythonOperator (
        task_id = 'group_by_region',
        python_callable = group_by_region
    )
# ----------------------------------------------------
# order of operations
# ----------------------------------------------------
read_csv_file >> remove_null_values >> [group_by_smoker, group_by_region]
# ----------------------------------------------------
