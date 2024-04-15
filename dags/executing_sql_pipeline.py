# ----------------------------------------------------
# Description: Airflow Python Pipeline Demo 1
# Operator: Python
# ----------------------------------------------------
# imports
# ----------------------------------------------------
from datetime import date, datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
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
    dag_id = 'execute_sql_pipeline',
    description = 'Execute an SQL Pipeline Example using sqlite operators',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['pipeline', 'sql'],
) as dag:

    create_table = SqliteOperator (
        task_id = 'create_table',
        sql = r"""
            CREATE TABLE IF NOT EXISTS users 
            (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                age INTEGER NOT NULL,
                city VARCHAR(50),
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ;
        """,
        sqlite_conn_id = 'my_sqlite_db_conn',
        dag = dag
    )

    insert_values_1 = SqliteOperator (
        task_id = 'insert_values_1',
        sql = r"""
            INSERT INTO users (name, age, is_active) 
            VALUES 
                ('Julie', 30, false),
                ('Peter', 55, true),
                ('Emily', 37, false),
                ('Katrina', 54, false),
                ('Joseph', 27, true)
            ;
        """,
        sqlite_conn_id = 'my_sqlite_db_conn',
        dag = dag
    )

    insert_values_2 = SqliteOperator (
        task_id = 'insert_values_2',
        sql = r"""
            INSERT INTO users (name, age) VALUES 
                ('Harry', 49),
                ('Nancy', 52),
                ('Elvis', 26),
                ('Mia', 20)
            ;
        """,
        sqlite_conn_id = 'my_sqlite_db_conn',
        dag = dag
    )

    delete_values = SqliteOperator(
        task_id = 'delete_values',
        sql = r"""
            DELETE FROM users WHERE is_active = 0;
        """,
        sqlite_conn_id = 'my_sqlite_db_conn',
        dag = dag,
    )

    update_values = SqliteOperator(
        task_id = 'update_values',
        sql = r"""
            UPDATE users SET city = 'Seattle';
        """,
        sqlite_conn_id = 'my_sqlite_db_conn',
        dag = dag,
    )

    display_results = SqliteOperator(
        task_id = 'display_results',
        sql = r"""SELECT * FROM users;""",
        sqlite_conn_id = 'my_sqlite_db_conn',
        dag = dag,
        do_xcom_push = True
    )
# ----------------------------------------------------
# order of operations
# ----------------------------------------------------
create_table >> [insert_values_1, insert_values_2] >> delete_values >> update_values >> display_results
# ----------------------------------------------------
