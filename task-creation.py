from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
import sql_statements

# Default arguments
default_args = {
    'owner': 'jay',
    'start_date': datetime(2019, 7, 16),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'depends_on_past': False,
    'schedule_interval': '@hourly',
    'max_active_runs': 1
}

# DAG definition
dag = DAG(
    'create_table_task',
    default_args=default_args,
    description='Drop and Create Tables for RedShift',
)

# Task to begin execution
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# Define table creation and dropping tasks for each table
table_definitions = [
    ('artists', 'ARTISTS'),
    ('songplays', 'SONGPLAYS'),
    ('songs', 'SONGS'),
    ('staging_events', 'STAGING_EVENTS'),
    ('staging_songs', 'STAGING_SONGS'),
    ('time', 'TIME'),
    ('users', 'USERS')
]

tasks = []

for table_name, statement_name in table_definitions:
    drop_task = PostgresOperator(
        task_id=f'drop_{table_name}',
        postgres_conn_id="redshift",
        sql=getattr(sql_statements, f'DROP_TABLE_{statement_name}'),
        dag=dag
    )

    create_task = PostgresOperator(
        task_id=f'create_{table_name}',
        postgres_conn_id="redshift",
        sql=getattr(sql_statements, f'CREATE_TABLE_{statement_name}'),
        dag=dag
    )

    start_operator >> drop_task >> create_task
    tasks.extend([drop_task, create_task])

# Task to stop execution
end_operator = DummyOperator(task_id='stop_execution', dag=dag)

# Define task dependencies
for task in tasks:
    task >> end_operator
