""" Scheduling of the data transformation to load data from source into DWH"""

import os
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.sensors import SqlSensor

#Please note : these are not created by me , but idea was to reuse this
from dwh.helpers.connection_helpers import send_failure_alerts
from dwh.plugins.operations_plugin import StartOperationOperator, CompleteOperationOperator

#different schedulers for each bank will help to keep main python seperate
from dwh.transformations.staging.bank1.bank1_staging import bank1_staging
from dwh.transformations.staging.bank2.bank2_staging import bank2_staging
from dwh.transformations.staging.bank3.bank3_staging import bank3_staging
from dwh.transformations.staging.bank4.bank4_staging import bank4_staging
from dwh.transformations.staging.bank5.bank5_staging import bank5_staging

DEFAULT_ARGS = {
    'owner': 'transformations',
    'depends_on_past': False,
    'start_date': datetime(2022, 9, 13),
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'on_failure_callback': send_failure_alerts,
    'postgres_conn_id': REDSHIFT_AIRFLOW_CONNECTION,
}

dag = DAG(
    dag_id='dwh_transformations',
    default_args=DEFAULT_ARGS,
    schedule_interval='0 0 * * *' 
    max_active_runs=1,
    catchup=False
)

#classify the each stage to transform data and store sqls/business logic in each stage
dwh_sql_subdirectory = [
    'functions', 'staging', 'dimensions', 'facts', 'marts'
]

dag_path = os.path.split(dag.fileloc)[0]
dag.template_searchpath = [os.path.join(dag_path, n) for n in dwh_sql_subdirectory]

#Asumption: data comes from 5 banks as of now , so create subdirectory for each bank to apply business logic
#helps in modularity if new data is added , does not affect the rest
subtasks_bank1 = bank1_staging(dag)
subtasks_bank2 = bank2_staging(dag)
subtasks_bank3 = bank3_staging(dag)
subtasks_bank4 = bank4_staging(dag)
subtasks_bank5 = bank5_staging(dag)

#create a sql to check if new data has arrived and then be able to trigger downstream
check_data_exists = SqlSensor(
    conn_id=REDSHIFT_AIRFLOW_CONNECTION,
    sql=""" select if required data for each bank has arrived
    """,
    task_id="check_data",
    poke_interval= x ,
    timeout=y,
    dag=dag,
    retries=0 #overrides normal retry of 3 since this is checking data availability and waiting for x hours
)

# ------------------------------------stage logging------------------------------------------

#start of ETL
start_operations_log = StartOperationOperator(
    dag=dag,
    task_id='start_operations_log',
    connection_id=REDSHIFT_AIRFLOW_CONNECTION,
    operation_name='dwh_transformations'
)

# functions
start_operations_log_functions = StartOperationOperator(
    dag=dag,
    task_id='start_operations_log_functions',
    connection_id=REDSHIFT_AIRFLOW_CONNECTION,
    operation_name='dwh_transformations#functions',
    operation_seed='{{ execution_date }}'
)

complete_operations_log_functions = CompleteOperationOperator(
    dag=dag,
    task_id='complete_operations_log_functions',
    connection_id=REDSHIFT_AIRFLOW_CONNECTION,
    operation_name='dwh_transformations#functions'
)

# staging
start_operations_log_staging = StartOperationOperator(
    dag=dag,
    task_id='start_operations_log_staging',
    connection_id=REDSHIFT_AIRFLOW_CONNECTION,
    operation_name='dwh_transformations#staging',
    operation_seed='{{ execution_date }}'
)

complete_operations_log_staging = CompleteOperationOperator(
    dag=dag,
    task_id='complete_operations_log_staging',
    connection_id=REDSHIFT_AIRFLOW_CONNECTION,
    operation_name='dwh_transformations#staging'
)

# dimensions

start_operations_log_dimensions = StartOperationOperator(
    dag=dag,
    task_id='start_operations_log_dimensions',
    connection_id=REDSHIFT_AIRFLOW_CONNECTION,
    operation_name='dwh_transformations#dimensions',
    operation_seed='{{ execution_date }}'
)

complete_operations_log_dimensions = CompleteOperationOperator(
    dag=dag,
    task_id='complete_operations_log_dimensions',
    connection_id=REDSHIFT_AIRFLOW_CONNECTION,
    operation_name='dwh_transformations#dimensions'
)

# facts

start_operations_log_facts = StartOperationOperator(
    dag=dag,
    task_id='start_operations_log_facts',
    connection_id=REDSHIFT_AIRFLOW_CONNECTION,
    operation_name='dwh_transformations#facts',
    operation_seed='{{ execution_date }}'
)

complete_operations_log_facts = CompleteOperationOperator(
    dag=dag,
    task_id='complete_operations_log_facts',
    connection_id=REDSHIFT_AIRFLOW_CONNECTION,
    operation_name='dwh_transformations#facts'
)

# marts

start_operations_log_marts = StartOperationOperator(
    dag=dag,
    task_id='start_operations_log_marts',
    connection_id=REDSHIFT_AIRFLOW_CONNECTION,
    operation_name='dwh_transformations#marts',
    operation_seed='{{ execution_date }}'
)


complete_operations_log_marts = CompleteOperationOperator(
    dag=dag,
    task_id='complete_operations_log_marts',
    connection_id=REDSHIFT_AIRFLOW_CONNECTION,
    operation_name='dwh_transformations#marts'
)

#complete ETL
complete_operations_log = CompleteOperationOperator(
    dag=dag,
    task_id='complete_operations_log',
    connection_id=REDSHIFT_AIRFLOW_CONNECTION,
    operation_name='dwh_transformations'
)

# --------------------------------------------stage scheduling-----------------------------------------
# functions
start_creating_tasks_functions = DummyOperator(
        task_id="start_creating_tasks_functions",
        dag=dag
    )

SQL_TASKS = []
sql_dir = os.path.join(dag_path, "functions")
# find all the nnn_....sql files in the same directory as scheduler
scan = os.scandir(sql_dir)
for entry in scan:
    if entry.is_file(): #and re.match(r'^[a-zA-Z]+*\.sql', entry.name):
        task_id = entry.name
        SQL_TASKS.append(PostgresOperator(
        task_id=task_id,
        sql=task_id,
        postgres_conn_id=REDSHIFT_AIRFLOW_CONNECTION,
        dag=dag))

finish_creating_tasks_functions = DummyOperator(
        task_id="finish_creating_tasks_functions",
        dag=dag
    )

start_operations_log >> check_data >> start_operations_log_functions
start_operations_log_functions >> start_creating_tasks_functions >> SQL_TASKS
SQL_TASKS >> finish_creating_tasks_functions >> complete_operations_log_functions

# staging
task_start_staging = DummyOperator(
    task_id="task_start_staging",
    dag=dag
)

task_finish_staging = PostgresOperator(
    task_id="task_finish_staging",
    sql='analyze_staging_tables.sql',
    dag=dag
)

complete_operations_log_functions >> start_operations_log_staging >> task_start_staging
task_start_staging.set_downstream([
    subtasks_bank1['start_task'],
    subtasks_bank2['start_task'],
    subtasks_bank3['start_task'],
    subtasks_bank4['start_task'],
    subtasks_bank5['start_task']
])

task_finish_staging.set_upstream([
    subtasks_bank1['end_task'],
    subtasks_bank2['end_task'],
    subtasks_bank3['end_task'],
    subtasks_bank4['end_task'],
    subtasks_bank5['end_task'],
])

task_finish_staging >> complete_operations_log_staging

#dimensions
start_creating_tasks_dimensions = DummyOperator(
    task_id="start_creating_tasks_dimensions",
    dag=dag
)

SQL_TASKS = []
sql_dir = os.path.join(dag_path, "dimensions")

# find all the nnn_....sql files in the same directory as scheduler
scan = os.scandir(sql_dir)
for entry in scan:
    if entry.is_file(): #and re.match(r'^[a-zA-Z]+*\.sql', entry.name):
        task_id = entry.name
        SQL_TASKS.append(PostgresOperator(
            task_id=task_id,
            sql=task_id,
            postgres_conn_id=REDSHIFT_AIRFLOW_CONNECTION,
            dag=dag))

finish_creating_tasks_dimensions = DummyOperator(
    task_id="finish_creating_tasks_dimensions",
    dag=dag
)

complete_operations_log_staging >> start_operations_log_dimensions >> start_creating_tasks_dimensions
start_creating_tasks_dimensions >> SQL_TASKS
SQL_TASKS >> finish_creating_tasks_dimensions >> complete_operations_log_dimensions

# facts
start_creating_tasks_facts = DummyOperator(
    task_id="start_creating_tasks_facts",
    dag=dag
)

SQL_TASKS = []
sql_dir = os.path.join(dag_path, "facts")

# find all the nnn_....sql files in the same directory as scheduler
scan = os.scandir(sql_dir)
for entry in scan:
    if entry.is_file(): # and re.match(r'^[a-zA-Z]+*\.sql', entry.name):
            task_id = entry.name
            SQL_TASKS.append(PostgresOperator(
                task_id=task_id,
                sql=task_id,
                postgres_conn_id=REDSHIFT_AIRFLOW_CONNECTION,
                dag=dag))

finish_creating_tasks_facts = DummyOperator(
    task_id="finish_creating_tasks_facts",
    dag=dag
)
complete_operations_log_dimensions >> start_operations_log_facts >> start_creating_tasks_facts
start_creating_tasks_facts >> SQL_TASKS >> finish_creating_tasks_facts >> complete_operations_log_facts

# marts

start_creating_tasks_marts = DummyOperator(
    task_id="start_creating_tasks_marts",
    dag=dag
)

SQL_TASKS = []
sql_dir = os.path.join(dag_path, "marts")

# find all the nnn_....sql files in the same directory as scheduler
scan = os.scandir(sql_dir)
for entry in scan:
    if entry.is_file(): # and re.match(r'^[a-zA-Z]+*\.sql', entry.name):
            task_id = entry.name
            SQL_TASKS.append(PostgresOperator(
                task_id=task_id,
                sql=task_id,
                postgres_conn_id=REDSHIFT_AIRFLOW_CONNECTION,
                dag=dag))

finish_creating_tasks_marts = DummyOperator(
    task_id="finish_creating_tasks_marts",
    dag=dag
)

complete_operations_log_facts >> start_operations_log_marts >> start_creating_tasks_marts
start_creating_tasks_marts >> SQL_TASKS >> finish_creating_tasks_marts >> complete_operations_log_marts

complete_operations_log_marts >> complete_operations_log
