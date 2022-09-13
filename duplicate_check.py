"""Following code is used to check the duplicates in a table,
   Input : List of schemas to check 
   Assumption : the primary key is defined in the table structure
   Output : Schema and table name with count of duplicate records in table
"""

import logging
import os
from datetime import datetime

import pandas as pd
import requests
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from tabulate import tabulate

from dwh.helpers import other_helpers
from dwh.helpers.connection_helpers import send_failure_to_slack, get_influx_url
from dwh.helpers.connection_helpers import send_success_to_slack
from dwh.plugins.operations_plugin import TodayDWHTransformationsOperationCompletedSensor

REDSHIFT_ADMIN_SCHEMA_CONN = 'redshift-primary-check'
REDSHIFT_AIRFLOW_CONN = 'redshift-01'
INFLUX_CONN_ID = 'influx'
SLACK_CONN_ID = 'slack-duplicates'

# Input : schemas to check duplicates for
SCHEMAS_TO_CHECK = (
    'datawarehouse', 'staging'
)

# Code for sending output message
def send_slack_msg(dataframe, **context):
    context['slack_alert_icon_url'] = Variable.get("dq_bot_icon")
    if dataframe.empty:
        text = "*No Duplicate records present for today ({environment})*".format(environment=other_helpers.get_work_env())
        context['slack_alert_text'] = text
        context['slack_alert_attachments'] = {"color": "008000"}
        send_success_to_slack(context)
    else:
        final_df_sorted = dataframe.sort_values(by='tablename', ascending=False)
        df = tabulate(final_df_sorted, headers='keys', tablefmt='psql')
        text = """ *Duplicate records present for today ({environment})*:
                ```{df}``` """.format(
            environment=other_helpers.get_work_env(),
            df=df)
        context['slack_alert_text'] = text
        context['slack_alert_attachments'] = {"color": "F08080"}
        send_failure_to_slack(context)

# Logic for idenitfying primary check and triggering the duplicate check
def trigger_duplicate_check(**context):
    """
    This method first queries the admin view v_generate_tbl_ddl to get the table
    details along with primary key. Then it iterates over each table and counts
    the number of duplicates for each and updates the info in grafana for the
    dashboard to track the number of duplicates.
    :return: None
    """
    influxdb_url = get_influx_url(INFLUX_CONN_ID,
                                  Variable.get("grafana_env_redshift_db"))

    pg_hook_admin = PostgresHook(postgres_conn_id=REDSHIFT_ADMIN_SCHEMA_CONN)

    df = pg_hook_admin.get_pandas_df("""SELECT 
            schemaname, 
            tablename, 
            tableowner,
            replace(substr(ddl, POSITION('(' IN ddl)+1 ),')','') AS primary_key 
        FROM
            admin.v_generate_tbl_ddl 
        WHERE 
            upper(ddl) LIKE '%PRIMARY KEY%'
            AND schemaname IN {schemas}
            -- the keuzenaamondersteunend_translations has constant dups. We ignore it unless resolved
            AND tablename <> 'keuzenaamondersteunend_translations'
        ORDER BY 
            3,1,2;""".format(schemas=SCHEMAS_TO_CHECK))
    df.columns = ['schemaname', 'tablename', 'tableowner', 'primary_key']

    pg_hook_airflow = PostgresHook(postgres_conn_id=REDSHIFT_AIRFLOW_CONN)

    final_df = pd.DataFrame()

    for index, row in df.iterrows():
        table_name = row['tablename']
        table_owner = row['tableowner']
        schema_name = row['schemaname']
        primary_key = row['primary_key']
        logging.info('start time for table %s', table_name)
        results_final = pg_hook_airflow.get_pandas_df(
            "select '{table_name}' as tablename, \
            '{table_owner}' as tableowner, \
            '{schema_name}' as schemaname, \
            count(*) from ( \
                select {primary_key} , \
                 count(*) from {schema_name}.{table_name} \
                 group by  {primary_key} having count(*) > 1 ) ;".format(
                table_name=table_name,
                table_owner=table_owner,
                primary_key=primary_key,
                schema_name=schema_name)
        )
        logging.info('end time for table %s', row['tablename'])
        count = results_final['count'].get(0)
        if count != 0:
            tag_table_name = '{}.{}'.format(schema_name,
                                     results_final['tablename'].get(0))
            final_df = final_df.append(results_final)
            data_string = 'primarycheck,tablename={tablename},owner={owner}\
                value={value}'.format(
                                tablename=tag_table_name,
                                owner=results_final['tableowner'].get(0),
                                value=count)
            requests.post(influxdb_url, data=data_string)
    send_slack_msg(final_df, **context)
    return

#default arguments 
args = {
    'owner': 'reshma.wadhwa',
    'depends_on_past': False,
    'email': ['reshma.wadhwa@takeaway.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'on_failure_callback': send_failure_to_slack,
    'retries': 3
}

dag = DAG(dag_id='dwh_primary_check_redshift',
          default_args=args,
          schedule_interval='0 4 * * *',
          start_date=datetime(2019, 1, 21),
          max_active_runs=1,
          user_defined_macros={
              'SLACK_CONN_ID': SLACK_CONN_ID
          }
          )

# will be used as the start of the DAG, waiting for DWH to complete
wait_for_main_dwh = TodayDWHTransformationsOperationCompletedSensor(
    dag=dag,
    task_id="wait_for_main_dwh",
    connection_id=REDSHIFT_AIRFLOW_CONN,
    timeout=60*60*5  # 5 hours
)

trigger_duplicate_check_task = PythonOperator(
    task_id='trigger-duplicate-check',
    python_callable=trigger_duplicate_check,
    provide_context=True,
    dag=dag
)

wait_for_main_dwh >> trigger_duplicate_check_task
