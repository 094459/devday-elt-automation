from airflow import DAG, settings, secrets
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import HttpSensor, S3KeySensor
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.contrib.secrets.aws_secrets_manager import SecretsManagerBackend
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago
import os
import sys
import boto3
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

DAG_ID = os.path.basename(__file__).replace('.py', '')

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='DevDay Athena export SciFi DAG',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['devday','demo'],
)

# Set Variables used in tasks and stored in AWS Secrets Manager

s3_dlake = Variable.get("s3_dlake", default_var="undefined")
s3_data = Variable.get("s3_data", default_var="undefined")
athena_db = Variable.get("athena_db", default_var="undefined")
athena_output = Variable.get("athena_output", default_var="undefined")

# Define the SQL we want to run to create the tables for our new venture
# This will export the table scifi to a gzip JSON file in the external_location

export_athena_scifi_table_query = """
CREATE TABLE {database}.export
    WITH (
          format = 'JSON',
          external_location = 's3://{datalake}/export/'
    ) AS SELECT * FROM {database}.scifi
""".format(database=athena_db, datalake=s3_dlake)

export_athena_scifi_table_query2 = """
CREATE TABLE {database}.export
    WITH (
          format = 'TEXTFILE',
          field_delimiter = ',',
          external_location = 's3://{datalake}/export/'
    ) AS SELECT * FROM {database}.scifi
""".format(database=athena_db, datalake=s3_dlake)


def py_display_variables(**kwargs):
    print("Data Lake location " + s3_dlake + " ")
    print("Data within Lake " + s3_data + " ")
    print("New Athena DB " + athena_db + " ")
    print("Output CSV file we create " + athena_output + " ")

disp_variables = PythonOperator (
	task_id='print_variables',
	provide_context=True,
	python_callable=py_display_variables,
	dag=dag
	)

# Supporting functions that are called by DAGs
def check_export_table(**kwargs):
    ath = boto3.client('athena')
    try:
        response = ath.get_table_metadata(
            CatalogName='AwsDataCatalog',
            DatabaseName= athena_db,
            TableName='export'
        )
        print("Table already exists - drop table")
        return "drop_athena_export_table"
    except:
        print("No Table Found")
        return "check_athena_export_table_done" 

def drop_athena_export_table(**kwargs):
    print("Dropping export Table")
    ath = boto3.client('athena')
    ath.start_query_execution(
        QueryString='DROP TABLE IF EXISTS export',
        QueryExecutionContext = {
                'Database' : athena_db
            },
        ResultConfiguration={'OutputLocation': 's3://{s3_dlake}/queries/'.format(s3_dlake=s3_dlake)},
        WorkGroup="devday-demo"
        )
def clear_export_folder(**kwargs):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_dlake)
    try:
        print("Deleting files in exports folder")
        for obj in bucket.objects.filter(Prefix='export/'):
            print(bucket.name,obj.key)
            s3.Object(bucket.name,obj.key).delete()
    except:
        print("Could not delete files for some reason - maybe not there?")

def export_scifi_tofile(**kwargs):
    print("Exporting the file for other users to use")
    ath = boto3.client('athena')
    s3 = boto3.resource('s3')
    try:
        get_query = ath.start_query_execution(
            QueryString ='select "$path" from export LIMIT 1',
            QueryExecutionContext = {
                'Database' : athena_db
            },
            ResultConfiguration = {
                'OutputLocation': 's3://'+s3_dlake+'/queries/'},
            WorkGroup="devday-demo"
        )
        get_query_exe_id = ath.get_query_execution(
            QueryExecutionId = get_query['QueryExecutionId']
            )

        time.sleep(5)
        result = ath.get_query_results(
            QueryExecutionId = get_query['QueryExecutionId']
        )
        
        result_data = result['ResultSet']['Rows'][1:]
        file = result_data[0]['Data'][0]['VarCharValue']
        print("Resource to be copied: ", file)
        renamed_file = file.split(s3_dlake +'/export/',1)[1]
        print("File to be copied: ", renamed_file)
        print("Using the following copy statement",s3_dlake + '/export/' + renamed_file )

        copy_source = {'Bucket':s3_dlake, 'Key': 'export/' + renamed_file}
        bucket = s3.Bucket(s3_dlake)
        obj = bucket.Object('movielens/scifi/scifi-movies.csv.gz')
        #obj = bucket.Object('movielens/scifi/scifi-movies.json.gz')
        obj.copy(copy_source)

    except Exception as e:
        print("Could not find path")
        print(e)

# Dag logic goes here

check_athena_export_table = BranchPythonOperator(
    task_id='check_athena_export_table',
    provide_context=True,
    python_callable=check_export_table,
    trigger_rule="all_done",
    dag=dag,
)

check_athena_export_table_done = DummyOperator(
    task_id="check_athena_export_table_done",
    dag=dag,
)
check_athena_export_table_pass = DummyOperator(
    task_id="check_athena_export_table_pass",
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)

drop_athena_export_table = PythonOperator (
    task_id='drop_athena_export_table',
	provide_context=True,
	python_callable=drop_athena_export_table,
	dag=dag
    )

clear_export_folder = PythonOperator (
    task_id='clear_export_folder',
	provide_context=True,
	python_callable=clear_export_folder,
	dag=dag
    )

export_athena_scifi_table = AWSAthenaOperator(
    task_id="export_athena_scifi_table",
    #query=export_athena_scifi_table_query,
    query=export_athena_scifi_table_query2, 
    workgroup = "devday-demo", 
    database=athena_db,
    sleep_time = 60,
    output_location='s3://'+s3_dlake+"/"+athena_output+'export_athena_scifi_table'
    )


export_scifi_tofile = PythonOperator (
    task_id='export_scifi_tofile',
	provide_context=True,
	python_callable=export_scifi_tofile,
	dag=dag
    )

check_athena_export_table.set_upstream(disp_variables)
drop_athena_export_table.set_upstream(check_athena_export_table)
check_athena_export_table_done.set_upstream(check_athena_export_table)
check_athena_export_table_pass.set_upstream(drop_athena_export_table)
check_athena_export_table_pass.set_upstream(check_athena_export_table_done)
export_athena_scifi_table.set_upstream(clear_export_folder)
clear_export_folder.set_upstream(check_athena_export_table_pass)
export_scifi_tofile.set_upstream(export_athena_scifi_table)