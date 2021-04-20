from airflow import DAG, settings, secrets
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.secrets.aws_secrets_manager import SecretsManagerBackend
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable
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
    description='DevDay First Apache Airflow DAG',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['devday','demo'],
)

# Set Variables used in tasks and stored in AWS Secrets Manager

s3_dlake = Variable.get("s3_dlake", default_var="undefined")
s3_data = Variable.get("s3_data", default_var="undefined")
athena_db = Variable.get("athena_db", default_var="undefined")
athena_output = Variable.get("athena_output", default_var="undefined")

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

disp_variables