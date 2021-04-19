from airflow import DAG, settings, secrets
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.contrib.secrets.aws_secrets_manager import SecretsManagerBackend
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
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
    description='DevDay EMR create SciFi DAG',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['devday','demo'],
)

# Set Variables used in tasks and stored in AWS Secrets Manager

s3_dlake = Variable.get("s3_dlake", default_var="undefined")
s3_data = Variable.get("s3_data", default_var="undefined")
emr_db = Variable.get("emr_db", default_var="undefined")
emr_output = Variable.get("emr_output", default_var="undefined")

def py_display_variables(**kwargs):
    print("Data Lake location " + s3_dlake + " ")
    print("Data within Lake " + s3_data + " ")
    print("EMR DB " + emr_db + " ")
    print("EMR Output location " + emr_output + " ")

disp_variables = PythonOperator (
	task_id='print_variables',
	provide_context=True,
	python_callable=py_display_variables,
	dag=dag
	)

## Amazon EMR info

CREATE_DATABASE = [
    {
        'Name': 'Create Comedy Database',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'hive-script',
                '--run-hive-script',
                '--args',
                '-f',
                's3://{{ var.value.s3_dlake }}/scripts/create-film.hql'
            ]
        }
    }
]

CREATE_TABLES = [
    {
        'Name': 'Create Tables',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'hive-script',
                '--run-hive-script',
                '--args',
                '-f',
                's3://{{ var.value.s3_dlake }}/scripts/create-film-tables.hql'
            ]
        }
    }
]

PRESTO_QUERY = [
    {
        'Name': 'Run Presto to Comedy Tables',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'bash',
                '-c',
                'aws s3 cp s3://devday-demo-airflow-sql/scripts/run-presto-query.sh . ; chmod +x run-presto-query.sh ; ./run-presto-query.sh ; echo removing script; rm run-presto-query.sh'
            ]
        }
    }
]

CREATE_COMEDY_TABLES = [
    {
        'Name': 'Create Comedy Tables',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'hive-script',
                '--run-hive-script',
                '--args',
                '-f',
                's3://{{ var.value.s3_dlake }}/scripts/create-comedy-film-table.hql'
            ]
        }
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'devday-demo-cluster-airflow',
    'ReleaseLabel': 'emr-5.32.0',
    'LogUri': 's3n://{{ var.value.s3_dlake }}/logs',
    'Applications': [
        {
            'Name': 'Spark',
        },
        {
            'Name': 'Pig',
        },
        {
            'Name': 'Hive',
        },
        {
            'Name': 'Presto',
        }
    ],
    'Instances': {
        'InstanceFleets': [
            {
                'Name': 'MASTER',
                'InstanceFleetType': 'MASTER',
                'TargetSpotCapacity': 1,
                'InstanceTypeConfigs': [
                    {
                        'InstanceType': 'm5.xlarge',
                    },
                ]
            },
            {
                'Name': 'CORE',
                'InstanceFleetType': 'CORE',
                'TargetSpotCapacity': 2,
                'InstanceTypeConfigs': [
                    {
                        'InstanceType': 'r5.xlarge',
                    },
                ],
            },
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
        'Ec2KeyName': 'ec2-rocket',
    },
    'Configurations': [
        {
            'Classification': 'hive-site',
            'Properties': {'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'}
        },
        {
            'Classification': 'presto-connector-hive',
            'Properties': {'hive.metastore.glue.datacatalog.enabled': 'true'}
        },
        {
            'Classification': 'spark-hive-site',
            'Properties': {'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'}
        }
    ],
    'VisibleToAllUsers': True,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'EbsRootVolumeSize': 32,
    'StepConcurrencyLevel': 1,
    'Tags': [
        {
            'Key': 'Environment',
            'Value': 'Development'
        },
        {
            'Key': 'Name',
            'Value': 'Airflow EMR Demo Project'
        },
        {
            'Key': 'Owner',
            'Value': 'Data Analytics Team'
        }
    ]
}

### To Do - create these dynamically and then upload to scripts folder

HIVE_CREATE_FILM = """
create database films; 
"""

HIVE_CREATE_COMEDY_TABLE = """
CREATE EXTERNAL TABLE films.comedy (
    title   STRING,
    year    INT,
    rating  INT

) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION 's3://devday-demo-airflow-sql/movielens/comedy/';
"""

PRESTO_SCRIPT_GEN_COMEDY_EXPFILE = """
#!/bin/bash
aws s3 cp s3://devday-demo-airflow-sql/scripts/create-comedy.sql .
presto-cli --catalog hive -f create-comedy.sql --output-format TSV > comedy-films.tsv
aws s3 cp comedy-films.tsv s3://devday-demo-airflow-sql/movielens/comedy/
"""
PRESTO_SQL_GEN_COMEDY_CSV = """
WITH comedydata AS (
SELECT REPLACE ( m.title , '"' , '' ) as title, r.rating
FROM films.movies m
INNER JOIN (SELECT rating, movieId FROM films.ratings) r on m.movieId = r.movieId WHERE REGEXP_LIKE (genres, 'Comedy')
  )
SELECT title, replace(substr(trim(title),-5),')','') as year, AVG(rating) as avrating from comedydata GROUP BY title ORDER BY year DESC,  title ASC
"""


## Supporting Python callables

def check_emr_database(**kwargs):
    ath = boto3.client('athena')
    # We use athena as we are using the Glue catalog for the hive metastore
    try:
        response = ath.get_database(
            CatalogName='AwsDataCatalog',
            DatabaseName=emr_db
        )
        print("Database already exists - skip creation")
        return "skip_emr_database_creation"
    except:
        print("No EMR Database Found")
        return "create_emr_database_step"

def check_emr_table(**kwargs):
    ath = boto3.client('athena')
    try:
        response = ath.get_table_metadata(
            CatalogName='AwsDataCatalog',
            DatabaseName= emr_db,
            TableName='movies'
        )
        # we check for movies, but we will be creating comedy
        print("Table exists - create comedy")
        return "run_presto_script_step"
    except:
        print("No Table Found - skip, as there are bigger problems!")
        return "check_emr_movie_table_skip" 

## Dags

check_emr_database = BranchPythonOperator(
    task_id='check_emr_database',
    provide_context=True,
    python_callable=check_emr_database,
    retries=1,
    dag=dag,
)

skip_emr_database_creation = DummyOperator(
    task_id="skip_emr_database_creation",
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag,
)

create_emr_database_cluster = EmrCreateJobFlowOperator(
    task_id='create_emr_database_cluster', 
    job_flow_overrides=JOB_FLOW_OVERRIDES,
	dag=dag
    )
create_emr_database_step = EmrAddStepsOperator(
    task_id='create_emr_database_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_database_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=CREATE_DATABASE,
    )
create_emr_database_sensor = EmrStepSensor(
    task_id='create_emr_database_sensor',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_database_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='create_emr_database_step', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    )

terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id='terminate_emr_cluster',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_database_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    )

emr_database_checks_done = DummyOperator(
    task_id="emr_database_checks_done",
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag,
)

create_emr_tables_step = EmrAddStepsOperator(
    task_id='create_emr_tables_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_database_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=CREATE_TABLES,
    )
create_emr_tables_sensor = EmrStepSensor(
    task_id='create_emr_tables_sensor',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_database_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='create_emr_tables_step', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    )


check_emr_movie_table = BranchPythonOperator(
    task_id='check_emr_movie_table',
    provide_context=True,
    python_callable=check_emr_table,
    trigger_rule="all_done",
    dag=dag,
)

check_emr_movie_table_skip = DummyOperator(
    task_id="check_emr_movie_table_skip",
    dag=dag,
)
check_emr_movie_table_done = DummyOperator(
    task_id="check_emr_movie_table_done",
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)

run_presto_script_step = EmrAddStepsOperator(
    task_id='run_presto_script_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_database_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=PRESTO_QUERY,
    )
run_presto_script_sensor = EmrStepSensor(
    task_id='run_presto_script_sensor',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_database_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='run_presto_script_step', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    )

create_comedy_table_step = EmrAddStepsOperator(
    task_id='create_comedy_table_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_database_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=CREATE_COMEDY_TABLES,
    )
create_comedy_table_sensor = EmrStepSensor(
    task_id='create_comedy_table_sensor',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_database_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='create_comedy_table_step', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    )



disp_variables >> create_emr_database_cluster >> check_emr_database

check_emr_database >> skip_emr_database_creation >> emr_database_checks_done  
check_emr_database >> create_emr_database_step >> create_emr_database_sensor >> create_emr_tables_step >> create_emr_tables_sensor >> emr_database_checks_done 

emr_database_checks_done >> check_emr_movie_table >> run_presto_script_step >> run_presto_script_sensor >> check_emr_movie_table_done >> create_comedy_table_step >> create_comedy_table_sensor >> terminate_emr_cluster
emr_database_checks_done >> check_emr_movie_table >> check_emr_movie_table_skip >> check_emr_movie_table_done >> create_comedy_table_step >> create_comedy_table_sensor >> terminate_emr_cluster 