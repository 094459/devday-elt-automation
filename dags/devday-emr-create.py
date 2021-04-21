from airflow import DAG, settings, secrets
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
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
    description='DevDay EMR DAG',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['devday','demo'],
)

# Set Variables used in tasks and stored in AWS Secrets Manager

s3_dlake = Variable.get("s3_dlake", default_var="undefined")
emr_db = Variable.get("emr_db", default_var="undefined")
emr_output = Variable.get("emr_output", default_var="undefined")
genre = Variable.get("emr_genre", default_var="undefined")
genre_t = Variable.get("emr_genre_table", default_var="undefined")


def py_display_variables(**kwargs):
    print("Data Lake location " + s3_dlake)
    print("EMR DB " + emr_db)
    print("Genre " + genre)
    print("Genre Table to be created " + genre_t)

disp_variables = PythonOperator (
	task_id='print_variables',
	provide_context=True,
	python_callable=py_display_variables,
	dag=dag
	)

## Amazon EMR info

CREATE_DATABASE = [
    {
        'Name': 'Create Genre Database',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'hive-script',
                '--run-hive-script',
                '--args',
                '-f',
                's3://{{ var.value.s3_dlake }}/scripts/create-film-db.hql'
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
                's3://{{ var.value.s3_dlake }}/scripts/create-film-db-tables.hql'
            ]
        }
    }
]

PRESTO_QUERY = [
    {
        'Name': 'Run Presto to create Genre Tables',
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

CREATE_GENRE_TABLES = [
    {
        'Name': 'Create Genre Tables',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'hive-script',
                '--run-hive-script',
                '--args',
                '-f',
                's3://{{ var.value.s3_dlake }}/scripts/create-genre-film-table.hql'
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
                'TargetSpotCapacity': 1,
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

### Amazon EMR Scripts - create and upload to Amazon S3

HIVE_CREATE_DB = """
create database {database}; 
""".format(database=emr_db)

HIVE_CREATE_DB_TABLES = """
CREATE EXTERNAL TABLE {database}.movies (
    movieId INT,
    title   STRING,
    genres  STRING

) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  LOCATION 's3://{datalake}/movielens/movies/';

CREATE EXTERNAL TABLE {database}.ratings (
    userId INT,
    movieId INT,
    rating INT,
    timestampId TIMESTAMP

) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  LOCATION 's3://{datalake}/movielens/ratings-alt/';
""".format(database=emr_db,datalake=s3_dlake)

HIVE_CREATE_GENRE_TABLE = """
CREATE EXTERNAL TABLE {database}.{genre_t} (
    title   STRING,
    year    INT,
    rating  INT

) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION 's3://{datalake}/movielens/{genre}/';
""".format(database=emr_db,genre=genre,datalake=s3_dlake,genre_t=genre_t)

PRESTO_SCRIPT_RUN_EXPFILE = """
#!/bin/bash
aws s3 cp s3://{datalake}/scripts/create-genre.sql .
presto-cli --catalog hive -f create-genre.sql --output-format TSV > {genre_t}-films.tsv
aws s3 cp {genre_t}-films.tsv s3://{datalake}/movielens/{genre}/
""".format(database=emr_db,genre=genre,datalake=s3_dlake,genre_t=genre_t)

PRESTO_SQL_GEN_GENRE_CSV = """
WITH {genre}data AS (
SELECT REPLACE ( m.title , '"' , '' ) as title, r.rating
FROM {database}.movies m
INNER JOIN (SELECT rating, movieId FROM {database}.ratings) r on m.movieId = r.movieId WHERE REGEXP_LIKE (genres, '{genre}')
  )
SELECT substr(title,1, LENGTH(title) -6) as title, replace(substr(trim(title),-5),')','') as year, AVG(rating) as avrating from {genre}data GROUP BY title ORDER BY year DESC,  title ASC ;
""".format(database=emr_db,genre=genre)


## Supporting Python callables

def py_create_emr_scripts(**kwargs):
    s3 = boto3.resource('s3')
    print("Creating scripts which will be executed by Amazon EMR - will overwrite existing scripts")
    # create create-film-db.hql
    object1 = s3.Object(s3_dlake, 'scripts/create-film-db.hql')
    object1.put(Body=HIVE_CREATE_DB)
    # create create-film-db-tables.hql
    object2 = s3.Object(s3_dlake, 'scripts/create-film-db-tables.hql')
    object2.put(Body=HIVE_CREATE_DB_TABLES)
    # create create-genre-film-table.hql
    object3 = s3.Object(s3_dlake, 'scripts/create-genre-film-table.hql')
    object3.put(Body=HIVE_CREATE_GENRE_TABLE)
    # create create-genre.sql
    object4 = s3.Object(s3_dlake, 'scripts/create-genre.sql')
    object4.put(Body=PRESTO_SQL_GEN_GENRE_CSV)
    # create run-presto-query.sh
    object5 = s3.Object(s3_dlake, 'scripts/run-presto-query.sh')
    object5.put(Body=PRESTO_SCRIPT_RUN_EXPFILE)   

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
        print("Table exists - create genre")
        return "run_presto_script_step"
    except:
        print("No Table Found - skip, as there are bigger problems!")
        return "check_emr_movie_table_skip" 

def check_genre_table(**kwargs):
    ath = boto3.client('athena')
    try:
        response = ath.get_table_metadata(
            CatalogName='AwsDataCatalog',
            DatabaseName= emr_db,
            TableName=genre_t
        )
        print("Table exists - skip creation we are done")
        return "skip_genre_table_creation"
    except:
        print("No Table Found - lets carry on")
        return "create_genre_table_step" 

def cleanup_emr_cluster_if_steps_fail(context):
    print("This is invoked when a running EMR cluster has a step running that fails.")
    print("If we do not do this, the DAG will stop but the cluster will still keep running")
    
    early_terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id='terminate_emr_cluster',
        job_flow_id=context["ti"].xcom_pull('create_emr_database_cluster'),
        aws_conn_id='aws_default',
        )
    return early_terminate_emr_cluster.execute(context=context)

## Dags

create_emr_scripts = PythonOperator (
	task_id='create_emr_scripts',
	provide_context=True,
	python_callable=py_create_emr_scripts,
	dag=dag
	)

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
    on_failure_callback=cleanup_emr_cluster_if_steps_fail,
    steps=CREATE_DATABASE,
    )
create_emr_database_sensor = EmrStepSensor(
    task_id='create_emr_database_sensor',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_database_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='create_emr_database_step', key='return_value')[0] }}",
    on_failure_callback=cleanup_emr_cluster_if_steps_fail,
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
    on_failure_callback=cleanup_emr_cluster_if_steps_fail,
    steps=CREATE_TABLES,
    )
create_emr_tables_sensor = EmrStepSensor(
    task_id='create_emr_tables_sensor',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_database_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='create_emr_tables_step', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    on_failure_callback=cleanup_emr_cluster_if_steps_fail,
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
    on_failure_callback=cleanup_emr_cluster_if_steps_fail,
    steps=PRESTO_QUERY,
    )
run_presto_script_sensor = EmrStepSensor(
    task_id='run_presto_script_sensor',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_database_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='run_presto_script_step', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    on_failure_callback=cleanup_emr_cluster_if_steps_fail,
    )

create_genre_table_step = EmrAddStepsOperator(
    task_id='create_genre_table_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_database_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    on_failure_callback=cleanup_emr_cluster_if_steps_fail,
    steps=CREATE_GENRE_TABLES,
    )
create_genre_table_sensor = EmrStepSensor(
    task_id='create_genre_table_sensor',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_database_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='create_genre_table_step', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    on_failure_callback=cleanup_emr_cluster_if_steps_fail,
    )

check_genre_table = BranchPythonOperator(
    task_id='check_genre_table',
    provide_context=True,
    python_callable=check_genre_table,
    retries=1,
    dag=dag,
)
skip_genre_table_creation = DummyOperator(
    task_id="skip_genre_table_creation",
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag,
)
check_genre_table_done = DummyOperator(
    task_id="check_genre_table_done",
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)



disp_variables >> create_emr_scripts >> create_emr_database_cluster >> check_emr_database

check_emr_database >> skip_emr_database_creation >> emr_database_checks_done  
check_emr_database >> create_emr_database_step >> create_emr_database_sensor >> create_emr_tables_step >> create_emr_tables_sensor >> emr_database_checks_done 

emr_database_checks_done >> check_emr_movie_table >> run_presto_script_step >> run_presto_script_sensor >> check_emr_movie_table_done >> check_genre_table 
emr_database_checks_done >> check_emr_movie_table >> check_emr_movie_table_skip >> check_emr_movie_table_done >> check_genre_table 

check_genre_table >> create_genre_table_step >> create_genre_table_sensor >> check_genre_table_done >> terminate_emr_cluster 
check_genre_table >> skip_genre_table_creation >> check_genre_table_done >> terminate_emr_cluster
