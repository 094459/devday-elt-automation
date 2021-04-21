from airflow import DAG, settings, secrets
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
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
    description='DevDay Athena Create SciFi DAG',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['devday','demo'],
)

# Set Variables used in tasks and stored in AWS Secrets Manager

s3_dlake = Variable.get("s3_dlake", default_var="undefined")
athena_db = Variable.get("athena_db", default_var="undefined")
athena_output = Variable.get("athena_output", default_var="undefined")

# Define the SQL we want to run to create the tables for our new venture

create_athena_movie_table_query="""
CREATE EXTERNAL TABLE IF NOT EXISTS {database}.movies (
  `movieId` int,
  `title` string,
  `genres` string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://{s3_dlake}/movielens/movies/'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'skip.header.line.count'='1'
); 
""".format(database=athena_db, s3_dlake=s3_dlake)

create_athena_ratings_table_query="""
CREATE EXTERNAL TABLE IF NOT EXISTS {database}.ratings (
  `userId` int,
  `movieId` int,
  `rating` int,
  `timestamp` bigint 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://{s3_dlake}/movielens/ratings/'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'skip.header.line.count'='1'
); 
""".format(database=athena_db, s3_dlake=s3_dlake)

create_athena_scifi_table_query = """
CREATE TABLE IF NOT EXISTS {database}.scifi AS WITH scifidata AS (
SELECT REPLACE ( m.title , '"' , '' ) as title, r.rating
FROM {database}.movies m
INNER JOIN (SELECT rating, movieId FROM {database}.ratings) r on m.movieId = r.movieId WHERE REGEXP_LIKE (genres, 'Sci-Fi')
)
SELECT substr(title,1, LENGTH(title) -6) as title, replace(substr(trim(title),-5),')','') as year, AVG(rating) as avrating from scifidata GROUP BY title ORDER BY year DESC,  title ASC;
""".format(database=athena_db)

def py_display_variables(**kwargs):
    print("Data Lake location " + s3_dlake + " ")
    print("New Athena DB " + athena_db + " ")
    print("Output CSV file we create " + athena_output + " ")

disp_variables = PythonOperator (
	task_id='print_variables',
	provide_context=True,
	python_callable=py_display_variables,
	dag=dag
	)

# Supporting functions that are called by DAGs
def check_athena_database(**kwargs):
    ath = boto3.client('athena')
    try:
        response = ath.get_database(
            CatalogName='AwsDataCatalog',
            DatabaseName=athena_db
        )
        print("Database already exists - skip creation")
        return "skip_athena_database_creation"
        #return "check_athena_export_table_done"
    except:
        print("No Database Found")
        return "create_athena_database"

def create_db(**kwargs):
    print("Creating the database if it doesnt exist")
    ath = boto3.client('athena')
    ath.start_query_execution(
        QueryString='CREATE DATABASE IF NOT EXISTS '+ athena_db,
        ResultConfiguration={'OutputLocation': 's3://{s3_dlake}/queries/'.format(s3_dlake=s3_dlake)},
        WorkGroup="devday-demo"
        )
# Dag logic goes here

check_athena_database = BranchPythonOperator(
    task_id='check_athena_database',
    provide_context=True,
    python_callable=check_athena_database,
    retries=1,
    dag=dag,
)

skip_athena_database_creation = DummyOperator(
    task_id="skip_athena_database_creation",
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag,
)

create_athena_database = PythonOperator (
    task_id='create_athena_database',
	provide_context=True,
	python_callable=create_db,
	dag=dag
    )

athena_database_checks_done = DummyOperator(
    task_id="athena_database_checks_done",
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag,
)

create_athena_movie_table = AWSAthenaOperator(
    task_id="create_athena_movie_table",
    query=create_athena_movie_table_query, 
    workgroup = "devday-demo", 
    database=athena_db,
    output_location='s3://'+s3_dlake+"/"+athena_output+'create_athena_movie_table'
    )
create_athena_ratings_table = AWSAthenaOperator(
    task_id="create_athena_movie_ratings",
    query=create_athena_ratings_table_query, 
    workgroup = "devday-demo", 
    database=athena_db,
    output_location='s3://'+s3_dlake+"/"+athena_output+'create_athena_ratings_table'
    )
create_athena_scifi_table = AWSAthenaOperator(
    task_id="create_athena_scifi_table",
    query=create_athena_scifi_table_query, 
    workgroup = "devday-demo", 
    database=athena_db,
    output_location='s3://'+s3_dlake+"/"+athena_output+'create_athena_scifi_table'
    )


disp_variables >> check_athena_database

check_athena_database >> skip_athena_database_creation >> athena_database_checks_done >> create_athena_movie_table >> create_athena_ratings_table >> create_athena_scifi_table  

check_athena_database >> create_athena_database >> athena_database_checks_done >> create_athena_movie_table >> create_athena_ratings_table >> create_athena_scifi_table  
