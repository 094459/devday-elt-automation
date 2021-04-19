import os
import boto3
import sys
import time

ath = boto3.client('athena')
s3 = boto3.resource('s3')
s3_dlake = "xxxxxxx"
athena_db = "scifimovies"

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
    renamed_file = file.split('export/',1)[1]
    renamed_file = file.split(s3_dlake +'/',1)[1]

    copy_source = {'Bucket':s3_dlake, 'Key':renamed_file}
    bucket = s3.Bucket(s3_dlake)
    obj = bucket.Object('movielens/scifi/scifi-movies.gz')
    obj.copy(copy_source)

except Exception as e:
    print("Could not find path")
    print(e)
