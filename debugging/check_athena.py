import os
import boto3
import sys

ath = boto3.client('athena')

try:
    response = ath.get_database(
        CatalogName='AwsDataCatalog',
        DatabaseName='scifimovies'
    )
    print("Database found")
except:
    print("No Database Found")


try:
    response = ath.get_table_metadata(
        CatalogName='AwsDataCatalog',
        DatabaseName='scifimovies',
        TableName='scifix'
    )
    print("Table Exists")
except:
    print("No Table Found")
