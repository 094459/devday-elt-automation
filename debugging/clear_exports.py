import os
import boto3
import sys

s3_dlake = "xxxxxxx"

s3 = boto3.resource('s3')
bucket = s3.Bucket(s3_dlake)
try:
        print("Deleting files in exports folder")
        for obj in bucket.objects.filter(Prefix='export/'):
            print(bucket.name,obj.key)
            s3.Object(bucket.name,obj.key).delete()
except:
        print("Could not delete files for some reason - maybe not there?")