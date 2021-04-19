import boto3
import sys
import time

s3 = boto3.resource('s3')
s3_dlake = "xxxxxxxx"

some_binary_data = """
WITH comedydata AS (
SELECT REPLACE ( m.title , '"' , '' ) as title, r.rating
FROM {database}.movies m
INNER JOIN (SELECT rating, movieId FROM {database}.ratings) r on m.movieId = r.movieId WHERE REGEXP_LIKE (genres, 'Comedy')
  )
SELECT title, replace(substr(trim(title),-5),')','') as year, AVG(rating) as avrating from comedydata GROUP BY title ORDER BY year DESC,  title ASC;
""".format(database=s3_dlake)


more_binary_data = 'Here we have some more data'

object = s3.Object(s3_dlake, 'filename.txt')
object.put(Body=some_binary_data)

# Method 2: Client.put_object()
client = boto3.client('s3')
client.put_object(Body=more_binary_data, Bucket=s3_dlake, Key='anotherfilename.txt')