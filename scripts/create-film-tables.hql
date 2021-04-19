CREATE EXTERNAL TABLE {db}.movies (
    movieId INT,
    title   STRING,
    genres  STRING

) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  LOCATION 's3://{s3dlake}/movielens/movies/';

CREATE EXTERNAL TABLE {db}.ratings (
    userId INT,
    movieId INT,
    rating INT,
    timestampId TIMESTAMP

) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  LOCATION 's3://{s3dlake}/movielens/ratings-alt/';