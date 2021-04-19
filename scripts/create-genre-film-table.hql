--rename this file to create-{genre}-film-table.hql
CREATE EXTERNAL TABLE {db}.{genre} (
    title   STRING,
    year    INT,
    rating  INT

) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LOCATION 's3://{s3dlake}/{db}/{genre}/';