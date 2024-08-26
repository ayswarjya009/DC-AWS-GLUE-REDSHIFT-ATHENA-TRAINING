CREATE EXTERNAL TABLE IF NOT EXISTS sample_db.sample_table_hks (
    id INT,
    name STRING,
    age INT,
    city STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = ','
) 
LOCATION 's3://hks-demo/input-sample-data-csv/';
TBLPROPERTIES ('skip.header.line.count'='1');
