CREATE EXTERNAL TABLE IF NOT EXISTS sample_db.employees (
    id INT,
    name STRING,
    age INT,
    department STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = ','
)
LOCATION 's3://your-bucket-name/athena-lab/employees.csv'
TBLPROPERTIES ('has_encrypted_data'='false');
