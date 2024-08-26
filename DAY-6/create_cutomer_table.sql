CREATE EXTERNAL TABLE IF NOT EXISTS sample_db.customers (
    customer_id INT,
    customer_name STRING,
    customer_address STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 's3://your-bucket-name/athena-lab/customers.csv'
TBLPROPERTIES ('skip.header.line.count'='1');
