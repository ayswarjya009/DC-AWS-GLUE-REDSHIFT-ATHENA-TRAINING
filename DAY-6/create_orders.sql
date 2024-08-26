CREATE EXTERNAL TABLE IF NOT EXISTS sample_db.orders (
    order_id INT,
    customer_id INT,
    customer_name STRING,
    customer_email STRING,
    item_id INT,
    item_name STRING,
    item_price DOUBLE,
    item_quantity INT,
    order_date DATE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 's3://your-bucket-name/athena-lab/orders.csv'
TBLPROPERTIES ('skip.header.line.count'='1');
