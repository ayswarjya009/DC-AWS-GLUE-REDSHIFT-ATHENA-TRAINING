CREATE EXTERNAL TABLE IF NOT EXISTS sample_db.departments (
    department_id INT,
    department_name STRING,
    manager STRUCT<name: STRING, age: INT>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://your-bucket-name/athena-lab/departments.json'
TBLPROPERTIES ('has_encrypted_data'='false');
