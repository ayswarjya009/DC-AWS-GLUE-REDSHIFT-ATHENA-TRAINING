## Join SQl Query
SELECT  coviddata.date,
        coviddata.state,
        coviddata.positiveincrease,
        coviddata.totaltestresultsincrease,
        statename.StateName
FROM    coviddata LEFT JOIN statename
        ON  coviddata.state = statename.Code
WHERE   coviddata.state in ('NY', 'CA')



## SQl Query for Pivot by State

SELECT  date, positivePercentageNY, positivePercentageCA
FROM    positivepercentage 
        pivot (avg(positivePercentage) as positivePercentage 
        for state in ('NY' as positivePercentageNY, 'CA' as positivePercentageCA))



## Create schema and Table

CREATE SCHEMA demo;

CREATE TABLE demo.customer
(
    cust_id INT,
    cust_name TEXT,
    cust_age INT
);

## Drop and Create Schema and Table

drop TABLE "demo"."customer";

CREATE TABLE demo.customer (cust_id TEXT, cust_name TEXT, cust_age TEXT );

## New table
CREATE TABLE demo.customercrawler (cust_id BIGINT, cust_name TEXT, cust_age BIGINT);

## Redshift Role ARN

arn:aws:iam::786461327180:role/HKSOregonRedShiftRole

## Redshift Spectrum External Schema

CREATE EXTERNAL SCHEMA spectrum_schema
FROM DATA CATALOG
DATABASE 'hks-redshift-rs-db'
IAM_ROLE 'arn:aws:iam::786461327180:role/HKSOregonRedShiftRole'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

## Redshift Spectrum External Table
CREATE EXTERNAL TABLE spectrum_schema.spectrum_table (
    StateName TEXT,
    Abbrev TEXT,
    Code TEXT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 's3://hks-demo-s3-bucket/input-state-csv/';




## Redshift Spectrum Fetching record from Internal and External Table using Join

select * from demo.customer c
JOIN spectrum_schema.orders o 
ON c.cust_id= o.customer_id;

## Create Table Syntax Employee Table

create table demo.employees(
    employee_id INTEGER,
    name VARCHAR(100),
    designation VARCHAR(50),
    salary NUMERIC(10,2),
    hire_date DATE DEFAULT CURRENT_DATE,
    department VARCHAR(50)
);

## Insert Record Command in Emplyees Table

INSERT INTO employees (employee_id, name, designation, salary, hire_date, department)
VALUES
    (1, 'John Doe', 'Developer', 75000.00, '2023-01-15', 'Engineering'),
    (2, 'Jane Smith', 'Sales Manager', 85000.00, '2022-11-03', 'Sales'),
    (3, 'Alice Johnson', 'HR Specialist', 60000.00, '2024-06-12', 'Human Resources'),
    (4, 'Bob Brown', 'Marketing Coordinator', 52000.00, '2024-02-25', 'Marketing'),
    (5, 'Charlie Davis', 'Data Analyst', 68000.00, '2023-09-20', 'Data Science');

