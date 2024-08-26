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

## AWS Redhift Cluster View Script

create table demo.employees(
    employee_id INTEGER,
    name VARCHAR(100),
    designation VARCHAR(50),
    salary NUMERIC(10,2),
    hire_date DATE DEFAULT CURRENT_DATE,
    department VARCHAR(50)
);

INSERT INTO demo.employees (employee_id, name, designation, salary, hire_date, department)
VALUES
(101, 'Hitesh', 'Cloud Engineer', 100000.00, '2024-08-24', 'Engineering'),
(1, 'John Doe', 'Developer', 75000.00, '2023-01-15', 'Engineering'),
(2, 'Jane Smith', 'Sales Manager', 85000.00, '2022-11-03', 'Sales'),
(3, 'Alice Johnson', 'HR Specialist', 60000.00, '2024-06-12', 'Human Resources'),
(4, 'Bob Brown', 'Marketing Coordinator', 52000.00, '2024-02-25', 'Marketing'),
(5, 'Charlie Davis', 'Data Analyst', 68000.00, '2023-09-20', 'Data Science');

select * from demo.employees;


CREATE OR REPLACE VIEW demo.all_employees AS
select * from demo.employees;

select * from demo.all_employees;

create or replace view demo.avg_salary_by_department AS
select department as dept, avg(salary) as avg_salary
from demo.employees
GROUP BY department;

select * from demo.avg_salary_by_department;

create or replace view demo.high_salary_emp AS
select * from demo.employees
where salary > 60000;

select * from demo.high_salary_emp;


create or replace view demo.high_salary_emp AS
select * from demo.employees
where salary > 70000;

select * from demo.high_salary_emp;

DROP VIEW IF EXISTS demo.employeess;

DROP VIEW IF EXISTS demo.all_employees;

DROP VIEW IF EXISTS demo.high_salary_emp;


BEGIN;
INSERT INTO demo.employees (employee_id, name, designation, salary, hire_date, department)
VALUES
(102, 'Aman', 'Cloud Engineer', 100000.00, '2024-08-24', 'Engineering');

INSERT INTO demo.employees (employee_id, name, designation, salary, hire_date, department)
VALUES
(103, 'Rohan', 'Cloud Engineer', 100000.00, '2024-08-24', 'Engineering');

COMMIT;

ROLLBACK;


## Functions in Redshift Cluster

create or replace function demo.calculate_square(INTEGER)
returns INTEGER
immutable
AS $$
select $1 * $1
$$ LANGUAGE SQL;

select calculate_square(5) as SQUARE_OF_5;
select calculate_square(8) as SQUARE_OF_8;


create or replace function demo.classify_number(INTEGER)
returns VARCHAR
immutable
AS $$
select case
when $1 > 0 then 'Positive'
when $1 < 0 then 'Negative'
END;
$$ LANGUAGE SQL;

select demo.classify_number(10) as classification;
select demo.classify_number(-10) as classification;


create or replace function demo.reverse_string(TEXT)
returns TEXT
immutable
AS $$
select REVERSE($1)
$$ LANGUAGE SQL;

select demo.reverse_string('Redshidt') as reversed_String;


## Stored Procedures in Redshift

create or replace procedure demo.insert_employee(
    emp_id INTEGER, 
    emp_name VARCHAR, 
    emp_designation VARCHAR, 
    emp_sal NUMERIC, 
    emp_hire_date DATE, 
    emp_dept VARCHAR
    )
    AS $$
    BEGIN
        INSERT INTO demo.employees(employee_id, name, designation, salary, hire_date, department)
        VALUES (emp_id,emp_name,emp_designation,emp_sal,emp_hire_date,emp_dept);
    END;
    $$ LANGUAGE plpgsql;


    CALL demo.insert_employee(10,'Alice', 'Developer', 75000, '2023-01-15', 'Engineering');

    select * from demo.employees;

    DROP procedure demo.insert_employee(
    emp_id INTEGER, 
    emp_name VARCHAR, 
    emp_designation VARCHAR, 
    emp_sal NUMERIC, 
    emp_hire_date DATE, 
    emp_dept VARCHAR
    );

## AWS Athena Documentation Link

https://docs.aws.amazon.com/athena/latest/ug/creating-tables.html
