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
CREATE TABLE demo.customercrawler (cust_id BIGINT, cust_name TEXT, cust_age BIGINT);
