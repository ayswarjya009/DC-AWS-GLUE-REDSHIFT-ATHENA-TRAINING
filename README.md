## Join SQl Query
SELECT  coviddata.date,
        coviddata.state,
        coviddata.positiveincrease,
        coviddata.totaltestresultsincrease,
        statename.StateName
FROM    coviddata LEFT JOIN statename
        ON  coviddata.state = statename.Code
WHERE   coviddata.state in ('NY', 'CA')
