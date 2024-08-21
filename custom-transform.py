def CreateMultipleOutput (glueContext, dfc) -> DynamicFrameCollection:
    df = dfc.select(list(dfc.keys())[0]).toDF()
    from pyspark.sql import functions as f
    
    df.createOrReplaceTempView("inputTable")
    df0 = spark.sql("SELECT TO_DATE(CAST(UNIX_TIMESTAMP(date, 'yyyyMMdd') AS TIMESTAMP)) as date, \
                            state , \
                            (positiveIncrease * 100 / totalTestResultsIncrease) as positivePercentage, \
                            StateName \
                    FROM inputTable ")
    
    df1 = df.withColumn('CombinedName', f.concat(f.col('StateName'), f.lit('('), f.col('state'), f.lit(')')))
    
    dyf0 = DynamicFrame.fromDF(df0, glueContext, "result0")
    dyf1 = DynamicFrame.fromDF(df1, glueContext, "result1")
    
    return DynamicFrameCollection({
                                    "CustomTransform0": dyf0, 
                                    "CustomTransform1": dyf1
                                    }, 
                                    glueContext)
