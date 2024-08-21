def AggregateCaseCount (glueContext, dfc) -> DynamicFrameCollection:
    df = dfc.select(list(dfc.keys())[0]).toDF()
    from pyspark.sql import functions as f
    
    df0 = df.groupBy("combinedname").agg({"positiveincrease": "sum", "totaltestresultsincrease": "sum"})
    dyf0 = DynamicFrame.fromDF(df0, glueContext, "result0")
    return DynamicFrameCollection({"CustomTransform0": dyf0}, glueContext)
