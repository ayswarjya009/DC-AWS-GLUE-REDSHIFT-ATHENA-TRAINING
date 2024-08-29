import sys

from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import col, when

sc = SparkContext()
spark = SparkSession(sc)

input_path = "s3://hks-demo/input-sample-data-csv/sample_data.csv"
output_path= "s3://hks-demo/output-sample-data-python/"

df = spark.read.csv(input_path, header=True, inferSchema = True)

df_tf = df.withColumn("status", when(col("age")>30, "Adult").otherwise("Young"))

df_tf.write.csv(output_path,mode='overwrite', header=True)

print("ETL Spark Job Successfully Executed")
