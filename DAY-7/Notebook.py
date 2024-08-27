import boto3
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AWS Glue Notebook").getOrCreate()


s3_bucket = "my-glue-bucket"
file_path = "s3://{}/your-data-file.csv".format(s3_bucket)
df = spark.read.csv(file_path, header=True, inferSchema=True)
