import boto3
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AWS Glue Notebook").getOrCreate()
