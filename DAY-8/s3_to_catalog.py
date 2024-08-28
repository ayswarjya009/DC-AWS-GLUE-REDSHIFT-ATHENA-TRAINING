import boto3
glue_client = boto3.client('glue')


database_name = "hks_demo_test"
response = glue_client.create_database(    
    DatabaseInput={
        'Name': database_name,
        'Description': 'This is my test DB'
    }
)
print("Database Created: ", response)

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

