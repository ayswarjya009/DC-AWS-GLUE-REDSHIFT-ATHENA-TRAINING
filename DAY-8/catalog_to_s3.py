import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

# Initialize GlueContext
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session


database_name = "hks_sample_db"
table_name = "customers"


dynamic_frame= glueContext.create_dynamic_frame.from_catalog(database = database_name, table_name = table_name)

dynamic_frame.toDF().show(5)

output_path = "s3://hks-demo/output-new-catalog-s3/"

glueContext.write_dynamic_frame.from_options(
    frame = dynamic_frame, 
    connection_type = "s3", 
    connection_options = {"path":output_path}, format = "csv",
    format_options={"seperator": ","}
)






