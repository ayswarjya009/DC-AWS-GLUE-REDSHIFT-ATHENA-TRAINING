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




s3_input_path = "s3://hks-demo/input-customer-csv/customers.csv"


df = spark.read.format("csv").option("header", "true").load(s3_input_path)
df.show()


output_s3_path = "s3://hks-demo/output-s3-catalog-demo/"
glueContext.write_dynamic_frame.from_options(
    frame = dyf,
    connection_type = "s3",
    connection_options = {"path": output_s3_path},
    format = "parquet",
    format_options = {"compression": "snappy"}
)

glue_client.create_table(
    DatabaseName=database_name,
    TableInput={
        'Name': 'tb_notebook',
        'StorageDescriptor': {
            'Columns': [{"Name": "customer_id", "Type": "int"},
                        {"Name": "customer_name", "Type": "string"},
                       {"Name": "customer_address", "Type": "string"}],
            'Location': output_s3_path,
            'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'Compressed': False,
            'NumberOfBuckets': -1,
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                'Parameters': {
                    'serialization.format': '1'
                }
            },
            'BucketColumns': [],
            'SortColumns': [],
            'StoredAsSubDirectories': False
        },
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'EXTERNAL': 'TRUE'
        }
    }
)
