import boto3
import pandas as pd 


s3 = boto3.client('s3')

s3.download_file('hks-demo','input-sample-data-csv/sample_data.csv','/tmp/sample_data.csv')

df = pd.read_csv('/tmp/sample_data.csv')

df['status'] = df['age'].apply(lambda x: 'Adult' if x>30 else 'Young')

df.to_csv('/tmp/transformed_data.csv', index=False)

s3.upload_file('/tmp/transformed_data.csv', 'hks-demo','output-sample-data-python/transformed_data.csv' )

print("Python job executed successfully")
