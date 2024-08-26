import boto3
import time

def lambda_handler(event, context):
    # Initialize the Athena and S3 clients
    athena = boto3.client('athena')
    s3_output = 's3://your-bucket-name/athena-results/'  # Replace with your bucket
    
    # Define the query
    query = "SELECT * FROM sample_db.customers LIMIT 10;"
    
    # Start the Athena query
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': 'sample_db'
        },
        ResultConfiguration={
            'OutputLocation': s3_output
        }
    )
    
    # Get the query execution ID
    query_execution_id = response['QueryExecutionId']
    
    # Wait for the query to complete
    while True:
        query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']
        
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        
        time.sleep(1)
    
    # Fetch the query results
    if status == 'SUCCEEDED':
        result = athena.get_query_results(QueryExecutionId=query_execution_id)
        return {
            'statusCode': 200,
            'body': result['ResultSet']['Rows']
        }
    else:
        return {
            'statusCode': 500,
            'body': 'Query failed: ' + status
        }
