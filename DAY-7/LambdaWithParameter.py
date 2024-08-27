import boto3
import time

def lambda_handler(event, context):
    # Athena client
    client = boto3.client('athena')

    # Extract parameters from the event
    table_name = event.get('table_name', 'your_default_table')
    condition = event.get('condition', '1=1')  # Default to no filtering

    # Set up the query
    query = f"SELECT * FROM {table_name} WHERE {condition} LIMIT 10;"
    database = "your_database"
    output = "s3://your-bucket-name/athena-results/"

    # Start the query execution
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output}
    )

    # Get the query execution ID
    query_execution_id = response['QueryExecutionId']

    # Wait for the query to finish
    while True:
        status = client.get_query_execution(QueryExecutionId=query_execution_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)

    # Check the status
    if state == 'SUCCEEDED':
        # Fetch results if needed
        result = client.get_query_results(QueryExecutionId=query_execution_id)
        return {
            'statusCode': 200,
            'body': f"Query succeeded and results are stored in {output}"
        }
    else:
        return {
            'statusCode': 400,
            'body': f"Query {state}"
        }
