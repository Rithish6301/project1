import json
import boto3
import os

# Initialize AWS clients
s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

# Get the Glue Job Name from environment variables
GLUE_JOB_NAME = os.environ['GLUE_JOB_NAME']

def lambda_handler(event, context):
    try:
        # Extract bucket name and file key from the event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']
        
        # Determine file format
        file_format = get_file_format(file_key)
        
        # Trigger the Glue job
        response = trigger_glue_job(bucket_name, file_key, file_format)
        
        return {
            'statusCode': 200,
            'body': json.dumps(f"Glue job triggered successfully for file {file_key}")
        }
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error processing file: {str(e)}")
        }

def trigger_glue_job(bucket_name, file_key, file_format):
    try:
        # Define job arguments
        job_arguments = {
            '--bucket': bucket_name,
            '--key': file_key,
            '--format': file_format
        }
        
        # Start Glue job
        response = glue_client.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments=job_arguments
        )
        
        print(f"Glue job {GLUE_JOB_NAME} started with run ID: {response['JobRunId']}")
        return response
    except Exception as e:
        print(f"Error triggering Glue job: {str(e)}")
        raise e

def get_file_format(file_key):
    # Identify file format based on file extension
    if file_key.lower().endswith(".parquet"):
        return 'parquet'
    elif file_key.lower().endswith('.avro'):
        return 'avro'
    else:
        raise ValueError(f"Unsupported file format for file: {file_key}")