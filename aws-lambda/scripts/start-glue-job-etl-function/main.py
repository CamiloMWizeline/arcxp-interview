import json
import boto3

def lambda_handler(event, context):
    glue_client = boto3.client('glue')

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']

    glue_job_name = 'tlc-trip-data-etl-transformation-step'

    job_parameters = {
        '--bucket_name': bucket_name,
        '--object_file': object_key.split('/')[-1],
        '--object_key': "/".join(object_key.split('/')[:-1]).replace("%3D", "="),
        '--reference_file_path': f's3://{bucket_name}/raw/taxi-zones/taxi_zone_lookup.csv'
    }

    print(f"Received event. Bucket: {bucket_name}, Key: {object_key}")
    print(f"Job Parameters: {json.dumps(job_parameters, indent=2)}")

    try:
        response = glue_client.start_job_run(
            JobName=glue_job_name,
            Arguments=job_parameters
        )
        print(f"Started Glue job with run ID: {response['JobRunId']}")
    except Exception as e:
        print(f"Error starting Glue job: {str(e)}")
        raise e

    return {
        'statusCode': 200,
        'body': json.dumps('Glue Job triggered successfully!')
    }
