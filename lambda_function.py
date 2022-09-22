import json
import boto3

def lambda_handler(event, context):
    bucket=event['Records'][0]['s3']['bucket']['name']
    
    key=event['Records'][0]['s3']['object']['key']
    print("bucket name is ....................", bucket)
    data_dt=key[:10]
    print("folder name is ....................................", data_dt)
    glue=boto3.client('glue');
    response=glue.start_job_run(JobName = 'customer_data_rs_ingestion', Arguments={"--data_date":data_dt})
    print("Lambda invoked !!")
    