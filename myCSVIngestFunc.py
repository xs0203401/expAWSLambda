import json
import boto3

glue_client = boto3.client("glue")

def lambda_handler(event, context):
    # TODO implement
    print("hello from csv-ingest-")
    bkt = event['Records'][0]['s3']['bucket']
    obj = event['Records'][0]['s3']['object']
    print(obj)
    print(bkt)
    return glue_client.start_job_run(
        JobName = 'myCSVIngest-0821',
         Arguments = {
           '--new_bucket_name'  :   json.dumps(bkt),
           '--new_object_key'   :   json.dumps(obj) } )

    
    # return [json.dumps(obj), json.dumps(bkt)]
    # return {
    #     'statusCode': 200,
    #     'body': json.dumps(obj),
    #     'bkt': json.dumps(bkt)
    # }
