import json

def lambda_handler(event, context):
    # TODO implement
    print("hello from csv-ingest-")
    bkt = event['Records'][0]['s3']['bucket']
    obj = event['Records'][0]['s3']['object']
    print(obj)
    return {
        'statusCode': 200,
        'body': json.dumps(obj),
        'bkt': json.dumps(bkt)
    }
