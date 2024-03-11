import json
import urllib.parse
import boto3

from model_output import ModelOutputHandler

print('Loading function')


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    s3 = boto3.client('s3')

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print(f'bucket: {bucket}')
    print (f'key: {key}')
    try:
        mo = ModelOutputHandler(bucket, key)
        file = mo.read_file()
        transformed_file = mo.add_columns(file)
        transformed_bucket, transformed_file_key = mo.write_parquet(transformed_file)

        response = s3.get_object(Bucket=transformed_bucket, Key=transformed_file_key)
        transformed_file_info = {
            'key': transformed_file_key,
            'content_type': response['ContentType']
        }
        print(f'TRANSFORMED FILE: {transformed_file_info}')
        return transformed_file_info

    except Exception as e:
        print(e)
        print('Error transforming object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
              
