import json
import urllib.parse

import boto3
from model_output import ModelOutputHandler

print('Loading function')


def lambda_handler(event, context):
    print('Received event: ' + json.dumps(event, indent=2))
    s3 = boto3.client('s3')

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    # temp check so we can use covid19-forecast-hub data for volume testing
    if 'metadata' in key:
        print('metadata file, skipping...')
        return

    print(f'bucket: {bucket}')
    print(f'key: {key}')
    try:
        mo = ModelOutputHandler.from_s3(bucket, key)
        transformed_bucket, transformed_file_key = mo.transform_model_output()

        response = s3.get_object(Bucket=transformed_bucket, Key=transformed_file_key)
        transformed_file_info = {'key': transformed_file_key, 'content_type': response['ContentType']}
        print(f'TRANSFORMED FILE: {transformed_file_info}')
        return transformed_file_info

    except Exception as e:
        print(e)
        print(
            'Error transforming object {} from bucket {}'.format(
                key, bucket
            )
        )
        raise e
