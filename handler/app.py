from datetime import datetime
from io import BytesIO
from urllib import response
from PIL import Image, ImageOps
import os
import uuid
import boto3
import json

s3 = boto3.client('s3')
size = int(os.getenv('THUMBNAIL_SIZE'))
dbtable = str(os.getenv('DYNAMODB_TABLE'))
dynamodb = boto3.resource(
    'dynamodb', region_name=str(os.getenv('REGION_NAME')))


def s3_thumbnail_generator(event, context):
    print('Event::', event)

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    img_size = event['Records'][0]['s3']['object']['size']

    if not '_thumbnail.png' in key:
        image = get_s3_image(bucket, key)
        thumbnail = image_to_thumbnail(image)
        thumbnail_key = new_filename(key)
        url = upload_to_s3(bucket, thumbnail_key, thumbnail, img_size)

        return url


def get_s3_image(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    imageContent = response['Body'].read()

    file = BytesIO(imageContent)
    img = Image.open(file)
    return img


def image_to_thumbnail(image):
    return ImageOps.fit(image, (size, size), Image.ANTIALIAS)


def new_filename(key):
    key_split = key.rsplit('.', 1)
    return key_split[0] + '_thumbnail.' + key_split[1]


def upload_to_s3(bucket, thumbnail_key, thumbnail, img_size):
    # We are saving the image into a BytesIO object to avoid writing to disk
    out_thumbnail = BytesIO()

    # You MUST specify the file type because there is no file name to discern it from
    thumbnail.save(out_thumbnail, 'PNG')
    out_thumbnail.seek(0)

    response = s3.put_object(
        ACL='public-read',
        Body=out_thumbnail,
        Bucket=bucket,
        ContentType='image/png',
        Key=thumbnail_key
    )

    print(response)

    url = '{}/{}/{}'.format(s3.meta.endpoint_url, bucket, thumbnail_key)

    # Save image url to db
    s3_save_thunmbnail_url_to_dynamo(url_path=url, img_size=img_size)

    return url


def s3_save_thunmbnail_url_to_dynamo(url_path, img_size):
    toint = float(img_size*0.53)/1000
    table = dynamodb.Table(dbtable)
    response = table.put_item(
        Item={
            'id': str(uuid.uuid4()),
            'url': str(url_path),
            'approxReducedSize': str(toint) + ' KB',
            'createdAt': str(datetime.now()),
            'updatedAt': str(datetime.now())
        }
    )

    print('Image upload::', response)

    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(response)
    }


def s3_get_thumbnail_urls(event, context):
    # get all images urls from bucket and show in a json format
    table = dynamodb.Table(dbtable)
    response = table.scan()
    data = response['Items']
    # paginate through the results in a loop
    print('Response::', response)
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
        'body': json.dumps(data)
    }


def s3_get_item(event, context):
    table = dynamodb.Table(dbtable)
    response = table.get_item(Key={
        'id': event['pathParameters']['id']
    })

    item = response['Item']

    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
        'body': json.dumps(item),
        'isBase64Encoded': False
    }

def s3_delete_item(event, context):
    item_id = event['pathParameters']['id']

    # set default error response
    response = {
        "statusCode": 500,
        "body": f'An error ocurred while deleting record {item_id}'
    }

    table = dynamodb.Table(dbtable)
    response = table.delete_item(Key={
        'id': item_id
    })

    all_good_resp = {
        'deleted': True,
        'itemDeletedId': item_id
    }

    # If deletion is successful for record
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        response = {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps(all_good_resp)
        }
    
    return response
