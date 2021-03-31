import json
import boto3
import os
import uuid
import requests
import logger_func as logger
import zipfile
from botocore.client import Config
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime,timedelta

IMAGE_DYNAMODB_TABLE = os.environ.get('IMAGE_DYNAMODB_TABLE')
PRODUCT_DYNAMODB_TABLE = os.environ.get('PRODUCT_DYNAMODB_TABLE')
dynamodbResource = boto3.resource('dynamodb')
s3 = boto3.client('s3', config=Config(signature_version='s3v4'))
client = boto3.resource('s3')
productTable = dynamodbResource.Table(PRODUCT_DYNAMODB_TABLE)
productImageTable = dynamodbResource.Table(IMAGE_DYNAMODB_TABLE)
S3_BUCKETNAME = os.environ.get('S3_BUCKETNAME')

def lambda_handler(event, context):
    print(event)

    try:
        image=event.get('image', None)
        client.Bucket(S3_BUCKETNAME).download_file('public/product/' +image, '/tmp/images.zip')
        
        
        zip=zipfile.ZipFile('/tmp/images.zip')
        for info in zip.infolist():
            scan_filter = {
                'sku': { 'ComparisonOperator': 'EQ', 'AttributeValueList': [info.filename[0:6]] }
            }
            responses = productTable.scan(ScanFilter=scan_filter, Limit=1000)
            
            
            if 'Items' in responses and len(responses['Items']) > 0:
                resp = productImageTable.query(
                    IndexName='gsi-ProductImages',
                    KeyConditionExpression=Key('productImageProductId').eq(responses['Items'][0]['productId']))
                
                if (len(resp['Items'])==0):
                    expressionAttributeNames={
                        '#image': 'image'
                    }
                        
                    expressionAttributeValues={
                        ':image': 'product/images/' + info.filename
                    }
                
                    updateExpression = 'set #image = :image'
                
                    productTable.update_item(
                        Key={
                            'productId': responses['Items'][0]['productId'],
                        },
                        ExpressionAttributeNames=expressionAttributeNames,
                        UpdateExpression=updateExpression,
                        ExpressionAttributeValues=expressionAttributeValues
                    )
                    
                now = datetime.now()
                current_datetime = now.strftime("%Y-%m-%dT%H:%M:%S.000Z") 
                payload = {}
                payload['productImageId'] = str(uuid.uuid4())
                payload['productImageProductId'] = responses['Items'][0]['productId']
                payload['sequence'] = len(resp['Items'])
                payload['createdAt'] = current_datetime
                payload['updatedAt'] = current_datetime
                payload['image'] = 'product/images/' + info.filename
                productImageTable.put_item(Item=payload)
            
        with zipfile.ZipFile('/tmp/images.zip', 'w') as zip_ref:
            zip_ref.extractall()  
        
        for filename in os.listdir('/tmp'):
            url = s3.generate_presigned_url('put_object', Params={
                                            'Bucket': S3_BUCKETNAME, 'Key': 'public/product/images/'+filename,
                                            'ContentType': 'multipart/form-data'
                }, ExpiresIn=10000, HttpMethod='PUT')
            print(url)
            
            files = {'file': ('/tmp/'+filename)}
            requests.put(url, files=files)
        
        return {
            'statusCode': 200,
            'body': json.dumps('imageUpload')
        }

    except Exception as ex:
        logger.logError(str(ex))
        return { 'statusCode': 500, 'message': str(ex)}
