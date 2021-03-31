import json
import boto3
import os
import io

# the yaml file belonged to a client so I couldn't include it in Github.
# the yaml file was provided by leader via CloudFormation too.
# yaml content was as follows:
"""
AWSTemplateFormatVersion : '2010-09-09'
Transform: AWS::Serverless-2016-10-31
Resources:
    lambda_func1:
        a lot of funcInfo
    lambda_func2:
        a lot of funcInfo
"""

cloudwatch_client = boto3.client("cloudwatch")

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")
s3_bucket_name = os.environ.get("S3_BUCKET_NAME")
s3_file = os.environ.get("S3_FILE")

def create_cloudwatch_alarm(lambdafunction_name, invoke_limit):
    cloudwatch_client.put_metric_alarm(
    AlarmName="Invoke alarm for "+lambdafunction_name,
    ComparisonOperator='GreaterThanOrEqualToThreshold',
    EvaluationPeriods=1,
    MetricName='Invocations',
    Dimensions=[
        {
          'Name': 'FunctionName',
          'Value': lambdafunction_name
        },
    ],
    Namespace='AWS/Lambda',
    Period=86400, #number of seconds in 1 day
    Statistic='Sum',
    Threshold=invoke_limit,
    ActionsEnabled=False,
    AlarmDescription='Alarm when lambda invokes more than '+str(invoke_limit)+' times'
    )


def lambda_handler(event, context):
    try:
        s3_resource.Bucket(s3_bucket_name).download_file(s3_file, '/tmp/file.yaml')
        with open("/tmp/file.yaml", 'r') as stream:
            for data in stream:
                if "FunctionName" in data:
                    functionName = data.replace("FunctionName: ","").replace(" ","").replace("\n","")
                    create_cloudwatch_alarm(functionName, 10) #set invoke limit here
    except Exception as error:
        logger.exception(error)
        response = {
            'status': 500,
            'error': {
                'function_name': "create_cloudwatch_alarm",
                'type': type(error).__name__,
                'description': str(error)
            },
        }
