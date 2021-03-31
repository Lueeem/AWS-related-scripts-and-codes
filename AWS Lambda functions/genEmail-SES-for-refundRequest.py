#import libraries
import boto3
import os
import csv
import datetime
import random
import uuid
from boto3.dynamodb.conditions import Key, Attr
import logging
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart

#logging - log settings
logger = logging.getLogger()
logger.setLevel(logging.WARNING)

#os - environment variables
REFUND_TABLE = os.environ.get('REFUND_TABLE')
SENDER_EMAIL = os.environ.get('SENDER_EMAIL')
RECIPIENT_EMAIL = os.environ.get('RECIPIENT_EMAIL')
TEMP_FILE = os.environ.get('TEMP_FILE')

#boto3 - aws sdk
dynamodbClient = boto3.client('dynamodb')
dynamodbResource = boto3.resource('dynamodb')
ses_client = boto3.client('ses', 'ap-southeast-2')

#dynamodb - table names
refundTable = dynamodbResource.Table(REFUND_TABLE)

#output
output = []

def generateCsvData(scanRefund):
    try:
        for data in scanRefund["Items"]:
            if(data['emailRefundStatus'] == False):
                payload = {}
                current_datetime = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                payload['Order Number'] = data['orderNumber']
                payload['Date Created'] = current_datetime
                payload['Amount'] = data['refundAmount']
                payload['TXN ID'] = data['refundTransactionId']
                payload['Payment Method'] = data['paymentMethod']
                payload['Reason'] = data['refundReason']
                output.append(payload)
        with open(TEMP_FILE, "w+") as outputFile:
            outputFile.truncate(0)
            header = ['Order Number', 'Date Created', 'Amount', 'TXN ID', 'Payment Method', 'Reason']
            writer = csv.DictWriter(outputFile, fieldnames=header)
            writer.writeheader()
            for data in output:
                writer.writerow(data)
    except Exception as error:
        logger.exception(error)
        response = {
            'status': 500,
            'error': {
                'function_name': "generateCsvData",
                'type': type(error).__name__,
                'description': str(error)
            },
        }

def sendEmail():
    current_datetime = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    msg = MIMEMultipart()
    msg['Subject'] = 'CompanyName Refund Request ' + current_datetime
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECIPIENT_EMAIL
   
    msg.preamble = 'Multipart message.\n'
   
    emailMessage = 'Dear Sir/Madam,\n\n'
    emailMessage += 'The attachment below is your refund request.\n\n'
    emailMessage += 'CompanyName Team'
    part = MIMEText(emailMessage)
    msg.attach(part)
   
    obj = open(TEMP_FILE, "rb")

    part = MIMEApplication(obj.read())
    part.add_header('Content-Disposition', 'attachment', filename="Refund Request.csv")
    msg.attach(part)

    result = ses_client.send_raw_email(
        RawMessage={'Data': msg.as_string()},
        Source=msg['From'],
        Destinations=[msg['To']]
    )

def lambda_handler(event, context):
    try:
        scanRefund = refundTable.scan()
        generateCsvData(scanRefund) #output = csvData
        sendEmail()
    except Exception as error:
        logger.exception(error)
        response = {
            'status': 500,
            'error': {
                'function_name': "lambda_handler",
                'type': type(error).__name__,
                'description': str(error)
            },
        }
