import requests
import json
import boto3
import botocore
import s3fs
import sys
import os
import uuid
import pandas as pd
from datetime import datetime,timedelta
from dateutil.parser import parse

# Account details are stored as constant.

API_KEY = os.environ.get("API_KEY")
DOMAIN = os.environ.get("DOMAIN")
FRESHDESK_LAMBDA_ARN = os.environ.get("FRESHDESK_LAMBDA_ARN")
FRESHDESK_CONVERSATION_LAMBDA_ARN = os.environ.get("FRESHDESK_CONVERSATION_LAMBDA_ARN")
S3_FRESHDESK_BUCKET = os.environ.get("S3_FRESHDESK_BUCKET")
S3_FRESHDESK_DATA_FILEPATH = os.environ.get("S3_FRESHDESK_DATA_FILEPATH")

lambdaClient = boto3.client('lambda')

START_DATE = ''
HISTORY_DATA = False

freshdeskTicketsArray = []
freshdeskTicketsConversationArray = []


# Function: GET Tickets
def lambda_handler(event, handler):
    getTickets(event)


def getTickets(event):
    global HISTORY_DATA
    global freshdeskTicketsArray
    global freshdeskTicketsConversationArray
    global START_DATE
    freshdeskTicketsArray = []
    freshdeskTicketsConversationArray = []

    START_DATE = event.get('startdate')
    HISTORY_DATA = False
    NO_TICKET_RECORD = False
    nextDateRange = None
    start_index = 1

    if (START_DATE is None or START_DATE == ''):
        START_DATE = str(datetime.date(datetime.now() - timedelta(days=1)))
        createDate = datetime.date(datetime.now() - timedelta(days=1))
        nextDateRange = datetime.date(datetime.now() - timedelta(days=1))
        HISTORY_DATA = False

    else:
        HISTORY_DATA = True

        # if start date is yesterday date, make it not history data
        createDate = datetime.date(datetime.strptime(START_DATE,"%Y-%m-%d"))
        if (createDate == datetime.date(datetime.now() - timedelta(days=1))):
            nextDateRange = datetime.date(datetime.now() - timedelta(days=1))
            HISTORY_DATA = False
        else:
        # call function to get next date range
            dateRange = getNextDateRange()
            nextDateRange = datetime.date(datetime.strptime(dateRange,"%Y-%m-%d"))
   
            if (nextDateRange >= datetime.now().date()):
                duration =  nextDateRange - datetime.now().date()
                nextDateRange -= (duration+timedelta(days=1))

    print(createDate, '  ', nextDateRange, '   ', HISTORY_DATA)
    
    while NO_TICKET_RECORD == False:

        response = requests.get(
            f"https://{DOMAIN}.freshdesk.com/api/v2/tickets?updated_since={createDate}&order_by=updated_at&order_type=asc&per_page=100&page={start_index}", auth = (API_KEY,""))
        
        responseContent = response.json()

        if(len(responseContent)>0):


            for item in responseContent:
                dt = parse(item['updated_at'])
                dateInRecord = dt.date()

                if HISTORY_DATA:
                    if (str(dateInRecord) >= str(nextDateRange)):
                        NO_TICKET_RECORD = True
                        break
                    else:                        
                        freshdeskTicketsArray.append(item)

                else:   
                    if (str(dateInRecord) > str(nextDateRange)):
                        NO_TICKET_RECORD = True
                        break
                    else:                        
                        freshdeskTicketsArray.append(item)

        else:
            print("no record")
            NO_TICKET_RECORD = True
            

        start_index+=1
        if(start_index == 301):
            NO_TICKET_RECORD = True

    storeData('freshdesk_tickets', freshdeskTicketsArray, nextDateRange)


def getNextDateRange():
    dateArray = START_DATE.split('-')
    monthToAdd = int(dateArray[1])

    dateRange = None
    if(monthToAdd >= 10):

        monthToAdd += 1
        if monthToAdd > 12:
            monthToAdd -= 12
            yearToAdd = int(dateArray[0]) + 1

            # more than december so year + 1
            dateRange = str(yearToAdd) + '-0' + str(monthToAdd) + '-' + dateArray[2]
 
        else:
            # not more than december so year remain same
            dateRange = dateArray[0] + '-' + str(monthToAdd) + '-' + dateArray[2]

    else:
        monthToAdd += 1
        if monthToAdd >=10:
            dateRange = dateArray[0] + '-' + str(monthToAdd) + '-' + dateArray[2]

        else:
            dateRange = dateArray[0] + '-0' + str(monthToAdd) + '-' + dateArray[2]

    return dateRange

# Function: Store data into CSV
def storeData(fileName, arrayItem, nextDateRange):
    global HISTORY_DATA

    FRESHDESK_HEADER = []
    
    STARTING_DATE = datetime.date(datetime.strptime(START_DATE,"%Y-%m-%d"))

    if (nextDateRange == datetime.date(datetime.now() - timedelta(days=1))):
        if (STARTING_DATE == datetime.date(datetime.now() - timedelta(days=1))):
            END_DATE = str(datetime.date(datetime.now() - timedelta(days=1)))
        else:    
            END_DATE = str(datetime.date(datetime.now() - timedelta(days=2)))
    else:
        END_DATE = str(nextDateRange-timedelta(days=1))

    dataList = []
    for record in arrayItem:
        tempArray=[]
        for item in record:
            tempArray.append(record[item])
        dataList.append(tempArray)

    uuidVariable = uuid.uuid4()
    
    if(len(arrayItem)>0):
        for key in arrayItem[0].keys():
            FRESHDESK_HEADER.append(key)

        saveLocation = f"s3://{S3_FRESHDESK_BUCKET}/{S3_FRESHDESK_DATA_FILEPATH}{fileName}({START_DATE}_{END_DATE}){uuidVariable}.csv"

        df = pd.DataFrame(dataList, columns=FRESHDESK_HEADER) 
        df.to_csv(saveLocation, index=False)
        print("Data saved.")

    if HISTORY_DATA:
        data = {'startdate': str(nextDateRange)}
        lambdaClient.invoke(FunctionName = FRESHDESK_LAMBDA_ARN,
                        InvocationType = 'Event',
                        Payload = json.dumps(data))

        
        print('run next Lambda')

    # arrayToPass = []
    # for ticket in freshdeskTicketsArray:
    #     arrayToPass.append(ticket['id'])

    # tickets_data = {'freshdesk_tickets_array': arrayToPass,
    #         'start_index': 0,
    #         'start_date': START_DATE,
    #         'end_date': END_DATE}
            
    # lambdaClient.invoke(FunctionName = FRESHDESK_CONVERSATION_LAMBDA_ARN,
    #                     InvocationType = 'Event',
    #                     Payload = json.dumps(tickets_data))
    
