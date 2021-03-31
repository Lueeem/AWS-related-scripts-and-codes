from googleapiclient.discovery import build
import pandas as pd
from pathlib import Path
from oauth2client.service_account import ServiceAccountCredentials

import os
import boto3
import botocore
import s3fs
import uuid
import json
import datetime
from datetime import datetime, timedelta
import sys

S3_GOOGLE_ANALYTIC_BUCKET = '' 
S3_GOOGLE_ANALYTIC_CREDENTIAL_BUCKET_NAME = '' 
S3_GOOGLE_ANALYTIC_CREDENTIAL_FILE_KEY = '' 
S3_GOOGLE_ANALYTIC_SAVE_FILEPATH = '' 

PAYLOAD_START_DATE = ''
PAYLOAD_END_DATE = ''

PAGE_INDEX = 0
GA_TO_WRITE = []
GA_HEADER = []
COST_TO_WRITE = []
COST_HEADER = []

S3 = boto3.client('s3')
S3_RES = boto3.resource('s3')

GOOGLE_ANALYTIC_API_NAME = 'analytics'
GOOGLE_ANALYTIC_API_VERSION = 'v3',
GOOGLE_ANALYTIC_SCOPES = 'https://www.googleapis.com/auth/analytics.readonly'

def lambda_handler(event,handler):
    global S3_GOOGLE_ANALYTIC_BUCKET
    global S3_GOOGLE_ANALYTIC_CREDENTIAL_BUCKET_NAME
    global S3_GOOGLE_ANALYTIC_CREDENTIAL_FILE_KEY
    global S3_GOOGLE_ANALYTIC_SAVE_FILEPATH
    global PAGE_INDEX

    global GA_TO_WRITE
    global GA_HEADER
    global COST_TO_WRITE
    global COST_HEADER
    
    
    PAGE_INDEX = 0
    GA_TO_WRITE = []
    GA_HEADER = []
    COST_TO_WRITE = []
    COST_HEADER = []

    PAYLOAD_START_DATE = event.get('startdate') 
    PAYLOAD_END_DATE = event.get('enddate') 

    S3_GOOGLE_ANALYTIC_BUCKET = os.environ.get("S3_GOOGLE_ANALYTIC_BUCKET") 
    S3_GOOGLE_ANALYTIC_SAVE_FILEPATH = os.environ.get("S3_GOOGLE_ANALYTIC_SAVE_FILEPATH") 
    S3_GOOGLE_ANALYTIC_CREDENTIAL_BUCKET_NAME = os.environ.get("S3_GOOGLE_ANALYTIC_CREDENTIAL_BUCKET_NAME") 
    S3_GOOGLE_ANALYTIC_CREDENTIAL_FILE_KEY = os.environ.get("S3_GOOGLE_ANALYTIC_CREDENTIAL_FILE_KEY")

    GOOGLE_ANALYTIC_CREDENTIAL_FILE_DESTINATION = S3_RES.Object(S3_GOOGLE_ANALYTIC_CREDENTIAL_BUCKET_NAME, S3_GOOGLE_ANALYTIC_CREDENTIAL_FILE_KEY)

    GOOGLE_ANALYTIC_CREDENTIAL_FILE_DESTINATION = GOOGLE_ANALYTIC_CREDENTIAL_FILE_DESTINATION.get()['Body'].read()

    # Authenticate and construct service.
 
    # SERVICE = get_service(
    #         api_name = GOOGLE_ANALYTIC_API_NAME,
    #         api_version = GOOGLE_ANALYTIC_API_VERSION,
    #         scopes = GOOGLE_ANALYTIC_SCOPES,
    #         key_file_location=GOOGLE_ANALYTIC_CREDENTIAL_FILE_DESTINATION)
    # PROFILE_ID = get_first_profile_id(SERVICE)
    
    try: 
        date_range = ""
        if (PAYLOAD_START_DATE == "" and PAYLOAD_END_DATE == "") or (PAYLOAD_START_DATE == None and PAYLOAD_END_DATE == None):
            date_range = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

            PAYLOAD_START_DATE= "yesterday"
            PAYLOAD_END_DATE= "yesterday"
        else:
            date_range = PAYLOAD_START_DATE + '_' + PAYLOAD_END_DATE
        
        ga_dimensions='ga:userType,ga:browser,ga:deviceCategory,ga:date,ga:country,ga:medium,ga:channelGrouping'
        ga_metrics='ga:sessions,ga:transactionsPerSession,ga:bounceRate,ga:exitRate,ga:users'

        cost_dimensions='ga:transactionId,ga:date'
        cost_metrics='ga:transactionRevenue'

        for VALUE in range(100):
            print("GA ",VALUE)
            if saveGAResult(
                getGAResult(
                    SERVICE, 
                    PROFILE_ID, 
                    VALUE, 
                    PAYLOAD_START_DATE,
                    PAYLOAD_END_DATE,
                    ga_dimensions,
                    ga_metrics),
                    PAGE_INDEX) == None:
                        break
        
        for VALUE in range(100):
            print("Cost ",VALUE)
            if saveCostResult(getGAResult(
                SERVICE, 
                PROFILE_ID, 
                VALUE, 
                PAYLOAD_START_DATE,
                PAYLOAD_END_DATE,
                cost_dimensions,
                cost_metrics),
                PAGE_INDEX) == None:
                    break


        NEXT_PAYLOAD_START_DATE = datetime.strptime(getNextDateRange(1),"%Y-%m-%d")
        NEXT_PAYLOAD_END_DATE = datetime.strptime(getNextDateRange(2),"%Y-%m-%d")
        NEXT_PAYLOAD_END_DATE = datetime.combine(NEXT_PAYLOAD_END_DATE, datetime.max.time().replace(microsecond=0))
        
        PAYLOAD_START_DATE = str(NEXT_PAYLOAD_START_DATE)
        PAYLOAD_END_DATE = str(NEXT_PAYLOAD_END_DATE)

        dayBeforeYesterday = datetime.now() - timedelta(days=2)
        minDayBeforeYesterday = datetime.combine(dayBeforeYesterday, datetime.min.time())
        maxDayBeforeYesterday = datetime.combine(dayBeforeYesterday, datetime.max.time().replace(microsecond=0))
        if(NEXT_PAYLOAD_START_DATE >= maxDayBeforeYesterday):        
            NEXT_PAYLOAD_END_DATE = maxDayBeforeYesterday


        if (NEXT_PAYLOAD_START_DATE >= minDayBeforeYesterday):
            duration = PAYLOAD_START_DATE - minDayBeforeYesterday          
            NEXT_PAYLOAD_START_DATE -= duration

        print('next Date Range: ', NEXT_PAYLOAD_START_DATE, '  ', NEXT_PAYLOAD_END_DATE)
        if NEXT_PAYLOAD_START_DATE != minDayBeforeYesterday:
            NEXT_PAYLOAD_START_DATE = str(datetime.strftime(NEXT_PAYLOAD_START_DATE, "%Y-%m-%d"))
            NEXT_PAYLOAD_END_DATE = str(datetime.strftime(NEXT_PAYLOAD_END_DATE, "%Y-%m-%d"))
            
        if len(GA_TO_WRITE)>0:
            saveCSV(date_range, "ga-data", GA_TO_WRITE, GA_HEADER)

        if len(COST_TO_WRITE)>0:
            saveCSV(date_range, "ga-cost-data", COST_TO_WRITE, COST_HEADER)
            
        
    except:  
        print("err: " + getNextDateRange(1))
      
        if len(GA_TO_WRITE)>0:
            saveCSV(date_range, "ga-data", GA_TO_WRITE, GA_HEADER)
            
        if len(COST_TO_WRITE)>0:
                    saveCSV(date_range, "ga-cost-data", COST_TO_WRITE, COST_HEADER)
                    

def get_service(api_name, api_version, scopes, key_file_location):
    
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
            key_file_location, scopes=scopes)

    service = build(api_name, api_version, credentials=credentials)

    return service


def get_first_profile_id(service):

    accounts = service.management().accounts().list().execute()

    if accounts.get('items'):
        # Get the first Google Analytics account.
        account = accounts.get('items')[0].get('id')

        # Get a list of all the properties for the first account.
        properties = service.management().webproperties().list(
                accountId=account).execute()

        if properties.get('items'):
            property = properties.get('items')[0].get('id')

            profiles = service.management().profiles().list(
                    accountId=account,
                    webPropertyId=property).execute()

            if profiles.get('items'):
                # return the first view (profile) id.
                return profiles.get('items')[0].get('id')

    return None


def getGAResult(service, profile_id,pag_index,start_date,end_date, dimension, metric):
    # Use the Analytics Service Object to query the Core Reporting API
    print("check " +start_date+"  "+end_date + '\n')
    return service.data().ga().get(
            ids='ga:' + profile_id,
            start_date = start_date,
            end_date = end_date,
            start_index = str(pag_index*10000+1),
            samplingLevel = 'HIGHER_PRECISION',
            dimensions = dimension,
            metrics = metric,
            max_results=str(pag_index*10000+10000)).execute()


def saveGAResult(results,pag_index):
    
    if results:

        GAResults = results.get('rows')
        GAHeaders = results.get('columnHeaders')
        if GAResults is not None:
            
            for header in GAHeaders:
                if not len(GA_HEADER) == len(GAHeaders):
                    
                    GA_HEADER.append(header['name'])

            for result in GAResults:
                GA_TO_WRITE.append(result)
            
            return True
        else:
            print ('No results found')
            return None

    return True

def saveCostResult(results,pag_index):
    
    if results:
        
        GAResults = results.get('rows')
        GAHeaders = results.get('columnHeaders')
        if GAResults is not None:
            
            for header in GAHeaders:
                if not len(COST_HEADER) == len(GAHeaders):
                    
                    COST_HEADER.append(header['name'])

            for result in GAResults:
                COST_TO_WRITE.append(result)
            
            return True
        else:
            print ('No results found')
            return None

    return True

def saveCSV(date_range, filename, DATA_TO_WRITE, DATA_HEADER):
    print(date_range)
    df = pd.DataFrame(DATA_TO_WRITE, columns=DATA_HEADER)

    PATH_TO_SAVE = "." #'s3://' + S3_GOOGLE_ANALYTIC_BUCKET +'/' + S3_GOOGLE_ANALYTIC_SAVE_FILEPATH + 'ga-raw-date(' + date_range +')'+ str(uuid.uuid4())+'.csv' 
    PATH_TO_SAVE = './' + filename + '(' + date_range +')'+ str(uuid.uuid4())+'.csv' 

    df.to_csv(PATH_TO_SAVE, index=False)


def getNextDateRange(month):
    dateArray = PAYLOAD_START_DATE.split('-')
    monthToAdd = int(dateArray[1])

    dateRange = None
    if(monthToAdd >= 10):
    
        monthToAdd += month
        if monthToAdd > 12:
            monthToAdd -= 12
            yearToAdd = int(dateArray[0]) + 1

            # more than december so year + 1
            dateRange = str(yearToAdd) + '-0' + str(monthToAdd) + '-' + dateArray[2]
 
        else:
            # not more than december so year remain same
            dateRange = dateArray[0] + '-' + str(monthToAdd) + '-' + dateArray[2]

    else:
        monthToAdd += month
        if monthToAdd >=10:
            dateRange = dateArray[0] + '-' + str(monthToAdd) + '-' + dateArray[2]

        else:
            dateRange = dateArray[0] + '-0' + str(monthToAdd) + '-' + dateArray[2]

    return dateRange
