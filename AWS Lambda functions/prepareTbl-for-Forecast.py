import json
import pandas as pd
import boto3
import os 
import io
from datetime import datetime,date,timedelta
from dateutil.relativedelta import relativedelta

BUCKET_NAME = 'forecastBucketName'
BUCKET_FILE_NAME = 'School and Public Holiday (all dates).csv'
LOCAL_FILE_NAME = 'holiday.csv'
SALES_FILE_NAME = "Promotion.xlsx"

s3 = boto3.resource("s3")

def lambda_handler(event, context):
    s3.meta.client.download_file(BUCKET_NAME, BUCKET_FILE_NAME, "/tmp/"+LOCAL_FILE_NAME)
    s3.meta.client.download_file(BUCKET_NAME, SALES_FILE_NAME, "/tmp/"+SALES_FILE_NAME)
    
    holidayDf = pd.read_csv("/tmp/"+LOCAL_FILE_NAME, usecols=['date', 'isKLPublicHoliday','isSelangorPublicHoliday','isSchoolHoliday'])
    
    holidayDf.date = pd.to_datetime(holidayDf.date,dayfirst=True)
    holidayDf['date'] = holidayDf['date'].dt.date

    holidayDf.loc[holidayDf[['isKLPublicHoliday','isSelangorPublicHoliday','isSchoolHoliday']].notnull().any(1), 'isHoliday'] = '1'
    holidayDf[list(['isKLPublicHoliday','isSelangorPublicHoliday','isSchoolHoliday'])] = holidayDf[list(['isKLPublicHoliday','isSelangorPublicHoliday','isSchoolHoliday'])].fillna(0.0).astype(int)
    holidayDf['isHoliday']=holidayDf['isHoliday'].fillna(0).astype(int)
    
    holidayDf['isHoliday']=holidayDf['isHoliday'].astype(str)
    
    holidayDf = holidayDf[['date', 'isHoliday']]
    recent_date = holidayDf['date'].max()    
    sixmonthago = recent_date - relativedelta(months=+6)
    
    holidayDf=holidayDf[holidayDf['date'] <= sixmonthago]
    
    holidayDf.to_csv("/tmp/modifiedHolidayLatest.csv", encoding='utf-8',index=False)
    
    my_sheet = "Sales"
    
    s3.meta.client.download_file(BUCKET_NAME, SALES_FILE_NAME, "/tmp/"+SALES_FILE_NAME)
    dfsales = pd.read_excel("/tmp/"+SALES_FILE_NAME, my_sheet, skiprows=4, usecols=['Item Number', 'Day','Sales Qty'])
    
    dfsales.Day = pd.to_datetime(dfsales.Day,dayfirst=True)
    
    recent_date = dfsales['Day'].max()
    sixmonthago = recent_date - relativedelta(months=+6)
    
    dfsales=dfsales[dfsales['Day'] <= sixmonthago]
    
    dfsales.to_csv("/tmp/salesCSV-reserve6.csv", encoding='utf-8',index=False)
    
    dfsales = pd.read_csv("/tmp/salesCSV-reserve6.csv")
    holidayDf = pd.read_csv("/tmp/modifiedHolidayLatest.csv")
      
    joined_tbl = pd.merge(dfsales,holidayDf,left_on='Day',right_on='date')
    joined_tbl.to_csv("/tmp/joined.csv", encoding='utf-8',index=False, columns=['Item Number', 'Day', 'isHoliday'], header=False)
    s3.meta.client.upload_file("/tmp/joined.csv", BUCKET_NAME, "tmp/joined.csv")
