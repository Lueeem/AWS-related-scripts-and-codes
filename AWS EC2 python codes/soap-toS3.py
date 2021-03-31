import json
import requests
import xmltodict
import csv
import awswrangler as wr
import pandas as pd
import boto3
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import os

def soapGetRequest(controllerNum, retry):
    try:
        if retry <= 10:
            controllerNum = str(controllerNum)
            url = "data source link"
            headers = {'Content-Type':'text/xml', 'SOAPAction':'soap get slave link'}
            body =  '<Envelope xmlns="xml namespace1"><Body><GetSlave xmlns="xml namespace2"><Controller>' + controllerNum + '</Controller></GetSlave></Body></Envelope>'
            
            resp = requests.post(url, headers=headers, data=body)
            if resp.status_code == 200:
                # Convert xml to python dict
                response_dict = xmltodict.parse(resp.text)
                
                # Process python dict
                for record in response_dict['soap:Envelope']['soap:Body']['GetSlaveResponse']['GetSlaveResult']['Items']['Item']:
                    if 'Name' in record and 'Value' in record and 'Units' in record:
                        if record['Name'] == "Total system power":
                            value = record['Value'] if record['Value'] else "0.00"
                            # Assign key value pair
                            print("Controller " + controllerNum + " recorded\n")
                            return {(controllerNum + " Total system power {kW}"): value}
                
                print("Controller " + controllerNum + " resource not found\n")
                return {(controllerNum + " Total system power {kW}"): "0.00"}
            else:
                print("Request failed for controller %s, remaining %d retry/retries...\n" % (controllerNum, (10 - retry)))
                return soapGetRequest(controllerNum, retry + 1)
        else:
            print("Request failed\n")
            return {(controllerNum + " Total system power {kW}"): "0.00"}
    except Exception as ex:
        print("Request Error: " + str(ex) + "\n")

def sendResourceData(controllerNum, utcDatetime, d):
    try:
        if d:
            ses_client = boto3.client('ses', region_name='ap-southeast-2')
            
            controllerNum = str(controllerNum)
            base_url = 'receiver API link'
            param = "&DeviceID=data_" + controllerNum + "&DateTimestamp=" + utcDatetime
            payload = {
                'controllerNumber': controllerNum,
                'timestamp': utcDatetime,
                }
            key = controllerNum + " Total system power {kW}"
            
            if key in d:
                param = param + "&ms1=" + str(d[key])
                payload[key] = d[key]
            else:
                param = param + "&ms1=0.00"
                payload[key] = "0.00"
            
            response = requests.put(base_url + param)
            retry = 1
            while response.status_code >= 400 and retry <= 10:
                response = requests.put(base_url + param)
                retry += 1
            if retry > 10:
                ses_client.send_email(
                    Source = "client2@CompanyName.com",
                    Destination = {
                        'ToAddresses': ["client1@CompanyName.com", "client2@CompanyName.com"],
                        'CcAddresses': [],
                        'BccAddresses': []
                        },
                    Message = {
                        'Subject': {
                            'Charset': "UTF-8",
                            'Data': "CompanyName data Soap: API Transfer Failed"
                            },
                        'Body': {
                            'Text': {
                                'Charset': "UTF-8",
                                'Data': "Failed to send resource data through API.\n" + json.dumps(payload)
                                }
                            }
                        }
                    )
                print("Failed to transfer data\n")
            else:
                print("Data transfer successful\n")
    except Exception as ex:
        print("Data Transfer Error: " + str(ex))

def produceCsvToS3(payloadList, inputDatetime):
    bucketName = "CompanyName-bucket"

    # Write data to csv
    header = ['Time', '901 Total system power {kW}', '902 Total system power {kW}', '903 Total system power {kW}']
    tempFile = open("/tmp/soapTemp.csv", 'w', newline='')
    writer = csv.DictWriter(tempFile, fieldnames=header)
    writer.writeheader()
    for data in payloadList:
        writer.writerow(data)
    tempFile.close()
    
    # Convert csv to parquet
    tempFile = open("/tmp/soapTemp.csv", "r")
    df = pd.read_csv(tempFile)

    previous_time = datetime.strptime(payloadList[0]['Time'], "%d/%m/%Y %H:%M:00") - timedelta(minutes=10)
    previous_time = previous_time.strftime("%d/%m/%Y %H:%M:00")

    for key, value in payloadList[0].items():
        if previous_time in last_value:
            if(key != "Time"):
                previous_value = str(float(df[key]))
                df[key] = str(round(float(df[key]) - float(last_value[previous_time][0][key]),2))
                print(df[key]+" = "+ previous_value +" - "+last_value[previous_time][0][key])
        else:
            if(key != "Time"):
                df[key] = 0.00

    wr.s3.to_parquet(
        df = df,
        path = "s3://" + bucketName + "/controllers/soap_data/year=" + inputDatetime.strftime("%Y") + "/month=" + inputDatetime.strftime("%m") + "/day=" + inputDatetime.strftime("%d") + "/CompanyNamesoap_" + datetime.strptime(payloadList[0]['Time'], "%d/%m/%Y %H:%M:00").strftime("%Y%m%d_%H%M") + ".parquet"
        )

    print("File Uploaded Successfully")

### Main Execution Below ###
last_value = {}
payload={}
while(True):
        if(payload != {}):
            last_value[payload['Time']] = [payload]

        now = datetime.utcnow() + timedelta(hours=8)

        for item in last_value.copy():
            previous = datetime.strptime(item, "%d/%m/%Y %H:%M:00")
            if(now.minute - previous.minute > 10):
                last_value.pop(item)
        
        #utcnow = utcnow.strftime("%Y-%m-%dT%H:%M:%SZ")
        payload = {'Time': now.strftime("%d/%m/%Y %H:%M:00")}
        
        outputDatetime =  now.strftime("%d/%m/%Y %H:%M:00")
        if(now.minute >= 0 and now.minute <= 9):
            payload['Time'] = outputDatetime.split(':')[0]+":10:"+outputDatetime.split(':')[2]
            now.replace(minute=10)
        elif(now.minute >= 10 and now.minute <= 19):
            payload['Time'] = outputDatetime.split(':')[0]+":20:"+outputDatetime.split(':')[2]
            now.replace(minute=20)
        elif(now.minute >= 20 and now.minute <= 29):
            payload['Time'] = outputDatetime.split(':')[0]+":30:"+outputDatetime.split(':')[2]
            now.replace(minute=30)
        elif(now.minute >= 30 and now.minute <= 39):
            payload['Time'] = outputDatetime.split(':')[0]+":40:"+outputDatetime.split(':')[2]
            now.replace(minute=40)
        elif(now.minute >= 40 and now.minute <= 49):
            payload['Time'] = outputDatetime.split(':')[0]+":50:"+outputDatetime.split(':')[2]
            now.replace(minute=50)
        elif(now.minute >= 50 and now.minute <= 59):
            payload['Time'] = outputDatetime.split(':')[0][:11]+str("%02d" %(now.hour+1,))+":00:"+outputDatetime.split(':')[2]
            now.replace(hour=now.hour+1, minute=0)
        
        sendresnow = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        for k in range(901, 910):
            payload.update(soapGetRequest(k, 1))
            sendResourceData(k, sendresnow, payload)
            
        print(payload)
        print("")
        produceCsvToS3([payload], now)
        

