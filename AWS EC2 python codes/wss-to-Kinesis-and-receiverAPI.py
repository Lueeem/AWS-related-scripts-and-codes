import boto3
import json
import requests
from websocket import create_connection
from datetime import datetime, timedelta

my_stream_name = 'CompanyNameDataStream'
kinesis_client = boto3.client('kinesis', region_name='ap-southeast-1')
ses_client = boto3.client('ses', region_name='ap-southeast-2')


newKey = ''
previousDict = {}
previousResDict = {}
newDict = {}

def bringOut(key, value, newKey):
    if (newKey == ''):
        newKey = key
    
    if (type(value) is dict):
        for k, v in value.items():
            bringOut(k, v, newKey+k)
    else:
        newDict[newKey] = value

def sendResourceData(d):
    try:
        if d:
            base_url = 'receiver api link'
            param = "&DeviceID=" + str(d["ResourcesID"]) + "&DateTimestamp=" + str(d["ResourcesCreated"])
            
            count = 1
            for key in ["ResourcesDataenergyunit", "ResourcesDataenergyvalue",
                        "ResourcesDataactivepowerunit", "ResourcesDataactivepowervalueA", "ResourcesDataactivepowervalueB", "ResourcesDataactivepowervalueC", "ResourcesDataactivepowervalueTotal",
                        "ResourcesDataapparentpowerunit", "ResourcesDataapparentpowervalueA", "ResourcesDataapparentpowervalueB", "ResourcesDataapparentpowervalueC", "ResourcesDataapparentpowervalueTotal",
                        "ResourcesDatareactivepowerunit", "ResourcesDatareactivepowervalueA", "ResourcesDatareactivepowervalueB", "ResourcesDatareactivepowervalueC", "ResourcesDatareactivepowervalueTotal",
                        "ResourcesDatavoltageunit", "ResourcesDatavoltagevalueAB", "ResourcesDatavoltagevalueBC", "ResourcesDatavoltagevalueCA", "ResourcesDatavoltagevalueAN", "ResourcesDatavoltagevalueBN", "ResourcesDatavoltagevalueCN", "ResourcesDatavoltagevalueLLAvg", "ResourcesDatavoltagevalueLNAvg",
                        "ResourcesDatacurrentunit", "ResourcesDatacurrentvalueA", "esourcesDatacurrentvalueB", "esourcesDatacurrentvalueC", "esourcesDatacurrentvalueTotal",
                        "ResourcesDatapowerfactorunit", "ResourcesDatapowerfactorvalueA", "ResourcesDatapowerfactorvalueB", "ResourcesDatapowerfactorvalueC", "ResourcesDatapowerfactorvalueAvg",
                        "ResourcesDatafrequencyunit", "ResourcesDatafrequencyvalue",
                        "ResourcesDatapeakdemandunit", "ResourcesDatapeakdemandvalue",
                        "ResourcesDatapredicteddemandunit", "ResourcesDatapredicteddemandvalue",
                        "ResourcesDatapresentdemandunit", "ResourcesDatapresentdemandvalue",
                        "ResourcesDatalastdemandunit", "ResourcesDatalastdemandvalue",
                        "ResourcesResponsestatus", "ResourcesResponseerror", "ResourcesTopic"]:
                if key in d:
                    param = param + "&ms" + str(count) + "=" + str(d[key])
                else:
                    param = param + "&ms" + str(count) + "=NULL"
                count += 1

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
                            'Data': "CompanyName Live: API Transfer Failed"
                            },
                        'Body': {
                            'Text': {
                                'Charset': "UTF-8",
                                'Data': "Failed to send resource data through API.\n" + json.dumps(d)
                                }
                            }
                        }
                    )
                print("Failed to transfer data\n")
            else:
                print("Data transfer successful\n")
    except Exception as ex:
        print("Data Transfer Error: " + str(ex))


ws = create_connection("wss link")
retry = 1
while True:
    try:
        
        #update previous value
        if(newDict != {}):
            previousResDict[newDict["ResourcesID"]] = newDict
            previousDict[newDict["ResourcesCreated"]] = previousResDict
                
        result = ws.recv()
        result = json.loads(result)
        for key, value in result.items():
            bringOut(key, value, newKey)
        newDict["ResourcesCreated"] = newDict["ResourcesCreated"][:-5]
        newDict["ResourcesCreated"] = datetime.strptime(newDict["ResourcesCreated"], "%Y-%m-%dT%H:%M:%S") + timedelta(hours=8)
        newDict["ResourcesCreated"] = newDict["ResourcesCreated"].strftime("%Y-%m-%dT%H:%M:%S")

        outputDatetime = newDict["ResourcesCreated"]
        inputDatetime =  datetime.strptime(outputDatetime, "%Y-%m-%dT%H:%M:%S")
        if(inputDatetime.minute >= 0 and inputDatetime.minute <= 9):
            newDict["ResourcesCreated"] = outputDatetime.split(':')[0]+":10:"+outputDatetime.split(':')[2]
        elif(inputDatetime.minute >= 10 and inputDatetime.minute <= 19):
            newDict["ResourcesCreated"] = outputDatetime.split(':')[0]+":20:"+outputDatetime.split(':')[2]
        elif(inputDatetime.minute >= 20 and inputDatetime.minute <= 29):
            newDict["ResourcesCreated"] = outputDatetime.split(':')[0]+":30:"+outputDatetime.split(':')[2]
        elif(inputDatetime.minute >= 30 and inputDatetime.minute <= 39):
            newDict["ResourcesCreated"] = outputDatetime.split(':')[0]+":40:"+outputDatetime.split(':')[2]
        elif(inputDatetime.minute >= 40 and inputDatetime.minute <= 49):
            newDict["ResourcesCreated"] = outputDatetime.split(':')[0]+":50:"+outputDatetime.split(':')[2]
        elif(inputDatetime.minute >= 50 and inputDatetime.minute <= 59):
            newDict["ResourcesCreated"] = outputDatetime.split(':')[0][:11]+str("%02d" %(inputDatetime.hour+1,))+":00:"+outputDatetime.split(':')[2]

        resID = newDict["ResourcesID"]

        #clear unneeded data in array
        for item in previousDict:
            checkDatetime = datetime.strptime(item, "%Y-%m-%dT%H:%M:%S")
            if(inputDatetime.minute - checkDatetime.minute > 10):
                previousDict.pop(item)

        previousDatetime = inputDatetime-timedelta(minutes=10)

        """
        if resID in previousDict[previousDatetime]:
            newDict["ResourcesDataenergyvalue"] = newDict["ResourcesDataenergyvalue"] - previousDict[previousDatetime][resID]["ResourcesDataenergyvalue"]
        """
        
        sendResourceData(newDict)
        jsonresult = json.dumps(newDict)

        kinesis_client.put_record(
            StreamName = my_stream_name,
            Data = jsonresult,
            PartitionKey = 'randomtext'
            )

        retry = 1
        print("Received '%s'\n" % jsonresult)
    except Exception as ex:
        print(str(ex))
        retry += 1
        if retry > 15:

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
                            'Data': "CompanyName Live: Websocket Down"
                            },
                        'Body': {
                            'Text': {
                                'Charset': "UTF-8",
                                'Data': "It seems like something is wrong with the websocket. Please investigate."
                                }
                            }
                        }
                    )

            print("Websocket is down\n")
            retry = 1
        ws.close()
        ws = create_connection("wss link")
        
