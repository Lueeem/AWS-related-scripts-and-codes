#import libraries
import boto3
import os
import csv
import datetime
import random
import uuid
from boto3.dynamodb.conditions import Key, Attr
import logging

#logging - log settings
logger = logging.getLogger()
logger.setLevel(logging.WARNING)

#os - environment variables
PRODUCT_TABLE_PRODUCT = os.environ.get('PRODUCT_TABLE_PRODUCT')
PRODUCT_TABLE_TAGGING = os.environ.get('PRODUCT_TABLE_TAGGING')
PRODUCT_TABLE_TAGGINGMAPPING = os.environ.get('PRODUCT_TABLE_TAGGINGMAPPING')
PRODUCT_TABLE_CATEGORY = os.environ.get('PRODUCT_TABLE_CATEGORY')
PRODUCT_TABLE_DEPARTMENT = os.environ.get('PRODUCT_TABLE_DEPARTMENT')
PRODUCT_TABLE_PRODUCTDEPARTMENT = os.environ.get('PRODUCT_TABLE_PRODUCTDEPARTMENT')
S3_BUCKETNAME = os.environ.get('S3_BUCKETNAME')
S3_PRODUCTLIST = os.environ.get('S3_PRODUCTLIST')
S3_PRODUCTHIERARCHY = os.environ.get('S3_PRODUCTHIERARCHY')

#boto3 - aws sdk
dynamodbClient = boto3.client('dynamodb')
dynamodbResource = boto3.resource('dynamodb')
s3 = boto3.resource('s3')

#dynamodb - table names
productTable = dynamodbResource.Table(PRODUCT_TABLE_PRODUCT)
productTaggingTable = dynamodbResource.Table(PRODUCT_TABLE_TAGGING)
productTaggingMappingTable = dynamodbResource.Table(PRODUCT_TABLE_TAGGINGMAPPING)
productCategoryTable = dynamodbResource.Table(PRODUCT_TABLE_CATEGORY)
productDepartmentTable = dynamodbResource.Table(PRODUCT_TABLE_PRODUCTDEPARTMENT)
departmentTable = dynamodbResource.Table(PRODUCT_TABLE_DEPARTMENT)

#function - convert datetime to iso8601
def datetimeString_to_iso8601(datetimeString):
    if(datetimeString != ""):
        output = datetime.datetime.strptime(datetimeString, "%m/%d/%Y")
        output = output.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        return(output)
    return(datetimeString)

#function - convert to float only if string is not blank
def notBlank_float(item):
    if(item != ""):
        #String is not empty or blank
        return(float(item))
    #String is empty or blank
    return(1)

#function for payload_product
def generatePayloadProduct(row, productId, productHierarchyReader, category_checklist):
    #get data from csv
    current_datetime = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    csvItemCode = row[0]
    csvItemName = row[1]
    csvItemCategoryCode = row[2]
    csvCategoryDesc = row[3]
    if(len(str(row[4])) <= 5):
        csvProductGroupCode = "0"+str(row[4])
    else:
        csvProductGroupCode = row[4]
    csvProductGroupDesc = row[5]
    csvDimension = row[6]
    csvTemperature = row[7]
    csvItemTitle = row[8]
    csvItemDesc = row[9]
    csvDepartment = row[10]
    csvSubDepartment = row[11]
    csvUOM = row[12]
    csvPickupPrice = notBlank_float(row[13])
    csvDeliveryPrice = notBlank_float(row[14])
    csvItemPublishStartDate = row[16]
    csvItemPublishEndDate = row[17]
    csvPickupDiscount = notBlank_float(row[18].replace("%","")) #example value = 10%
    csvDeliveryDiscount = notBlank_float(row[19]) #example value = 2.3
    csvDiscountStartDatetime = row[20]
    csvDiscountEndDatetime = row[21]
    csvMaximumPickupOrderQty = row[22]
    csvMaximumDeliveryOrderQty = row[23]
    csvFoodPreparationDuration = row[24]
    #csvtaggings = row[25]
    csvImage = row[25]
    
    #get data from categoryTable
    if csvCategoryDesc in category_checklist and csvCategoryDesc in added_category_checklist:
        if(len(str(csvItemCategoryCode))<=3):
            category = productCategoryTable.query(
                IndexName='code-index',
                KeyConditionExpression=Key('code').eq("0"+str(csvItemCategoryCode)))
        else:
            category = productCategoryTable.query(
                IndexName='code-index',
                KeyConditionExpression=Key('code').eq(csvItemCategoryCode))
        if(category['Items'][0]['parentId'] != "-"):
            category = productCategoryTable.query(
                KeyConditionExpression=Key('categoryId').eq(category['Items'][0]['parentId']))
        categoryDivisionCode = category['Items'][0]['code']
        categoryDivisionDesc = category['Items'][0]['title']
    else:
        if(len(str(csvItemCategoryCode))<=3):
            csvItemCategoryCode = "0"+str(csvItemCategoryCode)
            payload_category = {}
            payload_category["__typename"]={"S": "Category"}
            payload_category["categoryId"]={"S": str(uuid.uuid4())}
            payload_category["code"]={"S": csvItemCategoryCode}
            payload_category["createdAt"]={"S": current_datetime}
            payload_category["parentId"]={"S": "-"}
            payload_category["title"]={"S": csvCategoryDesc}
            payload_category["updatedAt"]={"S": current_datetime}
            categoryDivisionCode = csvItemCategoryCode
            categoryDivisionDesc = csvCategoryDesc
        else:
            payload_category = {}
            payload_category["__typename"]={"S": "Category"}
            payload_category["categoryId"]={"S": str(uuid.uuid4())}
            payload_category["code"]={"S": csvItemCategoryCode}
            payload_category["createdAt"]={"S": current_datetime}
            payload_category["parentId"]={"S": "-"}
            payload_category["title"]={"S": csvCategoryDesc}
            payload_category["updatedAt"]={"S": current_datetime}
            categoryDivisionCode = csvItemCategoryCode
            categoryDivisionDesc = csvCategoryDesc
        added_category_checklist.append(csvCategoryDesc)
    
    payload_product = {}
    
    #leave empty first
    payload_product["minDeliveryDuration"]={'N': 0}
    
    #get from productCategoryTable
    payload_product["divisionCode"]={'S': categoryDivisionCode}
    payload_product["divisionDesc"]={'S': categoryDivisionDesc}
    
    #string
    payload_product["productId"]={'S': productId}
    payload_product["itemCategoryCode"]={'S': csvItemCategoryCode}
    payload_product["itemCategoryDesc"]={'S': csvCategoryDesc}
    payload_product["name"]={'S': csvItemName}
    payload_product["__typename"]={'S': "Product"}
    payload_product["productGroupCode"]={'S': csvProductGroupCode}
    payload_product["productGroupDesc"]={'S': csvProductGroupDesc}
    payload_product["title"]={'S': csvItemTitle}
    payload_product["description"]={'S': csvItemDesc}
    payload_product["uom"]={'S': csvUOM}
    payload_product["sku"]={'S': csvItemCode}
    #payload_product["taggings"]={'S': csvtaggings}
    payload_product["categories"]={'S': csvCategoryDesc}
    payload_product["image"]={'S': csvImage}
    
    #department
    payload_product["departmentLevel1"]={'S': csvDepartment}
    payload_product["departmentLevel2"]={'S': csvSubDepartment}
    
    #number
    payload_product["dimension"]={'N': csvDimension}
    payload_product["temperature"]={'N': csvTemperature}
    payload_product["price"]={'N': csvDeliveryPrice} #suggest change column name to deliveryPrice
    payload_product["pickupPrice"]={'N': csvPickupPrice}
    payload_product["discount"]={'N': csvDeliveryDiscount} #suggest change column name to deliveryDiscount
    payload_product["discountedPrice"]={'N': csvDeliveryPrice*100/csvDeliveryDiscount} #suggest change column name to deliveryDiscountedName
    payload_product["pickupDiscount"]={'N': csvPickupDiscount}
    payload_product["pickupDiscountedPrice"]={'N': csvPickupPrice*100/csvPickupDiscount}
    payload_product["ecommerceMaximumQuantity"]={'N': csvMaximumDeliveryOrderQty}
    payload_product["pickupMaximumQuantity"]={'N': csvMaximumPickupOrderQty}
    payload_product["minFoodPreparationDuration"]={'N': csvFoodPreparationDuration}
    
    #datetime string
    payload_product["updatedAt"]={'S': current_datetime}
    payload_product["discountStartDate"]={'S': datetimeString_to_iso8601(csvDiscountStartDatetime)}
    payload_product["pickupDiscountStartDate"]={'S': datetimeString_to_iso8601(csvDiscountStartDatetime)}
    payload_product["itemPublishStartDate"]={'S': datetimeString_to_iso8601(csvItemPublishStartDate)}
    payload_product["itemPublishEndDate"]={'S': datetimeString_to_iso8601(csvItemPublishEndDate)}
    payload_product["createdAt"]={'S': current_datetime}
    
    payload_product["isDisabled"]={'BOOL': False}
    return(payload_product)

#function for productTagging and productTaggingMappingId COMPLETE
def productTagging_handler(productId, checklist, inputTaggingCol, added_tag):
    try:
        #iterate through separate productTagging in taggings column in csv
        for tag in inputTaggingCol:
            #if tag is not already in productTaggingMapping table and is not already added in same lambda
            if tag not in checklist and tag not in added_tag:
                productTaggingId = str(uuid.uuid4())
                current_datetime = datetime.datetime.utcnow()
                current_datetime = current_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                current_year = datetime.datetime.strptime(current_datetime, '%Y-%m-%dT%H:%M:%S.%fZ').strftime("%Y")
                end_datetime = current_year+"-12-31T23:59:59.999Z"
                
                #insert record into productTaggingMapping dynamodb table
                payload_map = {}
                payload_map['productTaggingMappingId']={'S': str(uuid.uuid4())}
                payload_map['productTaggingMappingProductId']={'S': productId}
                payload_map['productTaggingMappingProductTaggingId']={'S': productTaggingId}
                payload_map['createdAt']={'S': current_datetime}
                payload_map['updatedAt']={'S': current_datetime}
                
                dynamodbClient.put_item(
                    TableName=productTaggingMappingTable,
                    Items=payload_map
                )
                
                #insert record into productTagging dynamodb table
                payload_tag = {}
                payload_tag['productTaggingId'] = {'S': productTaggingId}
                payload_tag['__typename'] = {'S': "ProductTagging"}
                payload_tag['createdAt'] = {'S': current_datetime}
                payload_tag['effectiveEndDate'] = {'S': end_datetime}
                payload_tag['effectiveStartDate'] = {'S': current_datetime}
                payload_tag['image'] = {'S': ""}
                payload_tag['isDisabled'] = {'BOOL': false}
                payload_tag['modifiedBy'] = {'S': "robot@axrail.com"}
                payload_tag['title'] = {'S': tag}
                payload_tag['updatedAt'] = {'S': current_datetime}
                payload_tag['createdBy'] = {'S': "robot@axrail.com"}
                payload_tag['created'] = {'S': current_datetime}
                payload_tag['modified'] = {'S': current_datetime}
                
                dynamodbClient.put_item(
                    TableName=productTaggingTable,
                    Items=payload_tag
                )
                
                #append to added_tag
                added_tag.append(tag)
    except Exception as error:
        logger.exception(error)
        response = {
            'status': 500,
            'error': {
                'function_name': "productTagging_handler",
                'type': type(error).__name__,
                'description': str(error)
            },
        }

#function for department
def department_handler(productId, row, department_checklist, added_department_checklist):
    try:
        current_datetime = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        current_year = datetime.datetime.strptime(current_datetime, '%Y-%m-%dT%H:%M:%S.%fZ').strftime("%Y")
        end_datetime = current_year+"-12-31T23:59:59.999Z"
        csvDepartment = row[10]
        csvSubDepartment = row[11]
        if csvSubDepartment in department_checklist: 
            #group subDepartment to Department
            department = departmentTable.query(
                IndexName='title-index',
                KeyConditionExpression=Key('title').eq(csvDepartment))
            for result in department['Items']:
                if (result['parentId']=="-"):
                    payload_productdept = {}
                    payload_productdept['createdAt']={'S': current_datetime}
                    payload_productdept['updatedAt']={'S': current_datetime}
                    payload_productdept['productDepartmentId']={'S': str(uuid.uuid4())}
                    payload_productdept['productDepartmentDepartmentId']={'S': result['departmentId']}
                    payload_productdept['productDepartmentProductId']={'S': productId }
                    payload_productdept['__typename']={'S': 'ProductDepartment'}
                    
                    dynamodbClient.put_item(
                        TableName = productDepartmentTable,
                        Item = payload_productdept,
                        ReturnValues = 'NONE',
                        ReturnConsumedCapacity = 'NONE',
                        ReturnItemCollectionMetrics = 'NONE'
                    )
                    
                    break
        if(csvDepartment not in department_checklist and csvDepartment not in added_department_checklist):
            departmentId = str(uuid.uuid4())
            #insert into department table
            payload_dept = {}
            payload_dept["departmentId"]={'S':departmentId}	
            payload_dept["__typename"]={'S':"Department"}	
            payload_dept["createdAt"]={'S':current_datetime}	
            payload_dept["effectiveEndDate"]={'S':end_datetime}	
            payload_dept["effectiveStartDate"]={'S':current_datetime}	
            payload_dept["isDisabled"]={'BOOL':False}	
            payload_dept["modifiedBy"]={'S':"robot@axrail.com"}	
            payload_dept["parentId"]={'S':"-"}	
            payload_dept["title"]={'S':csvDepartment}	
            payload_dept["updatedAt"]={'S':current_datetime}
            
            dynamodbClient.put_item(
                TableName = departmentTable,
                Item = payload_dept,
                ReturnValues = 'NONE',
                ReturnConsumedCapacity = 'NONE',
                ReturnItemCollectionMetrics = 'NONE'
            )
            
            #insert into productDepartment table
            payload_productdept = {}
            payload_productdept['createdAt']={'S': current_datetime}
            payload_productdept['updatedAt']={'S': current_datetime}
            payload_productdept['productDepartmentId']={'S': str(uuid.uuid4())}
            payload_productdept['productDepartmentDepartmentId']={'S': departmentId}
            payload_productdept['productDepartmentProductId']={'S': productId}
            payload_productdept['__typename']={'S': 'ProductDepartment'}
            
            dynamodbClient.put_item(
                TableName = productDepartmentTable,
                Item = payload_productdept,
                ReturnValues = 'NONE',
                ReturnConsumedCapacity = 'NONE',
                ReturnItemCollectionMetrics = 'NONE'
            )
            
            added_department_checklist.append(csvDepartment)
        if(csvSubDepartment not in department_checklist and csvSubDepartment not in added_department_checklist):
            departmentId = str(uuid.uuid4())
            
            #insert into department table
            payload_dept = {}
            payload_dept["departmentId"]={'S':departmentId}	
            payload_dept["__typename"]={'S':"Department"}	
            payload_dept["createdAt"]={'S':current_datetime}	
            payload_dept["effectiveEndDate"]={'S':end_datetime}	
            payload_dept["effectiveStartDate"]={'S':current_datetime}	
            payload_dept["isDisabled"]={'BOOL':False}	
            payload_dept["modifiedBy"]={'S':"robot@axrail.com"}	
            payload_dept["parentId"]={'S':"-"}	
            payload_dept["title"]={'S':csvSubDepartment}	
            payload_dept["updatedAt"]={'S':current_datetime}
            
            dynamodbClient.put_item(
                TableName = departmentTable,
                Item = payload_dept,
                ReturnValues = 'NONE',
                ReturnConsumedCapacity = 'NONE',
                ReturnItemCollectionMetrics = 'NONE'
            )
            
            
            #insert into productDepartment table
            payload_productdept = {}
            payload_productdept['createdAt']={'S': current_datetime}
            payload_productdept['updatedAt']={'S': current_datetime}
            payload_productdept['productDepartmentId']={'S': str(uuid.uuid4())}
            payload_productdept['productDepartmentDepartmentId']={'S': departmentId}
            payload_productdept['productDepartmentProductId']={'S': productId}
            payload_productdept['__typename']={'S': 'ProductDepartment'}
            
            dynamodbClient.put_item(
                TableName = productDepartmentTable,
                Item = payload_productdept,
                ReturnValues = 'NONE',
                ReturnConsumedCapacity = 'NONE',
                ReturnItemCollectionMetrics = 'NONE'
            )
            
            added_department_checklist.append(csvSubDepartment)
    except Exception as error:
        logger.exception(error)
        response = {
            'status': 500,
            'error': {
                'function_name': "productTagging_handler",
                'type': type(error).__name__,
                'description': str(error)
            },
        }

#main function
def lambda_handler(event, context):
    try:
        #get data from productList csv file in s3 bucket
        s3.Bucket(S3_BUCKETNAME).download_file("CompanyName Products Dataset/"+S3_PRODUCTLIST, '/tmp/productlist.csv')
        productFile = open("/tmp/productlist.csv", newline='', encoding='latin-1')
        productReader = csv.reader(productFile, delimiter=',', quotechar='"')
        productReader.__next__()
        #productReader.__next__()
        
        #get data from productHierarchy csv file in s3 bucket
        s3.Bucket(S3_BUCKETNAME).download_file("CompanyName Products Dataset/"+S3_PRODUCTHIERARCHY, '/tmp/productHierarchy.csv')
        productHierarchyFile = open("/tmp/productHierarchy.csv", newline='', encoding='latin-1')
        productHierarchyReader = csv.reader(productHierarchyFile, delimiter=',', quotechar='"')
        productHierarchyReader.__next__()
        
        #set up checklist for validation of product
        scanProduct = productTable.scan() #dynamodb - product table scan
        productTitle_checklist = {}
        added_productTitle_checklist = []
        for item in scanProduct['Items']:
            existingProductTitle = item['name']
            productTitle_checklist[existingProductTitle] = item['productId']
        
        #set up checklist for validation of category
        scanCategory = productCategoryTable.scan()
        category_checklist = []
        added_category_checklist = []
        for item in scanCategory["Items"]:
            category_checklist.append(item['title'])
        
        #set up checklist for validation of department
        scanDepartment = departmentTable.scan()
        department_checklist = []
        added_department_checklist = []
        for item in scanDepartment["Items"]:
            department_checklist.append(item['title'])
        
        #set up checklist for validation of productTagging
        scanProductTagging = productTaggingTable.scan() #dynamodb - tagging table scan
        tagging_checklist = []
        added_tag_checklist = []
        for item in scanProductTagging['Items']:
            tagging_checklist.append(item['title'])
        
        #for each row in csv file
        for row in productReader:
            rowProductTitle = row[1]
            taggings = row[25].split(", ")
            if rowProductTitle not in productTitle_checklist.keys() and rowProductTitle not in added_productTitle_checklist:
                productId = str(uuid.uuid4())
                payload_product = generatePayloadProduct(row, productId, productHierarchyReader, category_checklist)
                return(payload_product)
                dynamodbClient.put_item(
                        TableName=productTable,
                        Items=payload_product
                    )
                department_handler(productId, row, department_checklist, added_department_checklist)
                productTagging_handler(productId, tagging_checklist, taggings, added_tag_checklist)
                added_productTitle_checklist.append(rowProductTitle)
            else:
                productId = productTitle_checklist[rowProductTitle]
                payload_product = generatePayloadProduct(row, productId, productHierarchyReader, category_checklist)
                return(payload_product)
                dynamodbClient.put_item(
                        TableName=productTable,
                        Items=payload_product
                    )
                department_handler(productId, row, department_checklist, added_department_checklist)
                productTagging_handler(productId, tagging_checklist, taggings, added_tag_checklist)
        productFile.close()
        productHierarchyFile.close()
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
