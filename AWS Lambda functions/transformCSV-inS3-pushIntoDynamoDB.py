"""
param:
productId = new uuid
row = rows from csv
department_checklist = python list with department_title from dynamodb
added_department_checklist = python list with department_title that are added within same process
"""

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
                KeyConditionExpression=Key('title').eq(csvDepartment)
            )
            
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
            added_department_checklist.append(csvSubDepartment)
            
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
            
    except Exception as error:
        logger.exception(error)
        response = {
            'status': 500,
            'error': {
                'function_name': "productDept_handler",
                'type': type(error).__name__,
                'description': str(error)
            },
        }
