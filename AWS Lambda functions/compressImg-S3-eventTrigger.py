import boto3 
import os 
import sys 
import uuid 
from PIL import Image 
from io import BytesIO, StringIO

s3_client = boto3.client('s3') 
s3_connection = boto3.resource('s3') 
list1 = [50, 100, 200, 400, 800, 1600] 

def createImage(x, y, im, ext, key): 
	size = x, y 
	print(size) 
	key = key + "_" + str(x) + "_" + str(y) + "." + ext 
	return im.resize(size), key 

def lambda_handler(event, context): 
	for record in event['Records']: 
		bucket = record['s3']['bucket']['name'] 
		key = record['s3']['object']['key'] 
		s3_object = s3_connection.Object(bucket, key) 
		response = s3_object.get() 
		split_key = key.split(".") 
		# Perform the resize operation 
		stream = StringIO(response['Body'].read()) 
		image=Image.open(stream) 
		x, y = image.size 
		if x == y: 
			for single_size in list1: 
				try:
					if single_size <= x: 
						resized_image, new_key = createImage(single_size, single_size, image, split_key[1], split_key[0]) 
						resized_data=resized_image.tobytes() 
						s3_resized_object = s3_connection.Object('testresize', new_key) 
						s3_resized_object.put(Body=resized_data, Bucket=bucket, Key=key) 
				except IOError: 
					print("Error") 
		elif x > y: 
			for single_size in list1: 
				try: 
					if single_size <= x: 
						single_size1 = int((single_size/x) * y) 
						if single_size1 != 0: 
							resized_image, new_key = createImage(single_size, single_size1, image, split_key[1], split_key[0]) 
							resized_data=resized_image.tobytes() 
							s3_resized_object = s3_connection.Object('testresize', new_key) 
							s3_resized_object.put(Body=resized_data, Bucket=bucket, Key=key) 
				except IOError: 
					print("Error") 
		else: 
			for single_size in list1: 
				try: 
					if single_size <= y: 
						single_size1 = int((single_size/y) * x) 
						if single_size1 != 0: 
							resized_image, new_key = createImage(single_size1, single_size, image, split_key[1], split_key[0]) 
							resized_data=resized_image.tobytes() 
							s3_resized_object = s3_connection.Object('testresize', new_key) 
							s3_resized_object.put(Body=resized_data, Bucket=bucket, Key=key) 
				except IOError: 
					print("Error") 
