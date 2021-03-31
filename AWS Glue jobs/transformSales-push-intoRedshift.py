"""
IMPORT LIBRARIES
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
from datetime import timedelta
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StringType, IntegerType, TimestampType
from pyspark.sql.functions import udf

"""
SET VALUE
"""
#environment setup
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME','gateway_country_key'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

"""
DECLARE LOCAL VARIABLE
"""
#set initial_datetime
initial_datetime = datetime(2018,1,1)
gateway_country = args['gateway_country_key']
gateway_country = gateway_country.split("_")
#set gateway
gateway = gateway_country[0]
#set country
country = gateway_country[1]
#set current_year
current_year = datetime.today().strftime("%Y")
#set current_month
current_month = datetime.today().strftime("%m")

"""
UDF
"""
#validate ip2 (based on remote_ip)
def ip2_exploder(remote_ip):
    if(remote_ip != ""):
        remote_ip_part = remote_ip.split(".")
        if(remote_ip_part[0] != "" and remote_ip_part[1] != ""):
            return(remote_ip_part[0]+"."+remote_ip_part[1])
ip2_udf = udf(ip2_exploder, StringType())
#validate ip3 (based on remote_ip)
def ip3_exploder(remote_ip):
    if(remote_ip != ""):
        remote_ip_part = remote_ip.split(".")
        if(remote_ip_part[0] != "" and remote_ip_part[1] != "" and remote_ip_part[2] != ""):
            return(remote_ip_part[0]+"."+remote_ip_part[1]+"."+remote_ip_part[2])
ip3_udf = udf(ip3_exploder, StringType())
#convert str to timestamp
str_to_timestamp =  F.udf (lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'), TimestampType())
#validate connection_type (based on carrier)
connection_type_udf = udf(lambda carrier: "wifi" if carrier == "" else "3g", StringType())
#validate free_sales_count (based on offer_id)
free_sales_count_udf = udf(lambda offerid: "1" if offerid is None else "0", StringType())
#validate optout24 (based on datediff)
optout24_udf = udf(lambda datediff: 1 if datediff == 0 else 0, IntegerType())

"""
GET DATAFRAME FROM TABLES
"""
def get_dataframe_marketing_var1():
    #datasource - marketing_var1
    datasource_marketing_var1 = glueContext.create_dynamic_frame.from_catalog(
        database = "athena-db1", 
        table_name = "marketing_companyname_marketing_var1", 
        redshift_tmp_dir = args["TempDir"], 
        transformation_ctx = "datasource_marketing_var1")
    #applymapping - marketing_var1
    applymapping_marketing_var1 = datasource_marketing_var1.apply_mapping([
        ("offer_id", "bigint", "offerid", "bigint"),
        ("name", "string", "offer_name", "string")
        ])
    #dataframe - marketing_var1
    dataframe_marketing_var1 = applymapping_marketing_var1.toDF()
    return(dataframe_marketing_var1)
def get_dataframe_marketing_var2():
    #datasource - marketing_var2
    datasource_marketing_var2 = glueContext.create_dynamic_frame.from_catalog(
        database = "athena-db1", 
        table_name = "marketing_companyname_marketing_var2", 
        redshift_tmp_dir = args["TempDir"], 
        transformation_ctx = "datasource_marketing_var2")
    #applymapping - marketing_var2
    applymapping_marketing_var2 = datasource_marketing_var2.apply_mapping([
        ("aff_id", "bigint", "affiliate_id", "bigint"),
        ("name", "string", "affiliate_name", "string")
        ])
    #dataframe - marketing_var2
    dataframe_marketing_var2 = applymapping_marketing_var2.toDF()
    return(dataframe_marketing_var2)
def get_dataframe_marketing_var3():
    #datasource - marketing_var3
    datasource_marketing_var3 = glueContext.create_dynamic_frame.from_catalog(
        database = "athena-db1", 
        table_name = "marketing_companyname_marketing_var3", 
        redshift_tmp_dir = args["TempDir"], 
        transformation_ctx = "datasource_marketing_var3")
    #applymapping - marketing_var3
    applymapping_marketing_var3 = datasource_marketing_var3.apply_mapping([
        ("flow_id", "bigint", "flow_type", "bigint"),
        ("flow_name", "string", "flow_name", "string")
        ])
    #dataframe - marketing_var3
    dataframe_marketing_var3 = applymapping_marketing_var3.toDF()
    return(dataframe_marketing_var3)
def get_dataframe_db_gateway_cust_setting():
    #datasource - gateway_cust_setting
    datasource_gateway_cust_setting = glueContext.create_dynamic_frame.from_catalog(
        database = "athena-db1", 
        table_name = "sms_companyname_db_gateway_cust_setting", 
        transformation_ctx = "datasource_cust_setting")
    #dataframe - gateway_cust_setting
    dataframe_gateway_cust_setting = datasource_gateway_cust_setting.toDF().where("enabled == 'yes'").filter(F.col("country") == country).filter(F.col("gateway") == gateway)
    return(dataframe_gateway_cust_setting)
def get_dataframe_db_gateway_cust():
    #naming = gateway_cust
    cust_table = "sms_companyname_db_gateway_"+gateway+"_"+country+"_cust"
    #datasource - gateway_cust
    datasource_gateway_cust = glueContext.create_dynamic_frame.from_catalog(
        database = "athena-db1", 
        table_name = cust_table, 
        redshift_tmp_dir = args["TempDir"], 
        transformation_ctx = "datasource_gateway_cust")
    #applymapping - gateway_cust
    applymapping_gateway_cust = datasource_gateway_cust.apply_mapping([
        ("rid", "bigint", "ref_cust_table_rid", "bigint"),
        ("subscribe_time", "string", "subscribetime", "string"),
        ("offer_id", "bigint", "offer_id", "bigint"),
        ("offer_price", "double", "offer_price", "double"),
        ("affiliate_id", "bigint", "affiliate_id", "bigint"),
        ("publisher_id", "string", "publisher_id", "string"),
        ("total_billed", "double", "total_billed", "double"),
        ("last_billed", "string", "last_billed", "string"),
        ("investor_campaign", "bigint", "investor_campaign", "bigint"),
        ("sync", "string", "sync", "string"),
        ("resub", "bigint", "resub", "bigint"),
        ("status", "bigint", "status", "bigint"),
        ("unsubscribe_time", "string", "unsubscribetime", "string"),
        ("shortcode", "bigint", "shortcode", "bigint"),
        ("keyword", "string", "keyword", "string"),
        ("operator", "string", "operator", "string"),
        ("msisdn", "bigint", "msisdn", "bigint"),
        ("mo_id", "string", "mo_id", "string")                                            
        ])
    #dataframe - gateway_cust                              
    dataframe_gateway_cust = applymapping_gateway_cust.toDF()
    #convert str to timestamp - subscribe_time                                           
    dataframe_gateway_cust = dataframe_gateway_cust.withColumn('subscribetime', str_to_timestamp(F.col('subscribetime')))
    #convert str to timestamp - unsubscribe_time
    dataframe_gateway_cust = dataframe_gateway_cust.withColumn('unsubscribetime', str_to_timestamp(F.col('unsubscribetime')))
    #filter dataframe_gateway_cust with conditions [(sync == 'pending') is commented out because most of records has value as 'done' in datasource]              
    dataframe_gateway_cust = dataframe_gateway_cust.filter(F.col("subscribetime") > initial_datetime).limit(10000) #where("sync == 'pending'")
    return(dataframe_gateway_cust)
def get_dataframe_db_gateway_cust_data():
    #naming = gateway_[$gateway]_[$country]_cust_data
    cust_data_table = "sms_companyname_db_gateway_"+gateway+"_"+country+"_cust_data"
    #datasource - gateway_cust_data
    datasource_gateway_cust_data = glueContext.create_dynamic_frame.from_catalog(
        database = "athena-db1", 
        table_name = cust_data_table, 
        redshift_tmp_dir = args["TempDir"], 
        transformation_ctx = "datasource_gateway_cust_data")
    #applymapping - gateway_cust_data                                            
    applymapping_gateway_cust_data = datasource_gateway_cust_data.apply_mapping([
        ("rid", "bigint", "ref_cust_data_table_rid", "bigint"),
        ("click_time", "string", "click_time", "string"),
        ("view_time", "string", "view_time", "string"),
        ("lead_time", "string", "lead_time", "string"),
        ("conversion_time", "string", "conversion_time", "string"),
        ("click_cost", "double", "click_cost", "double"),
        ("view_cost", "double", "view_cost", "double"),
        ("lead_cost", "double", "lead_cost", "double"),
        ("conversion_cost", "double", "conversion_cost", "double"),
        ("flow_type", "string", "flow_type", "string"),
        ("remote_ip", "string", "remoteip", "string"),
        ("carrier", "string", "carrier", "string"),
        ("client_type", "string", "client_type", "string"),
        ("device_name", "string", "device_name", "string"),
        ("offer_url", "string", "offer_url", "string"),
        ("webview_app", "string", "webview_app", "string"),
        ("total_click", "bigint", "total_click", "bigint"),
        ("total_view", "bigint", "total_view", "bigint"),
        ("total_lead", "bigint", "total_lead", "bigint"),
        ("ref_table_id", "bigint", "ref_cust_table_rid", "bigint")
        ])
    #dataframe - gateway_cust_data                                                         
    dataframe_gateway_cust_data = applymapping_gateway_cust_data.toDF()
    #convert str to timestamp - click_time 
    dataframe_gateway_cust_data = dataframe_gateway_cust_data.withColumn('click_time', str_to_timestamp(F.col('click_time')))
    #convert str to timestamp - view_time
    dataframe_gateway_cust_data = dataframe_gateway_cust_data.withColumn('view_time', str_to_timestamp(F.col('view_time')))
    #convert str to timestamp - lead_time
    dataframe_gateway_cust_data = dataframe_gateway_cust_data.withColumn('lead_time', str_to_timestamp(F.col('lead_time')))
    #convert str to timestamp - conversion_time
    dataframe_gateway_cust_data = dataframe_gateway_cust_data.withColumn('conversion_time', str_to_timestamp(F.col('conversion_time')))
    return(dataframe_gateway_cust_data)
def get_dataframe_db_gateway_info():
    #naming - gateway_info
    info_table = "sms_companyname_db_gateway_"+gateway+"_"+country+"_info"
    #datasource - gateway_info
    datasource_gateway_info = glueContext.create_dynamic_frame.from_catalog(
        database = "athena-db1", 
        table_name = info_table, 
        transformation_ctx = "datasource_gateway_info")
    #applymapping - gateway_info
    applymapping_gateway_info = datasource_gateway_info.apply_mapping([
        ("charge_price", "double", "charge_price", "double"),
        ("info_category", "string", "info_category", "string"),
        ("dn_status", "bigint", "dn_status", "bigint")
        ])
    #dataframe - gateway_info
    dataframe_gateway_info = applymapping_gateway_info.toDF().where("charge_price > 0").where("info_category == 'first'").filter(F.col("dn_status") == 1)
    return(dataframe_gateway_info)
    
"""
JOIN DATAFRAMES
"""
#inner join function
def inner_join(dataframe1, dataframe2, join_cond):
    joined = dataframe1.join(dataframe2, join_cond, 'inner')
    return(joined)
    
"""
PUSH TO REDSHIFT
"""
def push_to_redshift(dataframe_name):
    #convert dataframe to dynamicframe
    dynamicframe = DynamicFrame.fromDF(dataframe_name, glueContext, "nested")
    
    #applymapping - dynamicframe
    applymapping = dynamicframe.apply_mapping([
        ("rid", "bigint", "ref_cust_data_table_rid", "bigint"),
        ("click_time", "string", "click_time", "string"),
        ("view_time", "string", "view_time", "string"),
        ("lead_time", "string", "lead_time", "string"),
        ("conversion_time", "string", "conversion_time", "string"),
        ("click_cost", "double", "click_cost", "double"),
        ("view_cost", "double", "view_cost", "double"),
        ("lead_cost", "double", "lead_cost", "double"),
        ("conversion_cost", "double", "conversion_cost", "double"),
        ("flow_type", "string", "flow_type", "string"),
        ("flow_name", "string", "flow_name", "string"),
        ("remoteip", "string", "remote_ip", "string"),
        ("carrier", "string", "carrier", "string"),
        ("client_type", "string", "client_type", "string"),
        ("device_name", "string", "device_name", "string"), 
        ("offer_url", "string", "offer_url", "string"), 
        ("webview_app", "string", "webview_app", "string"),
        ("total_click", "bigint", "click_count", "bigint"),
        ("total_view", "bigint", "view_count", "bigint"),
        ("total_lead", "bigint", "lead_count", "bigint"),
        ("ref_cust_table_rid", "bigint", "ref_cust_table_rid", "bigint"),
        ("offer_name", "string", "offer_name", "string"),
        ("mo_id", "string", "mo_id", "string"),
        ("total_billed", "double", "total_billed", "double"),
        ("last_billed", "string", "last_billed", "string"),
        ("publisher_id", "string", "publisher_id", "string"),
        ("offer_price", "bigint", "offer_price", "double"),
        ("unsubscribetime", "string", "unsubscribe_time", "string"),
        ("status", "bigint", "status", "bigint"),
        ("investor_campaign", "bigint", "investor_campaign", "bigint"),
        ("connection_type", "string", "connection_type", "string"),
        ("sales_count", "bigint", "sales_count", "bigint"),
        ("free_sales_count", "bigint", "free_sales_count", "bigint"),
        ("update_year", "string", "update_year", "string"),
        ("update_month", "string", "update_month", "string"),
        ("resub", "bigint", "resub", "bigint"),
        ("optout24", "bigint", "optout24", "bigint"),
        ("offerid", "bigint", "offer_id", "bigint"),
        ("affiliate_id", "bigint", "affiliate_id", "bigint"),
        ("affiliate_name", "string", "affiliate_name", "string"),
        ("subscribetime", "string", "subscribe_time", "string"),
        ("country", "string", "country", "string"),
        ("gateway", "string", "gateway", "string"),
        ("free_pixels_count", "bigint", "free_pixels_count", "bigint"),
        ("pixels_count", "bigint", "pixels_count", "bigint"),
        ("first_billing_count", "bigint", "first_billing_count", "bigint"),
        ("ip2", "string", "ip2", "string"),
        ("ip3", "string", "ip3", "string"),
        ("shortcode", "bigint", "shortcode", "bigint"),
        ("keyword", "string", "keyword", "string"),
        ("operator", "string", "operator", "string"),
        ("msisdn", "bigint", "msisdn", "bigint")])
    
    #push to redshift
    sales_table = "gateway_cust_portfolio_"+gateway+"_"+country
    datasink = glueContext.write_dynamic_frame.from_jdbc_conf(
        frame = applymapping, 
        catalog_connection = "redshiftcluster", 
        connection_options = {"database" : "development", "dbtable" : sales_table}, 
        redshift_tmp_dir = args["TempDir"], 
        transformation_ctx = "datasink")
    
"""
CALL FUNCTIONS
"""
#get_dataframe_marketing_var1
dataframe_marketing_var1 = get_dataframe_marketing_var1()
#get_dataframe_marketing_var2
dataframe_marketing_var2 = get_dataframe_marketing_var2()
#get_dataframe_marketing_var3
dataframe_marketing_var3 = get_dataframe_marketing_var3()
#get_dataframe_db_gateway_cust_setting
dataframe_db_gateway_cust_setting = get_dataframe_db_gateway_cust_setting()
#get_dataframe_db_gateway_cust
dataframe_db_gateway_cust = get_dataframe_db_gateway_cust()
#get_dataframe_db_gateway_cust_data
dataframe_db_gateway_cust_data = get_dataframe_db_gateway_cust_data()
#get_dataframe_db_gateway_info
dataframe_db_gateway_info = get_dataframe_db_gateway_info()
#get_first_billing_wait_day_from_gateway_cust_setting
first_billing_wait_day = dataframe_db_gateway_cust_setting.select(["first_billing_wait_day"]).collect()[0][0]
#get_custom_info_table_from_gateway_cust_setting
custom_info_table = dataframe_db_gateway_cust_setting.select(["custom_info_table"]).collect()[0][0]
#get_count(*)_from_dataframe_db_gateway_info
count_db_gateway_info = dataframe_db_gateway_info.count()
#inner_join_marketing_var1_with_gateway_cust
join_var1_and_gateway_cust = inner_join(
    dataframe_db_gateway_cust, 
    dataframe_marketing_var1,
    dataframe_db_gateway_cust.offer_id == dataframe_marketing_var1.offerid
    )
#inner_join_marketing_var2_with_gateway_cust
join_var2_with_gateway_cust_and_var1 = inner_join(
    join_var1_and_gateway_cust, 
    dataframe_marketing_var2,
    join_var1_and_gateway_cust.affiliate_id == dataframe_marketing_var2.affiliate_id
    )
#inner_join_gateway_cust_data_with_var3
join_var3_with_gateway_cust_data = inner_join(
    dataframe_db_gateway_cust_data,
    dataframe_marketing_var3,
    dataframe_db_gateway_cust_data.flow_type == dataframe_marketing_var3.flow_type
    )
#inner_join_gateway_cust_with_gateway_cust_data
join_all = inner_join(
    join_var2_with_gateway_cust_and_var1,
    join_var3_with_gateway_cust_data,
    join_var2_with_gateway_cust_and_var1.ref_cust_table_rid == join_var3_with_gateway_cust_data.ref_cust_table_rid
    )
#add column - ip2
join_all = join_all.withColumn(
    "ip2",
    ip2_udf(join_all.remoteip)
    )
#add column - ip3
join_all = join_all.withColumn(
    "ip3",
    ip3_udf(join_all.remoteip)
    )
#add column - connection_type
join_all = join_all.withColumn(
    "connection_type",
    connection_type_udf(join_all.carrier)
    )
#add column - sales_count
join_all = join_all.withColumn(
    "sales_count",
    F.lit(1)
    )
#add column - resub
join_all = join_all.withColumn(
    "resub",
    F.lit(0)
    )
#add column - free_sales_count
join_all = join_all.withColumn(
    "free_sales_count",
    free_sales_count_udf(join_all.offerid)
    )
#add_column - update_year
join_all = join_all.withColumn(
    "update_year",
    F.lit(current_year)
    )
#add column - update_month
join_all = join_all.withColumn(
    "update_month",
    F.lit(current_month)
    )
#add column - datediff
join_all = join_all.withColumn(
    "datediff",
    F.datediff(F.col("subscribetime"), F.col("unsubscribetime"))
    )
#add column - optout24                                                                
join_all = join_all.withColumn(
    "optout24",
    optout24_udf(join_all.datediff)
    )
#add column - country
join_all = join_all.withColumn(
    "country",
    F.lit(country)
    )
#add column - gateway
join_all = join_all.withColumn(
    "gateway",
    F.lit(gateway)
    )
#add column - first_billing_count
join_all = join_all.withColumn("first_billing_count", F.when(count_db_gateway_info > 0, 1).otherwise(0))

#add column - free_pixels_count
join_all = join_all.withColumn(
    "free_pixels_count",
    F.when((F.col("offerid") != "") & (F.col("offer_price") == 0.00), 1).otherwise(0)
    )

#add column - pixels_count
join_all = join_all.withColumn(
    "pixels_count",
    F.when((F.col("offerid") != "") & (F.col("offer_price") != 0.00), 1).otherwise(0)
    )

#push to redshift
push_to_redshift(join_all)

#job commit
job.commit()
