import sys
from math import ceil
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StringType, IntegerType, TimestampType, DoubleType, DateType
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import unix_timestamp, from_unixtime, datediff, col, udf, concat, lit, hour, when, sum, count, length, count

#default prebuilt id
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#tic_tbl
tic_tbl_datasource = glueContext.create_dynamic_frame.from_catalog(database = "client_database_parquet", table_name = "tic_tbl", transformation_ctx = "tic_tbl_datasource")
tic_tbl_df = tic_tbl_datasource.toDF()
tic_tbl_df = tic_tbl_df.where(col("tic_booking_id").isNotNull())
tic_tbl_df = tic_tbl_df.withColumn("tic_hall_id", tic_tbl_df["tic_hall_id"].cast(IntegerType()))

#refund_tbl
refund_tbl_data = glueContext.create_dynamic_frame.from_catalog(database = "client_database_parquet", table_name = "refund_tbl", transformation_ctx = "refund_tbl_data")
refund_tbl_df = refund_tbl_data.toDF()
refund_tbl_df = refund_tbl_df.filter((col("refund_booking_id") != "") | (col("refund_booking_id").isNotNull()))
refund_tbl_df = refund_tbl_df.withColumn("refund_booking_id", col("refund_booking_id").cast(DoubleType()))

#tic left join refund_tbl
tic_tbl_df = tic_tbl_df.join(
    refund_tbl_df, 
    (tic_tbl_df.tic_booking_id == refund_tbl_df.refund_booking_id) & 
    (tic_tbl_df.tic_col_id == refund_tbl_df.refund_col_id) & 
    (tic_tbl_df.tic_row_id == refund_tbl_df.refund_row_id), how='left')
    
tic_tbl_df = tic_tbl_df.filter((col("refund_booking_id") == "") | (col("refund_booking_id").isNull()))

#location_hall
location_hall_datasource = glueContext.create_dynamic_frame.from_catalog(database = "client_database_parquet", table_name = "location_hall", transformation_ctx = "tic_tbl_datasource")
location_hall_df = location_hall_datasource.toDF()
tic_tbl_df = tic_tbl_df.join(location_hall_df, (tic_tbl_df.tic_hall_id == location_hall_df.hall_hall_id) & (tic_tbl_df.tic_loc_id == location_hall_df.hall_loca_id), how='left')

#halltype
halltype_datasource = glueContext.create_dynamic_frame.from_catalog(database = "client_database_parquet", table_name = "halltype", transformation_ctx = "tic_tbl_datasource")
halltype_df = halltype_datasource.toDF()
tic_tbl_df = tic_tbl_df.join(halltype_df, tic_tbl_df.halltype == halltype_df.halltype_id, how='left')

#timetable
timetable_datasource = glueContext.create_dynamic_frame.from_catalog(database = "client_database_parquet", table_name = "timetable", transformation_ctx = "timetable_datasource")
timetable_df = timetable_datasource.toDF()
timetable_df = timetable_df.where(col("showid").isNotNull())
str_to_date = udf(lambda x: datetime.strptime(x[:26], '%Y-%m-%d %H:%M:%S.%f') if x != '' else None, DateType())
timetable_df = timetable_df.withColumn("showdate", str_to_date(col("showdate")))

#make sure data type match with each other
tic_tbl_df = tic_tbl_df.withColumn("tic_show_id", tic_tbl_df["tic_show_id"].cast(IntegerType()))\
    .withColumn("tic_show_time", tic_tbl_df["tic_show_time"].cast(IntegerType()))\
    .withColumn("tic_film_id", tic_tbl_df["tic_film_id"].cast(IntegerType()))

join_conditions = [
    timetable_df.showid == tic_tbl_df.tic_show_id, 
    timetable_df.locid == tic_tbl_df.tic_loc_id, 
    timetable_df.showdate == tic_tbl_df.tic_show_date, 
    timetable_df.showtime == tic_tbl_df.tic_show_time, 
    timetable_df.filmid == tic_tbl_df.tic_film_id
]
tic_tbl_df = tic_tbl_df.join(timetable_df, join_conditions, how='inner')

#only need to rename
str_to_date = udf(lambda x: datetime.strptime(x[:26], '%d/%m/%Y') if x != '' and '/' in x else datetime.strptime(x[:26], '%Y-%m-%d') if x != '' and '-' in x else None, DateType())
tic_tbl_df = tic_tbl_df.withColumn("movie date", 
    when(col("tic_bus_date").like("%/%/%"), str_to_date(col("tic_bus_date")))\
    .when(col("tic_bus_date").like("%-%-%"), str_to_date(col("tic_bus_date")))\
    .otherwise(lit(None)))

tic_tbl_df = tic_tbl_df.withColumnRenamed("tic_loc_id", "location id")
tic_tbl_df = tic_tbl_df.withColumnRenamed("hall_hall_id", "hall_no")
tic_tbl_df = tic_tbl_df.withColumnRenamed("halltype_name", "hall_type")

"""
new_tic_tbl_byday
- movie date, location id, tic_film_id, tic_hall_id, hall_type, nbo, admission
"""
new_tic_tbl_byday_df = tic_tbl_df.groupBy("movie date","location id","tic_film_id","tic_hall_id","hall_type").agg(
    sum(col("tic_tkt_amt")-col("tic_tkt_gst_amt")-col("tic_tkt_ent_tax")+col("tic_tkt_surc_amt")-col("tic_tkt_surc_gst_amt")).alias("NBO"),
    count(col("tic_booking_id")).alias("admission")
)

new_tic_tbl_byday = DynamicFrame.fromDF(new_tic_tbl_byday_df, glueContext, "nested")

datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = new_tic_tbl_byday, catalog_connection = "redshift_connector", connection_options = {"dbtable": "new_tic_tbl_byday", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink4")

"""
new_tic_tbl_hourly
- movie date, location id, tic_film_id, tic_hall_id, hall_type, nbo, admission, tic_show_time
"""
new_tic_tbl_hourly_df = tic_tbl_df.groupBy("movie date","location id","tic_film_id","tic_hall_id","hall_type","tic_show_time").agg(
    sum(col("tic_tkt_amt")-col("tic_tkt_gst_amt")-col("tic_tkt_ent_tax")+col("tic_tkt_surc_amt")-col("tic_tkt_surc_gst_amt")).alias("NBO"),
    count(col("tic_booking_id")).alias("admission")
)

new_tic_tbl_hourly = DynamicFrame.fromDF(new_tic_tbl_hourly_df, glueContext, "nested")
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = new_tic_tbl_hourly, catalog_connection = "redshift_connector", connection_options = {"dbtable": "new_tic_tbl_hourly", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink4")

"""
new_showtime_hourly
- movie date, movie id, location id, showtime, hourly sales by showtime, showtime_grp, hall_no, hall_type, no of show, total seats
"""
tic_tbl_df = tic_tbl_df.withColumnRenamed("showtime", "showtime")
tic_tbl_df = tic_tbl_df.withColumnRenamed("tic_film_id", "movie id")

hourlysales_grouping = udf(lambda x: "0"+x[:len(x)-2]+":00AM" if int(x[:len(x)-2]) < 12 else str(int(x[:len(x)-2])-12)+":00PM", StringType())

tic_tbl_df = tic_tbl_df.withColumn("showtime_grp", 
    when((col("showtime") >= 2300) | (col("showtime") <= 559), lit("Midnight (10pm onwards)"))\
    .when((col("showtime") >= 600) | (col("showtime") <= 1259), lit("Early (before 1pm)"))\
    .when((col("showtime") >= 1300) | (col("showtime") <= 1759), lit("Afternoon (1pm - 5:59pm)"))\
    .when((col("showtime") >= 1800) | (col("showtime") <= 2159), lit("Evening (6pm - 9:59pm)"))\
    .otherwise(lit(None))
    )

tic_tbl_df = tic_tbl_df.withColumn("hourly sales by showtime", hourlysales_grouping(col("showtime")))
new_showtime_hourly_df = tic_tbl_df.groupBy("movie date","movie id","location id","showtime","showtime_grp","hall_no","hall_type").agg(
    sum(col("totalseats")).alias("total seats"),
    count(col("showid")).alias("no of show")
)

new_showtime_hourly = DynamicFrame.fromDF(new_showtime_hourly_df, glueContext, "nested")
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = new_showtime_hourly, catalog_connection = "redshift_connector", connection_options = {"dbtable": "new_showtime_hourly", "database": "development"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink4")


job.commit()