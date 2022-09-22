import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import lit,col

## @params: [JOB_NAME]
#args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

if ('--{}'.format('data_date') in sys.argv):
    args = getResolvedOptions(sys.argv, ['data_date'])
    data_date = args.get("data_date")
    
    c_df = glueContext.create_dynamic_frame.from_catalog(database = "raw", table_name = "dev_public_customer", redshift_tmp_dir = args["TempDir"], transformation_ctx = "c_df")
        
    customer_df=c_df.toDF().where(col("insert_date")==data_date)#2022-09-22
    print("customer_df data for date 2022-09-22...............................................................",data_date)
    customer_df.show()
    print("schema of customer_df...........................................................................................")
    customer_df.printSchema()
    
    dynamic_transform_df=DynamicFrame.fromDF(customer_df, glueContext, "dynamic_transformdf")
    
    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dynamic_transform_df, catalog_connection = "redshift_ cluster1", connection_options = {"dbtable": "customer_temp", "database": "dev"}, redshift_tmp_dir ="s3://silver-data-bucket/temp/" , transformation_ctx = "datasink4")
    
    #cs_df = glueContext.create_dynamic_frame.from_catalog(database = "raw", table_name = "dev_public_customer_snapshot", redshift_tmp_dir = args["TempDir"], transformation_ctx = "cs_df")
    
    #customer_snapshot_df=cs_df.toDF()
    #.where(col("insert_date")==data_date)
     
    
    #match_df=c.join(cs_df,c.account_no == cs_df.account_no, 'leftsemi')
    #unmatch_df=cs_df.join(c,cs_df.account_no == c.account_no, 'leftanti')
    #updated_df = match_df.withColumn("status", lit("updated"))
    #unmatch_df.printSchema()
    #updated_df.printSchema()
    #final_df=unmatch_df.union(updated_df)
    #dynamic_transform_df=DynamicFrame.fromDF(final_df, glueContext, "dynamic_transformdf")
    
    #post_query="begin;delete from target_table using stage_table where stage_table.id = target_table.id ; insert into target_table select * from stage_table; drop table stage_table; end;"
    
    #datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = datasource0, catalog_connection = "test_red", connection_options = {"preactions":"drop table if exists
    #stage_table;create table stage_table as select * from target_table where 1=2;","dbtable": "stage_table", "database": "redshiftdb","postactions":post_query},
    #redshift_tmp_dir = 's3://s3path', transformation_ctx = "datasink4")
    
    #datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dynamic_transform_df, catalog_connection = "redshift_ cluster1", connection_options = {"dbtable": "customer_snapshot", "database": "dev"}, redshift_tmp_dir ="s3://silver-data-bucket/temp/" , transformation_ctx = "datasink4")
job.commit()