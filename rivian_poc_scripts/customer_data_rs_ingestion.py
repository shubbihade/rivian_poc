import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame
 

## @params: [JOB_NAME]
#args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

if ('--{}'.format('data_date') in sys.argv):
    args = getResolvedOptions(sys.argv, ['data_date'])
    data_date = args.get("data_date")

    #logger.info(f"data_date is :  {data_date}")
    path_to_read = "s3://cust-data/" + data_date + "/*"
    df = spark.read.csv(path_to_read,header=True)
    
    transform_df = df.withColumn("insert_date",F.to_date(F.lit(data_date),'yyyy-MM-dd'))
    
    dynamic_transform_df=DynamicFrame.fromDF(transform_df, glueContext, "dynamic_transformdf")
    
    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dynamic_transform_df, catalog_connection = "redshift_ cluster1", connection_options = {"dbtable": "customer", "database": "dev"}, redshift_tmp_dir ="s3://silver-data-bucket/temp/" , transformation_ctx = "datasink4")
    
    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dynamic_transform_df, catalog_connection = "redshift_ cluster1", connection_options = {"dbtable": "customer_temp", "database": "dev"}, redshift_tmp_dir ="s3://silver-data-bucket/temp/" , transformation_ctx = "datasink4")
job.commit()