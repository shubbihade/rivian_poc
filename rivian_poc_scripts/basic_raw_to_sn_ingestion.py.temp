import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import lit,col

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

if ('--{}'.format('data_date') in sys.argv):
    args = getResolvedOptions(sys.argv, ['data_date'])
    data_date = args.get("data_date")
    
    customer_df = glueContext.create_dynamic_frame.from_catalog(database = "raw", table_name = "dev_public_customer", redshift_tmp_dir = args["TempDir"], transformation_ctx = "customer_df")
        
    updated_df=customer_df.toDF().where(col("insert_date")==data_date).withColumn("status",lit('inserted'))
    updated_df.show()
    updated_df.printSchema()
    
    dynamic_transform_df=DynamicFrame.fromDF(updated_df, glueContext, "dynamic_transformdf")
    
    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dynamic_transform_df, catalog_connection = "redshift_ cluster1", connection_options = {"dbtable": "customer_snapshot", "database": "dev"}, redshift_tmp_dir ="s3://silver-data-bucket/temp/" , transformation_ctx = "datasink4")
    
job.commit()