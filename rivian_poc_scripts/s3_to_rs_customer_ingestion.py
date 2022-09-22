import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://silver-data-bucket/customer_table_null.csv"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("ACCOUNT_NO", "string", "ACCOUNT_NO", "string"),
        ("NAME", "string", "NAME", "string"),
        ("AGE", "string", "AGE", "string"),
        ("COUNTRY", "string", "COUNTRY", "string"),
        ("GENDER", "string", "GENDER", "string"),
        ("ACCOUNT_STATUS", "string", "ACCOUNT_STATUS", "string"),
        ("LAST_UPDATED_DATE", "string", "LAST_UPDATED_DATE", "string"),
        ("CREATED_DATE", "string", "CREATED_DATE", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="raw",
    table_name="dev_public_customer",
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="S3bucket_node3",
)

job.commit()
