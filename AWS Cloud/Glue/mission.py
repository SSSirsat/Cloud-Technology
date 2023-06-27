import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import Window
import pyspark.sql.functions as f
from awsglue import DynamicFrame

from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, max

from pyspark.conf import SparkConf

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'iceberg_job_catalog_warehouse'])
conf = SparkConf()

## Please make sure to pass runtime argument --iceberg_job_catalog_warehouse with value as the S3 path 
conf.set("spark.sql.catalog.job_catalog.warehouse", args['iceberg_job_catalog_warehouse'])
conf.set("spark.sql.catalog.job_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.job_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("spark.sql.catalog.job_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
conf.set("spark.sql.iceberg.handle-timestamp-without-timezone","true")

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# Step 1)
# Reading data from CDC S3 location 
# Creating DynamicFrame from that data inside Glue Job.

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
        "paths": ["s3://cdc-aurora-2-s3-target-bucket/data/visa_admin/mission/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)
# Step 2)
# Script generated for node ApplyMapping
# Here we are updating our columns data type as per need (ex, string -> to -> timestamp)

ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("id", "string", "id", "bigint"),
        ("created_time", "string", "created_time", "timestamp"),
        ("created_user", "string", "created_user", "string"),
        ("updated_time", "string", "updated_time", "timestamp"),
        ("updated_user", "string", "updated_user", "string"),
        ("country_code", "string", "country_code", "string"),
        ("mission_address", "string", "mission_address", "string"),
        ("mission_name", "string", "mission_name", "string"),
        ("region", "string", "region", "int"),
        ("state", "string", "state", "string"),
        ("task_order_id", "string", "task_order_id", "int"),
        ("time_zone", "string", "time_zone", "string"),
        ("country", "string", "country", "int"),
        ("clonable", "string", "clonable", "boolean"),
        ("status", "string", "status", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

IncrementalInputDF = ApplyMapping_node2.toDF()

if not IncrementalInputDF.rdd.isEmpty():
    # ## Apply De-duplication logic on input data, to pickup latest record based on timestamp and operation 
    IDWindowDF = Window.partitionBy(IncrementalInputDF.id).orderBy(IncrementalInputDF.updated_time).rangeBetween(-sys.maxsize, sys.maxsize)
                  
    # Add new columns to capture first and last OP value and what is the latest timestamp
    inputDFWithTS= IncrementalInputDF.withColumn("max_op_date",max(IncrementalInputDF.updated_time).over(IDWindowDF))
    
    # Filter out new records that are inserted, then select latest record from existing records and merge both to get deduplicated output 
    NewInsertsDF = inputDFWithTS.filter("updated_time=max_op_date")
    
    newdy = DynamicFrame.fromDF(NewInsertsDF, glueContext, 'newdy')

    tempDF = newdy.toDF()
    w = Window.partitionBy("id").orderBy(f.desc("updated_time"))
    df = tempDF.withColumn("rn", f.row_number().over(w)).filter("rn = 1").drop("rn")

    # Register the deduplicated input as temporary table to use in Iceberg Spark SQL statements
    df.createOrReplaceTempView("incremental_input_data")
    df.show()
    
    ## Perform merge operation on incremental input data with MERGE INTO. This section of the code uses Spark SQL to showcase the expressive SQL approach of Iceberg to perform a Merge operation
    IcebergMergeOutputDF = spark.sql("""
    MERGE INTO glue_catalog.source_tables.mission t
    USING (SELECT id,created_time,created_user,updated_time,updated_user,country_code,mission_address,mission_name,region,state,task_order_id,time_zone,country,clonable,status
    FROM incremental_input_data) as s 
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET t.created_time=s.created_time,t.created_user=s.created_user,t.updated_time=s.updated_time,t.updated_user=s.updated_user,t.country_code=s.country_code,t.mission_address=s.mission_address,t.mission_name=s.mission_name,t.region=s.region,t.state=s.state,t.task_order_id=s.task_order_id,t.time_zone=s.time_zone,t.country=s.country,t.clonable=s.clonable,t.status=s.status
    WHEN NOT MATCHED THEN INSERT (id,created_time,created_user,updated_time,updated_user,country_code,mission_address,mission_name,region,state,task_order_id,time_zone,country,clonable,status) 
	VALUES (s.id,s.created_time,s.created_user,s.updated_time,s.updated_user,s.country_code,s.mission_address,s.mission_name,s.region,s.state,s.task_order_id,s.time_zone,s.country,s.clonable,s.status)
    """)

    job.commit()