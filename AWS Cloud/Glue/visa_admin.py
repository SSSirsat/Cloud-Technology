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
        "paths": ["s3://cdc-aurora-2-s3-target-bucket/data/visa_admin/visa_fee/"],
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
        ("op", "string", "op", "string"),
        ("id", "string", "id", "bigint"),
        ("created_time", "string", "created_time", "timestamp"),
        ("created_user", "string", "created_user", "string"),
        ("updated_time", "string", "updated_time", "timestamp"),
        ("updated_user", "string", "updated_user", "string"),
        ("amount", "string", "amount", "int"),
        ("created_by", "string", "created_by", "string"),
        ("created_date", "string", "created_date", "timestamp"),
        ("visa_category_id", "string", "visa_category_id", "int"),
        ("visa_class_code", "string", "visa_class_code", "string"),
        ("task_order_id", "string", "task_order_id", "bigint"),
        ("mission_id", "string", "mission_id", "bigint"),
        ("status", "string", "status", "string"),
        ("visa_class_id", "string", "visa_class_id", "int"),
        ("tier", "string", "tier", "string"),
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
    NewInsertsDF = inputDFWithTS.filter("updated_time=max_op_date").filter("op='I'")
    UpdateDeleteDf = inputDFWithTS.filter("updated_time=max_op_date").filter("op IN ('U','D')")
    finalInputDF = NewInsertsDF.unionAll(UpdateDeleteDf)

    # newdy = DynamicFrame.fromDF(NewInsertsDF, glueContext, 'newdy')
    # tempDF = newdy.toDF()
    # w = Window.partitionBy("id").orderBy(f.desc("updated_time"))
    # df = tempDF.withColumn("rn", f.row_number().over(w)).filter("rn = 1").drop("rn")

    # Register the deduplicated input as temporary table to use in Iceberg Spark SQL statements
    finalInputDF.createOrReplaceTempView("incremental_input_data")
    finalInputDF.show()
    
    ## Perform merge operation on incremental input data with MERGE INTO. This section of the code uses Spark SQL to showcase the expressive SQL approach of Iceberg to perform a Merge operation
    IcebergMergeOutputDF = spark.sql("""
    MERGE INTO glue_catalog.iceberg_database_partition.visa_fee as t
    USING (SELECT op,id,created_time,created_user,updated_time,updated_user,amount,created_by,created_date,visa_category_id,visa_class_code,task_order_id,mission_id,status,visa_class_id,tier
    FROM incremental_input_data) as s 
    ON t.id = s.id
    WHEN MATCHED AND s.op = 'D' THEN DELETE
    WHEN MATCHED THEN UPDATE SET t.created_time=s.created_time,t.created_user=s.created_user,t.updated_time=s.updated_time,t.updated_user=s.updated_user,t.amount=s.amount,t.created_by=s.created_by,t.created_date=s.created_date,t.visa_category_id=s.visa_category_id,t.visa_class_code=s.visa_class_code,t.task_order_id=s.task_order_id,t.mission_id=s.mission_id,t.status=s.status,t.visa_class_id=s.visa_class_id,t.tier=s.tier

    WHEN NOT MATCHED THEN INSERT (id,created_time,created_user,updated_time,updated_user,amount,created_by,created_date,visa_category_id,visa_class_code,task_order_id,mission_id,status,visa_class_id,tier) 
	VALUES (s.id,s.created_time,s.created_user,s.updated_time,s.updated_user,s.amount,s.created_by,s.created_date,s.visa_category_id,s.visa_class_code,s.task_order_id,s.mission_id,s.status,s.visa_class_id,s.tier)
    """)

    job.commit()

    