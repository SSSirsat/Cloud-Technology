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
        ("inc_id","string ","inc_id","bigint ")
        ("inc_cnt_id_acknowledged_by","string ","inc_cnt_id_acknowledged_by","string ")
        ("inc_affected_end_user","string ","inc_affected_end_user","string ")
        ("applicant_full_name","string ","applicant_full_name","string ")
        ("inc_applicant_id","string ","inc_applicant_id","string ")
        ("application_id","string ","application_id","string ")
        ("appointment_id","string ","appointment_id","bigint ")
        ("assign_type","string ","assign_type","string ")
        ("inc_assigned_to","string ","inc_assigned_to","string ")
        ("inc_callback_method","string ","inc_callback_method","string ")
        ("inc_change_type","string ","inc_change_type","string ")
        ("customer_id","string ","customer_id","string ")
        ("inc_date_created","string ","inc_date_created","date ")
        ("inc_department","string ","inc_department","string ")
        ("inc_descr","string ","inc_descr","string ")
        ("dossier_number","string ","dossier_number","string ")
        ("ds_160_number","string ","ds_160_number","string ")
        ("field_json","string ","field_json","string ")
        ("inc_category","string ","inc_category","string ")
        ("inc_subcategory","string ","inc_subcategory","string ")
        ("incident_display_id","string ","incident_display_id","string ")
        ("inc_incoming_type","string ","inc_incoming_type","string ")
        ("last_modified","string ","last_modified","timestamp ")
        ("last_modified_by","string ","last_modified_by","string ")
        ("mission_id","string ","mission_id","int ")
        ("next_sla_id","string ","next_sla_id","string ")
        ("inc_cnt_id_opened_by","string ","inc_cnt_id_opened_by","string ")
        ("parent_sla_id","string ","parent_sla_id","string ")
        ("post_user_id","string ""post_user_id","bigint ")
        ("inc_priority","string ","inc_priority","string ")
        ("inc_record_status","string ","inc_record_status","string ")
        ("inc_resolved_date","date ","inc_resolved_date","date ")
        ("inc_resolved_time","date ","inc_resolved_time","date ")
        ("inc_resolved_time_taken","string ","inc_resolved_time_taken","bigint ")
        ("inc_response_time","string ","inc_response_time","bigint ")
        ("role_id","string ","role_id","bigint ")
        ("inc_severity","string ","inc_severity","string ")
        ("inc_severity","string ","inc_severity","string ")
        ("inc_sla_id","string ","inc_sla_id","bigint ")
        ("sla_value","string ","sla_value","string ")
        ("inc_status","string ","inc_status","string ")
        ("inc_status_1","string ","inc_status_1","string ")
        ("inc_status_2","string ","inc_status_2","string ")
        ("inc_status_3","string ","inc_status_3","string ")
        ("inc_summary","string ","inc_summary","string ")
        ("user_id","string ","user_id","bigint ")
        ("user_role","string","user_role","string")

    ],
    transformation_ctx="ApplyMapping_node2",
)

IncrementalInputDF = ApplyMapping_node2.toDF() 

if not IncrementalInputDF.rdd.isEmpty():
    # ## Apply De-duplication logic on input data, to pickup latest record based on timestamp and operation 
    IDWindowDF = Window.partitionBy(IncrementalInputDF.id).orderBy(f.desc(IncrementalInputDF.updated_time))
                  
    # Add new columns to capture first and last OP value and what is the latest timestamp
    inputDFWithTS= IncrementalInputDF.withColumn("max_op_date",max(IncrementalInputDF.updated_time).over(IDWindowDF))
  
    # Filter out new records that are inserted, then select latest record from existing records and merge both to get deduplicated output 
    NewInsertsDF = inputDFWithTS.filter("updated_time=max_op_date").filter("op IN ('I','U')")
    NewInsertsDF1 = NewInsertsDF.withColumn("rn", f.row_number().over(IDWindowDF)).filter("rn = 1").drop("rn")
    NewInsertsDF1.createOrReplaceTempView("incremental_input_data1")
    NewInsertsDF1.show()
    
    IcebergMergeOutputDF = spark.sql("""
    MERGE INTO glue_catalog.iceberg_database_partition.visa_fee as t
    USING (SELECT op,id,created_time,created_user,updated_time,updated_user,amount,created_by,created_date,visa_category_id,visa_class_code,task_order_id,mission_id,status,visa_class_id,tier
    FROM incremental_input_data1) as s 
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET t.created_time=s.created_time,t.created_user=s.created_user,t.updated_time=s.updated_time,t.updated_user=s.updated_user,t.amount=s.amount,t.created_by=s.created_by,t.created_date=s.created_date,t.visa_category_id=s.visa_category_id,t.visa_class_code=s.visa_class_code,t.task_order_id=s.task_order_id,t.mission_id=s.mission_id,t.status=s.status,t.visa_class_id=s.visa_class_id,t.tier=s.tier

    WHEN NOT MATCHED THEN INSERT (id,created_time,created_user,updated_time,updated_user,amount,created_by,created_date,visa_category_id,visa_class_code,task_order_id,mission_id,status,visa_class_id,tier) 
	VALUES (s.id,s.created_time,s.created_user,s.updated_time,s.updated_user,s.amount,s.created_by,s.created_date,s.visa_category_id,s.visa_class_code,s.task_order_id,s.mission_id,s.status,s.visa_class_id,s.tier)
    """)

    # Filter out Deleted records that are inserted, then select latest record from existing records and merge both to get deduplicated output 
    DeleteDf = inputDFWithTS.filter("updated_time=max_op_date").filter("op='D'")
    DeleteDf1 = DeleteDf.withColumn("rn", f.row_number().over(IDWindowDF)).filter("rn = 1").drop("rn")
    DeleteDf1.createOrReplaceTempView("incremental_input_data2")
    DeleteDf1.show()
    ## Perform merge operation on incremental input data with Delete flag
    
    IcebergMergeOutputDF = spark.sql("""
    MERGE INTO glue_catalog.iceberg_database_partition.visa_fee as t
    USING (SELECT op,id,created_time,created_user,updated_time,updated_user,amount,created_by,created_date,visa_category_id,visa_class_code,task_order_id,mission_id,status,visa_class_id,tier
    FROM incremental_input_data2) as s 
    ON t.id = s.id
    WHEN MATCHED AND s.op = 'D' THEN DELETE
    """)
    job.commit()

print(current_timestamp())