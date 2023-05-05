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

# Script generated for node Amazon S3
AmazonS3_node1680763254156 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://cdc-aurora-2-s3-target-bucket/data/visa_application/doc_delivery_tracking/"
        ],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1680763254156",
)

# Script generated for node Amazon S3
AmazonS3_node1681291913232 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://cdc-aurora-2-s3-target-bucket/data/visa_application/chat/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1681291913232",
)

# Script generated for node Amazon S3
AmazonS3_node1681292717594 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://cdc-aurora-2-s3-target-bucket/data/visa_application/file_transfer_verification/"
        ],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1681292717594",
)

# Script generated for node Amazon S3
AmazonS3_node1681291357570 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://cdc-aurora-2-s3-target-bucket/data/visa_application/appointment_process/"
        ],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1681291357570",
)

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
        "paths": [
            "s3://cdc-aurora-2-s3-target-bucket/data/visa_application/doc_delivery/"
        ],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1681292335377 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://cdc-aurora-2-s3-target-bucket/data/visa_application/file_transfer/"
        ],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1681292335377",
)

# Script generated for node Amazon S3
AmazonS3_node1681291574769 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://cdc-aurora-2-s3-target-bucket/data/visa_application/call_log/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1681291574769",
)

# Script generated for node Amazon S3
AmazonS3_node1681291132257 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://cdc-aurora-2-s3-target-bucket/data/visa_application/applicant_association/"
        ],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1681291132257",
)

# Script generated for node Amazon S3
AmazonS3_node1681292193987 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://cdc-aurora-2-s3-target-bucket/data/visa_application/fields/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1681292193987",
)

# Script generated for node Change Schema
ChangeSchema_node1680763284906 = ApplyMapping.apply(
    frame=AmazonS3_node1680763254156,
    mappings=[
        ("delivery_tracking_id", "string", "delivery_tracking_id", "string"),
        ("applicant_to_courier_dt", "string", "applicant_to_courier_dt", "string"),
        ("courier_company_name", "string", "courier_company_name", "string"),
        ("courier_post_cd", "string", "courier_post_cd", "string"),
        ("courier_to_applicant_dt", "string", "courier_to_applicant_dt", "string"),
        ("courier_to_post_dt", "string", "courier_to_post_dt", "string"),
        ("courier_tracking_num", "string", "courier_tracking_num", "string"),
        ("doc_arrival_location", "string", "doc_arrival_location", "string"),
        ("doc_tracking_comments", "string", "doc_tracking_comments", "string"),
        ("document_reported_lost_dt", "string", "document_reported_lost_dt", "string"),
        ("post_to_courier_dt", "string", "post_to_courier_dt", "string"),
        ("return_to_post_dt", "string", "return_to_post_dt", "string"),
        (
            "vendor_doc_del_tracking_id",
            "string",
            "vendor_doc_del_tracking_id",
            "string",
        ),
        ("id", "string", "id", "string"),
    ],
    transformation_ctx="ChangeSchema_node1680763284906",
)

# Script generated for node Change Schema
ChangeSchema_node1681291921122 = ApplyMapping.apply(
    frame=AmazonS3_node1681291913232,
    mappings=[
        ("id", "string", "id", "bigint"),
        ("created_time", "string", "created_time", "timestamp"),
        ("created_user", "string", "created_user", "varchar"),
        ("updated_time", "string", "updated_time", "timestamp"),
        ("updated_user", "string", "updated_user", "varchar"),
        ("api_agent_id", "string", "api_agent_id", "varchar"),
        ("call_system_id", "string", "call_system_id", "bigint"),
        ("campaign_id", "string", "campaign_id", "varchar"),
        ("chat_comments", "string", "chat_comments", "varchar"),
        ("call_end_time", "string", "call_end_time", "varchar"),
        ("chat_reason", "string", "chat_reason", "varchar"),
        ("chat_resolution_state", "string", "chat_resolution_state", "varchar"),
        ("chat_service_provider", "string", "chat_service_provider", "varchar"),
        ("chat_start_time", "string", "chat_start_time", "varchar"),
        ("country", "string", "country", "varchar"),
        ("crt_object_id", "string", "crt_object_id", "varchar"),
        ("disposition", "string", "disposition", "varchar"),
        ("ds160number", "string", "ds160number", "varchar"),
        ("from_contact_no", "string", "from_contact_no", "varchar"),
        ("incident_id", "string", "incident_id", "bigint"),
        ("post", "string", "post", "varchar"),
        ("status", "string", "status", "varchar"),
        ("sub_disposition", "string", "sub_disposition", "varchar"),
        ("to_contact_no", "string", "to_contact_no", "varchar"),
        ("url", "string", "url", "varchar"),
    ],
    transformation_ctx="ChangeSchema_node1681291921122",
)

# Script generated for node Change Schema
ChangeSchema_node1681292719995 = ApplyMapping.apply(
    frame=AmazonS3_node1681292717594,
    mappings=[
        ("id", "string", "id", "bigint"),
        ("created_time", "string", "created_time", "timestamp"),
        ("created_user", "string", "created_user", "varchar"),
        ("updated_time", "string", "updated_time", "timestamp"),
        ("updated_user", "string", "updated_user", "varchar"),
        ("message", "string", "message", "varchar"),
        ("passed", "string", "passed", "varchar"),
        ("rule_id", "string", "rule_id", "binary"),
        ("rule_name", "string", "rule_name", "varchar"),
        ("sub_error_message", "string", "sub_error_message", "varchar"),
        ("table_name", "string", "table_name", "varchar"),
        ("file_transfer_id", "string", "file_transfer_id", "bigint"),
    ],
    transformation_ctx="ChangeSchema_node1681292719995",
)

# Script generated for node Change Schema
ChangeSchema_node1681291359170 = ApplyMapping.apply(
    frame=AmazonS3_node1681291357570,
    mappings=[
        ("appointment_process_id", "string", "appointment_process_id", "int"),
        ("check_in", "string", "check_in", "timestamp"),
        ("end_time", "string", "end_time", "timestamp"),
        ("start_time", "string", "start_time", "timestamp"),
        ("user_login_id", "string", "user_login_id", "varchar"),
        ("appointment_id", "string", "appointment_id", "int"),
        ("counter_id", "string", "counter_id", "int"),
        ("greeter_comments", "string", "greeter_comments", "varchar"),
    ],
    transformation_ctx="ChangeSchema_node1681291359170",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("id", "string", "id", "string"),
        ("addr_doc_delivery_city", "string", "addr_doc_delivery_city", "bigint"),
        ("addr_doc_delivery_country", "string", "addr_doc_delivery_country", "varchar"),
        ("addr_doc_delivery_line2", "string", "addr_doc_delivery_line2", "varchar"),
        ("addr_doc_delivery_line3", "string", "addr_doc_delivery_line3", "varchar"),
        (
            "addr_doc_delivery_postal_code",
            "string",
            "addr_doc_delivery_postal_code",
            "varchar",
        ),
        ("addr_doc_delivery_state", "string", "addr_doc_delivery_state", "varchar"),
        ("addr_doc_delivery_line1", "string", "addr_doc_delivery_line1", "varchar"),
        ("created_date", "string", "created_date", "timestamp"),
        ("doc_delivery_service_type", "string", "doc_delivery_service_type", "varchar"),
        ("modified_by_id", "string", "modified_by_id", "varchar"),
        ("modified_date", "string", "modified_date", "varchar"),
        (
            "vendor_rec_doc_delivery_id",
            "string",
            "vendor_rec_doc_delivery_id",
            "varchar",
        ),
        ("created_by_id", "string", "created_by_id", "varchar"),
        ("applicant_id", "string", "applicant_id", "varchar"),
        ("application_id", "string", "application_id", "varchar"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Change Schema
ChangeSchema_node1681292338651 = ApplyMapping.apply(
    frame=AmazonS3_node1681292335377,
    mappings=[
        ("id", "string", "id", "bigint"),
        ("created_time", "string", "created_time", "timestamp"),
        ("created_user", "string", "created_user", "varchar"),
        ("updated_time", "string", "updated_time", "timestamp"),
        ("updated_user", "string", "updated_user", "varchar"),
        ("file_name", "string", "file_name", "varchar"),
        ("verified", "string", "verified", "varchar"),
        ("hash_code", "string", "hash_code", "int"),
        ("is_approved", "string", "is_approved", "boolean"),
        ("is_moved", "string", "is_moved", "boolean"),
        ("location", "string", "location", "varchar"),
        ("visa_type", "string", "visa_type", "varchar"),
        ("mission_id", "string", "mission_id", "int"),
        ("post_id", "string", "post_id", "int"),
        ("batch_id", "string", "batch_id", "varchar"),
    ],
    transformation_ctx="ChangeSchema_node1681292338651",
)

# Script generated for node Change Schema
ChangeSchema_node1681291576274 = ApplyMapping.apply(
    frame=AmazonS3_node1681291574769,
    mappings=[
        ("id", "string", "id", "bigint"),
        ("created_time", "string", "created_time", "timestamp"),
        ("created_user", "string", "created_user", "varchar"),
        ("updated_time", "string", "updated_time", "timestamp"),
        ("updated_user", "string", "updated_user", "varchar"),
        ("api_agent_id", "string", "api_agent_id", "varchar"),
        ("call_comments", "string", "call_comments", "varchar"),
        ("call_end_time", "string", "call_end_time", "varchar"),
        ("call_reason", "string", "call_reason", "varchar"),
        ("call_resolution_state", "string", "call_resolution_state", "varchar"),
        ("call_service_provider", "string", "call_service_provider", "varchar"),
        ("call_start_time", "string", "call_start_time", "varchar"),
        ("call_system_id", "string", "call_system_id", "bigint"),
        ("campaign_id", "string", "campaign_id", "varchar"),
        ("country", "string", "country", "varchar"),
        ("crt_object_id", "string", "crt_object_id", "varchar"),
        ("disposition", "string", "disposition", "varchar"),
        ("ds160number", "string", "ds160number", "varchar"),
        ("from_contact_no", "string", "from_contact_no", "varchar"),
        ("post", "string", "post", "varchar"),
        ("status", "string", "status", "varchar"),
        ("sub_disposition", "string", "sub_disposition", "varchar"),
        ("url", "string", "url", "varchar"),
        ("chat_comments", "string", "chat_comments", "varchar"),
        ("chat_reason", "string", "chat_reason", "varchar"),
        ("chat_resolution_state", "string", "chat_resolution_state", "varchar"),
        ("chat_service_provider", "string", "chat_service_provider", "varchar"),
        ("chat_start_time", "string", "chat_start_time", "varchar"),
        ("to_contact_no", "string", "to_contact_no", "varchar"),
        ("incident_id", "string", "incident_id", "bigint"),
    ],
    transformation_ctx="ChangeSchema_node1681291576274",
)

# Script generated for node Change Schema
ChangeSchema_node1681291133866 = ApplyMapping.apply(
    frame=AmazonS3_node1681291132257,
    mappings=[
        ("id", "string", "id", "bigint"),
        ("applicant_id", "string", "applicant_id", "varchar"),
        ("application_id", "string", "application_id", "varchar"),
        ("relation", "string", "relation", "varchar"),
        ("user_id", "string", "user_id", "varchar"),
    ],
    transformation_ctx="ChangeSchema_node1681291133866",
)

# Script generated for node Change Schema
ChangeSchema_node1681292196779 = ApplyMapping.apply(
    frame=AmazonS3_node1681292193987,
    mappings=[
        ("id", "string", "id", "bigint"),
        ("destination_name", "string", "destination_name", "varchar"),
        ("destination_type", "string", "destination_type", "varchar"),
        ("source_name", "string", "source_name", "varchar"),
        ("mapping_id", "string", "mapping_id", "bigint"),
    ],
    transformation_ctx="ChangeSchema_node1681292196779",
)

# Script generated for node Amazon S3
AmazonS3_node1680763464955 = glueContext.getSink(
    path="s3://cdc-parquet-tables-bucket/parquet_folder/data/visa_application/doc_delivery_tracking/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1680763464955",
)
AmazonS3_node1680763464955.setCatalogInfo(
    catalogDatabase="parquet_database", catalogTableName="doc_delivery_tracking"
)
AmazonS3_node1680763464955.setFormat("glueparquet")
AmazonS3_node1680763464955.writeFrame(ChangeSchema_node1680763284906)
# Script generated for node Amazon S3
AmazonS3_node1681291923186 = glueContext.getSink(
    path="s3://cdc-parquet-tables-bucket/parquet_folder/data/visa_application/chat/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1681291923186",
)
AmazonS3_node1681291923186.setCatalogInfo(
    catalogDatabase="parquet_database", catalogTableName="chat"
)
AmazonS3_node1681291923186.setFormat("glueparquet")
AmazonS3_node1681291923186.writeFrame(ChangeSchema_node1681291921122)
# Script generated for node Amazon S3
AmazonS3_node1681292722891 = glueContext.getSink(
    path="s3://cdc-parquet-tables-bucket/parquet_folder/data/visa_application/file_transfer_verification/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1681292722891",
)
AmazonS3_node1681292722891.setCatalogInfo(
    catalogDatabase="parquet_database", catalogTableName="file_transfer_verification"
)
AmazonS3_node1681292722891.setFormat("glueparquet")
AmazonS3_node1681292722891.writeFrame(ChangeSchema_node1681292719995)
# Script generated for node Amazon S3
AmazonS3_node1681291360602 = glueContext.getSink(
    path="s3://cdc-parquet-tables-bucket/parquet_folder/data/visa_application/appointment_process/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1681291360602",
)
AmazonS3_node1681291360602.setCatalogInfo(
    catalogDatabase="parquet_database", catalogTableName="appointment_process"
)
AmazonS3_node1681291360602.setFormat("glueparquet")
AmazonS3_node1681291360602.writeFrame(ChangeSchema_node1681291359170)
# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://cdc-parquet-tables-bucket/parquet_folder/data/visa_application/doc_delivery/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="parquet_database", catalogTableName="doc_delivery"
)
S3bucket_node3.setFormat("glueparquet")
S3bucket_node3.writeFrame(ApplyMapping_node2)
# Script generated for node Amazon S3
AmazonS3_node1681292341107 = glueContext.getSink(
    path="s3://cdc-parquet-tables-bucket/parquet_folder/data/visa_application/file_transfer/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1681292341107",
)
AmazonS3_node1681292341107.setCatalogInfo(
    catalogDatabase="parquet_database", catalogTableName="file_transfer"
)
AmazonS3_node1681292341107.setFormat("glueparquet")
AmazonS3_node1681292341107.writeFrame(ChangeSchema_node1681292338651)
# Script generated for node Amazon S3
AmazonS3_node1681291577987 = glueContext.getSink(
    path="s3://cdc-parquet-tables-bucket/parquet_folder/data/visa_application/call_log/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1681291577987",
)
AmazonS3_node1681291577987.setCatalogInfo(
    catalogDatabase="parquet_database", catalogTableName="call_log"
)
AmazonS3_node1681291577987.setFormat("glueparquet")
AmazonS3_node1681291577987.writeFrame(ChangeSchema_node1681291576274)
# Script generated for node Amazon S3
AmazonS3_node1681291138378 = glueContext.getSink(
    path="s3://cdc-parquet-tables-bucket/parquet_folder/data/visa_application/applicant_association/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1681291138378",
)
AmazonS3_node1681291138378.setCatalogInfo(
    catalogDatabase="parquet_database", catalogTableName="applicant_association"
)
AmazonS3_node1681291138378.setFormat("glueparquet")
AmazonS3_node1681291138378.writeFrame(ChangeSchema_node1681291133866)
# Script generated for node Amazon S3
AmazonS3_node1681292199018 = glueContext.getSink(
    path="s3://cdc-parquet-tables-bucket/parquet_folder/data/visa_application/fields/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1681292199018",
)
AmazonS3_node1681292199018.setCatalogInfo(
    catalogDatabase="parquet_database", catalogTableName="fields"
)
AmazonS3_node1681292199018.setFormat("glueparquet")
AmazonS3_node1681292199018.writeFrame(ChangeSchema_node1681292196779)
job.commit()
