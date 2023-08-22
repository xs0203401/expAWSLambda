import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.utils import AnalysisException

import boto3
import json

s3_client = boto3.client('s3')

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    "new_object_key",
    "new_bucket_name"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

TARGET_BUCKET = 'my-table-0821'
TARGET_FILE_KEY = 'output.parquet'

logger = glueContext.get_logger()


logger.info("args::::" + str(args))

bucket_str = args['new_bucket_name']
bucket_json = json.loads(bucket_str)
bucket_name = bucket_json['name']
logger.info("bucket::::" + str(bucket_name))
object_str = args['new_object_key']
object_json = json.loads(object_str)
object_key = object_json['key']
object_key_category = object_key.split("_")[0]
logger.info("object::::" + str(object_key) + " | object_category::::" + str(object_key_category))

s3_file_path = f"s3a://{bucket_name}/{object_key}"
df_new = spark.read.csv(s3_file_path, header=True, inferSchema=True)

df_new = df_new.withColumn("language_category", lit(object_key_category))
df_new = df_new.withColumn("ingest_datetime", current_timestamp())
df_new = df_new.select([col(col_name) for col_name in df_new.columns if col_name != '_c0'])
df_new.createTempView('df_new')

# Except existing rows
# res = s3_client.list_objects(Bucket=TARGET_BUCKET)
# if (res.get("Contents", -1) != -1):
try:
    df_old = spark.read.parquet(f"s3a://{TARGET_BUCKET}/{TARGET_FILE_KEY}")
    df_new = df_old.union(df_new)
    # logger.info("inside_try::::df_old.count()::::" + str(df_old.count()))
    # logger.info("inside_try::::df_new.count()::::" + str(df_new.count()))
    df_old = df_new
except AnalysisException:
    logger.info("first_load::::Continue")

df_new.show(5) # Execute
logger.info("before_write::::df_new.count()::::" + str(df_new.count()))
df_new.write.parquet(f"s3a://{TARGET_BUCKET}/{TARGET_FILE_KEY}", mode="overwrite", compression="snappy")


spark.stop()
job.commit()
