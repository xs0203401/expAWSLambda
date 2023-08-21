import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit, current_timestamp

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
logger = glueContext.get_logger()

logger.info("args::::" + str(args))

bucket_str = args['new_bucket_name'].replace('\\','\')
bucket_json = json.loads(bucket_str)
bucket_name = bucket_json['name']
object_str = args['new_object_key']
object_json = json.loads(object_str)
object_key = object_json['key']
object_key_category = object_key.split("_")[0]

s3_file_path = f"s3a://{bucket_name}/{object_key}"
df_new = spark.read.csv(s3_file_path, header=True, inferSchema=True)

df_new = df_new.withColumn("language_category", lit(object_key_category))
df_new = df_new.select([col(col_name) for col_name in df_new.columns if col_name != '_c0'])

target_bucket = 'my-table-0821'
target_file_key = 'output.parquet'

# Except existing rows
res = s3_client.list_objects(Bucket=target_bucket)
if (res.get("contents", -1) != -1):
    df_old = spark.read.parquet(f"s3a://{target_bucket}/{target_file_key}")
    df_new = df_new.exceptAll(df_old.select([col(col_name) for col_name in df_new.columns]))

df_new = df_new.withColumn("ingest_datetime", current_timestamp())
df_new.write.parquet(f"s3a://{target_bucket}/{target_file_key}", mode="overwrite", compression="snappy")

spark.stop()
job.commit()