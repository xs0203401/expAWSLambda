import io
import random
import logging
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType
from pyspark.sql.functions import col

import pandas as pd
import boto3
from botocore.exceptions import ClientError


s3_client = boto3.client('s3')

def get_spark():
    # Notes: hadoop-aws and aws-java-sdk needs match versions
    spark = SparkSession.builder.master("local[4]").appName('SparkDelta') \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", 
                "io.delta:delta-core_2.12:1.1.0,"
                "org.apache.hadoop:hadoop-aws:3.2.2,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.180") \
        .getOrCreate()
        
    spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")

    return spark

spark = get_spark()

def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True


# # Local Test event
# import json
# with open('./test_event_s3_put.json') as e:
#     event = json.load(e)

## Local
# filePath = './data/chinese_top100_artist.csv'
# df=spark.read.format("csv").option("header","true").load(filePath)

# # Testing uploding using boto3 s3_client
file_name = 'data/korean_top100_artist.csv'
bucket = 'csv-ingest-0821'
object_name = 'korean_top100_artist-4.csv'

response = s3_client.upload_file(file_name, bucket, object_name)
print(response)

# s3_client.list_objects(Bucket=bucket)

# response = s3_client.get_object(Bucket=bucket, Key=object_name)
# csv_content = response['Body'].read().decode('utf-8')
# csv_file = io.StringIO(csv_content)
# df = pd.read_csv(csv_file)
# spark_df = spark.createDataFrame(df)


# Testing reading from spark from s3
region = 'us-east-2'
# s3_file_path = 's3a://csv-input-20230814/chinese_top100_artist.csv'
s3_file_path = "s3a://csv-input-20230814/biostats.csv"
df = spark.read.csv(s3_file_path, header=True, inferSchema=True)


while True:
    target_bucket = f"hello-bucket-20230820-{random.randint(10000000, 99999999)}"
    if create_bucket(target_bucket, region=region):
        break

df = df.select([col(col_name).alias(trimmed_name) for col_name, trimmed_name in zip(df.columns, [col_name.strip(' "').replace(' ', '_').replace('(', '_').replace(')','_') for col_name in df.columns])]) # Trimmed column names for "biostats.csv"
df.write.parquet(f"s3a://{target_bucket}/test_output.parquet")