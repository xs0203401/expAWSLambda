import io
import random
import logging
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType
from pyspark.sql.utils import AnalysisException


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
'''
- data/chinese_top100_artist.csv
- data/japanese_top100_artist.csv
- data/korean_top100_artist.csv
'''

# # Testing uploding using boto3 s3_client
file_name = 'data/korean_top100_artist.csv'
bucket = 'csv-ingest-0821'
object_name = 'korean_top100_artist.csv'

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
bucket_str = 'csv-ingest-0821'
key_str = 'korean_top100_artist.csv'
key_category = key_str.split("_")[0]
s3_read_path = f"s3a://{bucket_str}/{key_str}"
# s3_file_path = 's3a://csv-input-20230814/chinese_top100_artist.csv'
# s3_file_path = "s3a://csv-input-20230814/biostats.csv"

TABLE_SCHEMA_LITE = StructType([
    StructField("_c0", IntegerType(), True),
    StructField("artist_name", StringType(), True),
    StructField("popularity", IntegerType(), True),
    StructField("followers", IntegerType(), True),
    StructField("artist_link", StringType(), True),
    StructField("genres", StringType(), True),
    StructField("top_track", StringType(), True),
    StructField("top_track_album", StringType(), True),
    StructField("top_track_popularity", IntegerType(), True),
    StructField("top_track_release_date", StringType(), True),
    StructField("top_track_duration_ms", IntegerType(), True),
    StructField("top_track_explicit", BooleanType(), True),
    StructField("top_track_album_link", StringType(), True),
    StructField("top_track_link", StringType(), True)
])
df1 = spark.read.csv(s3_read_path, header=True, schema=TABLE_SCHEMA_LITE).withColumn("language_category", lit(key_category)) # .withColumn("ingest_datetime", current_timestamp())
df1 = df1.select([col(col_name) for col_name in df1.columns if col_name != '_c0' and col_name != "ingest_datetime"])

# exceptAll
df_copy.printSchema()
df1.printSchema()
df_all = df_copy.select([col(col_name) for col_name in df_copy.columns if col_name != '_c0' and col_name != "ingest_datetime"])
df3 = df1.exceptAll(df_all)
# df3 = df1.union(df_copy.select([col(col_name) for col_name in df_copy.columns if col_name != "ingest_datetime"])).distinct()
# df3.where(col("language_category") == 'korean').orderBy('artist_name').show()

# ExceptAll - sql
df_copy.select([col(col_name) for col_name in df_copy.columns if col_name != '_c0' and col_name != "ingest_datetime"]).createTempView("t_all")
df1.createTempView("t0")
show = lambda x: spark.sql(x).show(30)
show("select * from t0 except all select * from t_all")




with open('/Users/henryliu/Temp00/expAWSLambda/test_event_s3_put.json') as f:
    event = json.load(f)

bkt = event['Records'][0]['s3']['bucket']
obj = event['Records'][0]['s3']['object']

res = s3_client.list_buckets()
target_bucket = res['Buckets'][4]['Name']

while True:
    target_bucket = f"hello-bucket-20230820-{random.randint(10000000, 99999999)}"
    if create_bucket(target_bucket, region=region):
        break

# # df = df.select([col(col_name).alias(trimmed_name) for col_name, trimmed_name in zip(df.columns, [col_name.strip(' "').replace(' ', '_').replace('(', '_').replace(')','_') for col_name in df.columns])]) # Trimmed column names for "biostats.csv"
# df = df.select([col(col_name) if col_name != '_c0' else col(col_name).alias("row_id") for col_name in df.columns])
# df.write.parquet(f"s3a://{target_bucket}/test_output.parquet", mode="overwrite", compression="snappy")

# target_bucket = 'my-table-0821'
# target_file_key = 'output.parquet'

# df1 = spark.read.parquet(f"s3a://{target_bucket}/test_output.parquet")
# df1.createTempView('test1')
# df2 = spark.sql('select * from test1')
# df1.count()
# df2.union(df1).distinct().count()


# df = df.limit(25)
# df1 = df1.exceptAll(df)
# df1.count()
# df2.show()



# # Glue start job run
# glue_client = boto3.client('glue')
# glue_client.start_job_run(
#         JobName = 'myCSVIngest-0821',
#          Arguments = {
#            '--new_bucket_name'  :   json.dumps(bkt),
#            '--new_object_key'   :   json.dumps(obj) } )


TABLE_SCHEMA = StructType([
    StructField("artist_name", StringType(), True),
    StructField("popularity", IntegerType(), True),
    StructField("followers", IntegerType(), True),
    StructField("artist_link", StringType(), True),
    StructField("genres", StringType(), True),
    StructField("top_track", StringType(), True),
    StructField("top_track_album", StringType(), True),
    StructField("top_track_popularity", IntegerType(), True),
    StructField("top_track_release_date", StringType(), True),
    StructField("top_track_duration_ms", IntegerType(), True),
    StructField("top_track_explicit", BooleanType(), True),
    StructField("top_track_album_link", StringType(), True),
    StructField("top_track_link", StringType(), True),
    StructField("language_category", StringType(), True),
    StructField("ingest_datetime", TimestampType(), True)
])

# Test output file
s3_location = "s3a://my-table-0821/output.parquet/"
# s3_location = "s3a://my-table-0821/ReadOnly__output.parquet/"
df_copy = spark.read.parquet(s3_location, header=True, inferSchema=True)
df_copy.select(col("language_category")).distinct().show(10)
df_copy.show()


# # TEST COPY ReadOnly
# TARGET_BUCKET = 'my-table-0821'
# TARGET_FILE_KEY = 'output.parquet'
# TARGET_READONLY_FILE_KEY = f'ReadOnly__{TARGET_FILE_KEY}'
# s3_client.copy_object(
#     CopySource=f"/{TARGET_BUCKET}/{TARGET_FILE_KEY}",
#     Bucket=TARGET_BUCKET,
#     Key=TARGET_READONLY_FILE_KEY
# )

# df_copy = spark.read.parquet(f"s3a://{TARGET_BUCKET}/{TARGET_FILE_KEY}")
# df_copy.write.parquet(f"s3a://{TARGET_BUCKET}/{TARGET_READONLY_FILE_KEY}", mode="overwrite", compression="snappy")