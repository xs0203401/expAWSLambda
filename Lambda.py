# import findspark; findspark.init()
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

import pandas as pd
import boto3
import io

s3_client = boto3.client('s3')

def get_spark():
    # Notes: hadoop-aws and aws-java-sdk needs match versions
    spark = SparkSession.builder.master("local[4]").appName('SparkDelta') \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem") \
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


# spark = SparkSession.builder\
#         .master('local')\
#         .appName("test_intake")\
#         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#         .config("spark.jars.packags", "org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.529")\
#         .getOrCreate()


#         # .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
#         # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.10.2,org.apache.hadoop:hadoop-client:2.10.2")\
#         # .config("spark.jars.excludes", "com.google.guava:guava")\


# # Local Test event
# import json
# with open('./test_event_s3_put.json') as e:
#     event = json.load(e)

## Local
# filePath = './data/chinese_top100_artist.csv'
# df=spark.read.format("csv").option("header","true").load(filePath)

# Testing uploding using boto3 s3_client
file_name = 'data/japanese_top100_artist.csv'
bucket = 'csv-input-20230814'
object_name = 'japanese_top100_artist.csv'

response = s3_client.upload_file(file_name, bucket, object_name)
print(response)


s3_client.list_objects(Bucket=bucket)

response = s3_client.get_object(Bucket=bucket, Key=object_name)
csv_content = response['Body'].read().decode('utf-8')
csv_file = io.StringIO(csv_content)
df = pd.read_csv(csv_file)
# spark_df = spark.createDataFrame(df)


# Testing reading from spark
s3_file_path = 's3a://csv-input-20230814/chinese_top100_artist.csv'
s3_file_path = "s3a://csv-input-20230814/biostats.csv"
# df = spark.read.csv(s3_file_path, header=True, inferSchema=True)




# # Auxiliary
# with open('test_download.csv', 'wb') as f:
#     s3_client.download_fileobj(bucket, object_name, f)

# ts = ("io.delta:delta-core_2.12:1.1.0,"
#                 "org.apache.hadoop:hadoop-aws:3.2.2,"
#                 "com.amazonaws:aws-java-sdk-bundle:1.12.180")

# pandas_schema = {
#     "Unnamed: 0": "int64",
#     "artist_name": "object",
#     "popularity": "int64",
#     "followers": "int64",
#     "artist_link": "object",
#     "genres": "object",
#     "top_track": "object",
#     "top_track_album": "object",
#     "top_track_popularity": "int64",
#     "top_track_release_date": "object",
#     "top_track_duration_ms": "int64",
#     "top_track_explicit": "bool",
#     "top_track_album_link": "object",
#     "top_track_link": "object"
# }

# spark_schema = StructType([
#     StructField("Unnamed: 0", IntegerType(), True),
#     StructField("artist_name", StringType(), True),
#     StructField("popularity", IntegerType(), True),
#     StructField("followers", IntegerType(), True),
#     StructField("artist_link", StringType(), True),
#     StructField("genres", StringType(), True),
#     StructField("top_track", StringType(), True),
#     StructField("top_track_album", StringType(), True),
#     StructField("top_track_popularity", IntegerType(), True),
#     StructField("top_track_release_date", StringType(), True),
#     StructField("top_track_duration_ms", IntegerType(), True),
#     StructField("top_track_explicit", BooleanType(), True),
#     StructField("top_track_album_link", StringType(), True),
#     StructField("top_track_link", StringType(), True)
# ])