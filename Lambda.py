import findspark; findspark.init()
from pyspark.sql.session import SparkSession
import pandas as pd
import boto3
import io

s3_client = boto3.client('s3')
spark = SparkSession.builder.master('local').appName("test_intake").getOrCreate()


# Local Test event
import json
with open('./test_event_s3_put.json') as e:
    event = json.load(e)

filePath = './data/chinese_top100_artist.csv'
df=spark.read.format("csv").option("header","true").load(filePath)


file_name = './data/chinese_top100_artist.csv'
bucket = 'csv-input-20230814'
object_name = 'chinese_top100_artist.csv'

s3_file_path = 's3://csv-input-20230814/chinese_top100_artist.csv'

# response = s3_client.upload_file(file_name, bucket, object_name)

s3_client.list_objects(Bucket=bucket)

response = s3_client.get_object(Bucket=bucket, Key=object_name)
csv_content = response['Body'].read().decode('utf-8')
csv_file = io.StringIO(csv_content)
df = pd.read_csv(csv_file)

spark_df = spark.createDataFrame(df)
# df = spark.read.csv(s3_file_path, header=True, inferSchema=True)

with open('test_download.csv', 'wb') as f:
    s3_client.download_fileobj(bucket, object_name, f)
