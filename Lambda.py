import findspark; findspark.init()
from pyspark.sql.session import SparkSession
import boto3


spark = SparkSession.builder.master('local').appName("test_intake").getOrCreate()

filePath = './data/chinese_top100_artist.csv'
df=spark.read.format("csv").option("header","true").load(filePath)