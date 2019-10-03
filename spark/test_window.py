from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from secrete import db_password, db_user_name, bucket_parquet, bucket_large
from pyspark.sql.types import StructType, StructField, DateType, StringType, DoubleType, IntegerType
import sys, time, datetime


def strategy_1_all(app_name, bucket_name, file_name, tbl_name, write_mode, target_amount=100, mvw=7):
    '''
    A naïve trading approach: buy at the beginning of each month if moving average price is less than previous close,
    pnl is calculated with the last close and purchase price
    :param target_amount: the amount purchase each Month
    :param mvw: moving average window
    :return: Output to postgres
    '''
    spark = SparkSession.builder \
        .master("spark://ip-10-0-0-5:7077") \
        .appName(app_name) \
        .config("spark.sql.broadcastTimeout", 10000) \
        .config('spark.sql.shuffle.partitions', 1900)\
        .config('spark.sql.autoBroadcastJoinThreshold', 10485760*4 )\
        .config('spark.sql.files.maxPartitionBytes', 1024*1024*128)\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    schema = StructType([
        StructField("date", DateType(), True),
        StructField("ticker", StringType(), True),
        StructField("sector", StringType(), True),
        StructField("adj_close", StringType(), True),
        StructField("high", StringType(), True),
        StructField("low", StringType(), True),
        StructField("open", StringType(), True),
        StructField("close", StringType(), True),
        StructField("volume", StringType(), True),
    ])
    # load files
    if file_name.split('.')[1] == 'parquet':
    # .option("inferSchema", "true")
        df = spark.read.parquet("s3a://" + bucket_name + "/" + file_name)
    elif file_name.split('.')[1] == 'csv':
        df = spark.read.csv("s3a://" + bucket_name + "/" + file_name, header=True)

    # Data cleaing: remove the columns not needed
    df = df.drop('open', 'close', 'volume', 'high', 'low')

    df = df.withColumn('length', F.length(F.col('sector')))
    x=df.agg(F.max(df.length)).collect()[0][0]
    print(x)



if __name__ == '__main__':
    # testing using SIB
    strategy_1_all('test moving average using SIB', bucket_parquet, "simulate_SIB.parquet", 'test', 'append')
