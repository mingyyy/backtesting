import boto3
import pandas as pd
import pyarrow as pa
from s3fs import S3FileSystem
import pyarrow.parquet as pq
from secrete import bucket_simulation, bucket_upload, bucket_prices, bucket_parquet


def csv2parquet(bucket_name):
    s3 = boto3.client('s3', region_name='us-east-1')

    files = list(boto3.resource('s3').Bucket(bucket_name).objects.all())
    for f in files:
        object_key = f.key
        obj = s3.get_object(Bucket=bucket_name, Key=object_key)
        df = obj['Body'].read().decode('utf-8')

        table = pa.Table.from_pandas(df)

        output_file = "s3://"+bucket_name+"/"+object_key
        s3fs = S3FileSystem()

        pq.write_to_dataset(table=table,
                            root_path=output_file,partition_cols=['date'],
                            filesystem=s3fs)
        print("File converted from CSV to parquet completed")


from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *


def convertor(bucket_name):
    if __name__ == "__main__":
        sc = SparkContext(appName="CSV2Parquet")
        sqlContext = SQLContext(sc)

        schema = StructType([StructField("date", StringType(), True)])
        bucket_name = bucket_prices
        file_name = "sample1.csv"

        # ticker,open,close,adj_close,low,high,volume,date
        schema = StructType([
            StructField("ticker", StringType(), True),
            StructField("open", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("adj_close", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("volume", DoubleType(), True),
            StructField("date", DateType(), True),
        ])

        # schema = StructType([
        #     StructField("col1", DateType(), True),
        #     StructField("col2", IntegerType(), True),
        #     StructField("col3", StringType(), True),
        #     StructField("col6", DoubleType(), True)])
        rdd = sc.textFile("s3a://" + bucket_name + "/" + file_name).map(lambda line: line.split(","))

        print(rdd)
        df = sqlContext.createDataFrame(rdd, schema)
        df.write.parquet("../output/s3_pq")


if __name__ == "__main__":
    # csv2parquet(bucket_upload)
    convertor((bucket_parquet))
