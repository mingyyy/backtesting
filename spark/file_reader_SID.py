from pyspark.sql import SparkSession
from secrete import bucket_simulation, bucket_parquet, bucket_prices, bucket_large, bucket_larger
from pyspark.sql.types import StructType, StructField, DateType, StringType, DoubleType, IntegerType
from pyspark.sql import functions as F

def quiet_logs(spark):
  logger = spark._jvm.org.apache.log4j
  logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def load_files(from_bucket, to_bucket, app_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config('spark.sql.files.maxPartitionBytes', 1024 * 1024 * 128) \
        .getOrCreate()

    quiet_logs(spark)

    # read in all csv files from this bucket to a single df

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
    df = spark.read.csv("s3a://" + from_bucket + "/SID*.csv", header=True, schema=schema)
    df.drop('open', 'close', 'volume', 'high', 'low')
    df = df.withColumn('year', F.year(df.date).cast('integer'))

    print(df.rdd.getNumPartitions())

    df1=df.filter(df.year.between(1900, 1930))
    df1.write.parquet("s3a://" + to_bucket + "/simulate_SID1.parquet", mode="overwrite")
    df1.unpersist()

    df2=df.filter(df.year.between(1930, 1950))
    df2.write.parquet("s3a://" + to_bucket + "/simulate_SID2.parquet", mode="overwrite")
    df2.unpersist()

    df3=df.filter(df.year.between(1950, 1970))
    df3.write.parquet("s3a://" + to_bucket + "/simulate_SID3.parquet", mode="overwrite")
    df3.unpersist()

    df4=df.filter(df.year.between(1970, 1990))
    df4.write.parquet("s3a://" + to_bucket + "/simulate_SID4.parquet", mode="overwrite")
    df4.unpersist()

    df5=df.filter(df.year.between(1990, 2019))
    df5.write.parquet("s3a://" + to_bucket + "/simulate_SID5.parquet", mode="overwrite")
    df5.unpersist()
    # read in all parquet files from this bucket to a single df
    # df = spark.read.parquet("s3a://" + bucket_name + "/*.parquet", header=True)

    # write to the same bucket as a single parquet file


if __name__ == '__main__':
    load_files(bucket_large, bucket_parquet,'load SID_* by batch')