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
        .config('spark.sql.shuffle.partitions', 700) \
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
    df = spark.read.csv("s3a://" + from_bucket + "/SIL*.csv", header=True, schema=schema)
    df.drop('open', 'close', 'volume', 'high', 'low')
    # df = df.withColumn('year', F.year(df.date).cast('integer'))

    df.write.parquet("s3a://" + to_bucket + "/simulate_SIL.parquet", mode="overwrite")


if __name__ == '__main__':
    load_files(bucket_larger, bucket_parquet,'load SIL_* by batch')
