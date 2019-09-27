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
        .config('spark.sql.files.maxPartitionBytes', 1024 * 1024 * 50) \
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
    df = spark.read.csv("s3a://" + from_bucket + "/SIE*.csv", header=True, schema=schema)
    df.drop('open', 'close', 'volume', 'high', 'low')
    df = df.withColumn('year', F.year(df.date).cast('integer'))

    df.filter(df.year.between(1900, 1930)).write.parquet("s3a://" + to_bucket + "/simulate_SIE1.parquet", mode="overwrite")
    df.filter(df.year.between(1930, 1950)).write.parquet("s3a://" + to_bucket + "/simulate_SIE2.parquet", mode="overwrite")
    df.filter(df.year.between(1950, 1970)).write.parquet("s3a://" + to_bucket + "/simulate_SIE3.parquet", mode="overwrite")
    df.filter(df.year.between(1970, 1990)).write.parquet("s3a://" + to_bucket + "/simulate_SIE4.parquet", mode="overwrite")
    df.filter(df.year.between(1990, 2019)).write.parquet("s3a://" + to_bucket + "/simulate_SIE5.parquet", mode="overwrite")

    # read in all parquet files from this bucket to a single df
    # df = spark.read.parquet("s3a://" + bucket_name + "/*.parquet", header=True)
    # write to the same bucket as a single parquet file


if __name__ == '__main__':
    load_files(bucket_larger, bucket_parquet,'load SIE_* by batch')