from pyspark.sql import SparkSession
from secrete import bucket_simulation, bucket_parquet, bucket_prices, bucket_large
from pyspark.sql.types import StructType, StructField, DateType, StringType, DoubleType, IntegerType
from pyspark import SparkContext
from pyspark.sql import SQLContext


def quiet_logs(spark):
  logger = spark._jvm.org.apache.log4j
  logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def load_files(from_bucket, to_bucket, app_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
        # .config("spark.some.config.option", "some-value") \
        # .getOrCreate()
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
    df = spark.read.csv("s3a://" + from_bucket + "/SIB_*", header=True)

    # read in all parquet files from this bucket to a single df
    # df = spark.read.parquet("s3a://" + bucket_name + "/*.parquet", header=True)

    # write to the same bucket as a single parquet file
    df.write.parquet("s3a://" + to_bucket + "/simulate_SIB.parquet", mode="overwrite")


if __name__ == '__main__':
    load_files(bucket_large, bucket_parquet,'load all SIB csv file')
