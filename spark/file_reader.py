from pyspark.sql import SparkSession
from pyspark.sql import DataFrameReader
from pyspark.sql.context import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from secrete import bucket_simulation



def quiet_logs(spark):
  logger = spark._jvm.org.apache.log4j
  logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def load_files(bucket_name, app_name):
    spark = SparkSession.builder \
        .master("spark://ip-10-0-0-13:7077") \
        .appName(app_name) \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    quiet_logs(spark)

    # read in all csv files from this bucket to a single df
    df = spark.read.csv("s3a://" + bucket_name + "/*.csv", header=True)

    # read in all parquet files from this bucket to a single df
    # df = spark.read.parquet("s3a://" + bucket_name + "/*.parquet", header=True)

    # df.describe().show()
    df.explain()

    # write to the same bucket as a single parquet file
    df.write.parquet("s3a://" + bucket_name + "/simulated01.parquet", mode="overwrite")


if __name__ == '__main__':
    load_files(bucket_simulation,'load all csv files')
