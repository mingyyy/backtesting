from pyspark.sql import SparkSession
from secrete import bucket_simulation, bucket_parquet, bucket_prices, bucket_large, bucket_larger, db_user_name, db_password
from pyspark.sql.types import StructType, StructField, DateType, StringType, DoubleType, IntegerType,FloatType
import subprocess


def quiet_logs(spark):
  logger = spark._jvm.org.apache.log4j
  logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def load_files(from_bucket, app_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config('spark.sql.files.maxPartitionBytes', 1024 * 1024 * 128) \
        .config('spark.sql.shuffle.partitions', 200) \
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
    df = spark.read.option("inferSchema", "true").csv("s3a://" + from_bucket + "/SIB_0.csv", header=True)
    types = [f.dataType for f in df.schema.fields]
    fieldnames = [f.name for f in df.schema.fields]
    # types = [DateType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType]
    # fieldnames = ['date', 'ticker', 'sector', 'adj_close', 'high', 'low', 'open', 'close', 'volume']

    # command = 'cat helloworld'
    # process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    # output, error = process.communicate()


    tbl_name = 'test_header'

    url = 'postgresql://10.0.0.9:5432/'
    properties = {'user': db_user_name, 'password': db_password, 'driver': 'org.postgresql.Driver'}
    # df.write.jdbc(url='jdbc:%s' % url, table=tbl_name, mode='overwrite', properties=properties)

    pushdown_query = "create table {} ()".format(tbl_name)
    spark.read.jdbc(url='jdbc:%s' % url, table=pushdown_query, properties=properties)


    return types, fieldnames


if __name__ == '__main__':
    s = load_files(bucket_large, 'test schema search using SIB_0')
    print(s)