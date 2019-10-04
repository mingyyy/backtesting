from pyspark.sql import SparkSession
from secrete import bucket_prices, bucket_parquet, host
from connect_s3 import save_to_bucket
from api_18080 import check_stages, check_jobs


def quiet_logs(spark):
  logger = spark._jvm.org.apache.log4j
  logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def convert_files(from_bucket, from_file, to_bucket, to_file, app_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config('spark.sql.files.maxPartitionBytes', 1024 * 1024 * 128) \
        .config('spark.sql.shuffle.partitions', 700) \
        .getOrCreate()
    quiet_logs(spark)

    # read in all csv files from this bucket, convert to a single df
    df = spark.read.csv("s3a://" + from_bucket + "/" + from_file, header=True)
    # df.drop('open', 'close', 'volume', 'high', 'low')
    df.write.parquet("s3a://" + to_bucket + "/" + to_file, mode="overwrite")

    # save history log to S3 bucket
    app_id = spark.sparkContext.applicationId
    get_history(app_id)


def get_history(app_id):
    path = 'http://{}:18080/api/v1/applications/'.format(host)
    if check_jobs(path, app_id,'jobs') == 'ok':
        sms = 'Congrats! All jobs succeeded!'
        print(sms)
    else:
        print('Ohh, sorry! Something went wrong, please check the logs.')
        save_to_bucket(check_stages(path, app_id, 'stages'), "log_"+app_id)


if __name__ == '__main__':
    convert_files(bucket_prices, "historical_stock_prices.csv", bucket_parquet, "prices.parquet", 'convert historical prices to parquet')