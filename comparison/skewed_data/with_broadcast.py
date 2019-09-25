from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from secrete import bucket_parquet
import sys


def strategy_1_all():
    spark = SparkSession.builder \
        .master("spark://ip-10-0-0-13:7077") \
        .appName("work with parquet file") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    # load parquet file
    bucket_name = bucket_parquet
    file_name = "simulate_test.parquet"
    df = spark.read.option("inferSchema", "true").parquet("s3a://" + bucket_name + "/" + file_name)
    df = df.drop('open', 'close', 'volume', 'high', 'low', 'sector')
    # get the last adj_close price for each ticker in the series
    w = Window.partitionBy(df.ticker).orderBy(df.date).rangeBetween(-sys.maxsize, sys.maxsize)
    new_df = df.select(df.ticker, F.max(df.date).over(w).alias('max_date')).dropDuplicates()
    new_df = new_df.withColumnRenamed('max_date', 'date')
    new_df = F.broadcast(new_df).join(df, ['ticker', 'date'], 'inner').select('ticker', 'adj_close')
    new_df = new_df.withColumnRenamed('adj_close', 'last_close')
    # Use the last price as the sell_price
    new_df = F.broadcast(new_df).join(df, 'ticker')
    new_df.show(20)

    '''
    comparison(importing Parquet file 3.1GB):
        1. Running on t2.large: without sorting with broadcoast: -> 1.7min
        2. Running on t2.large: without sorting without broadcoast -> 3.1 min

    '''


if __name__ == '__main__':
    strategy_1_all()