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

    # Data cleaing: remove the unrealistic prices
    df = df.withColumn("adj_close", df.adj_close.cast("double"))
    df = df.withColumn('maxN', F.when((df.adj_close < 0.01) | (df.adj_close > 1000), 0).otherwise(1))
    df = df.filter(df['maxN'] == 1).drop('maxN')


    # test case: DF broadcast join 3m
    w = Window.partitionBy(df.ticker).orderBy(df.date).rangeBetween(-sys.maxsize, sys.maxsize)
    new_df = df.select(df.ticker,F.max(df.date).over(w).alias('max_date')).dropDuplicates()
    new_df = new_df.withColumnRenamed('max_date', 'date')
    new_df = F.broadcast(new_df).join(df, ['ticker', 'date'], 'inner').select('ticker', 'adj_close')
    new_df = new_df.withColumnRenamed('adj_close', 'last_close')

    # test case: sql using sort-merge join 3.9m
    # df.createTempView('t1')
    # new_df = spark.sql('select sub.ticker, t1.adj_close last_close from t1 inner join (select ticker, max(date) m '
    #                    'from t1 group by ticker) sub on t1.ticker=sub.ticker and t1.date=sub.m')
    new_df.printSchema()
    new_df.show(5)


if __name__ == '__main__':
    # SIB parquet file: 10G
    strategy_1_all('test sql using SIB (10G) - sql join', bucket_parquet, "simulate_SIB.parquet", 'test', 'append')
