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
        .config("spark.sql.broadcastTimeout", 36000) \
        .config('spark.sql.shuffle.partitions', 400)\
        .config('spark.sql.autoBroadcastJoinThreshold', 20485760 )\
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
    # load parquet file
    # df = spark.read.option("inferSchema", "true").parquet("s3a://" + bucket_name + "/" + file_name)

    # load csv file
    df = spark.read.csv("s3a://" + bucket_name + "/" + file_name, header=True, schema=schema)

    # Data cleaing: remove the columns not needed
    df = df.drop('open', 'close', 'volume', 'high', 'low')

    # Data cleaing: remove the unrealistic prices
    df = df.withColumn("adj_close", df.adj_close.cast("double"))
    df = df.withColumn('maxN', F.when((df.adj_close < 0.0001) | (df.adj_close > 10000000), 0).otherwise(1))
    df = df.filter(df['maxN'] == 1).drop('maxN')
    # print(df.count())

    # get the last adj_close price for each ticker in the series
    w = Window.partitionBy(df.ticker).orderBy(df.date).rangeBetween(-sys.maxsize, sys.maxsize)
    new_df = df.select(df.ticker,F.max(df.date).over(w).alias('max_date')).dropDuplicates()
    new_df = new_df.withColumnRenamed('max_date', 'date')
    # print(new_df.count())
    new_df = F.broadcast(new_df).join(df, ['ticker', 'date'], 'inner').select('ticker', 'adj_close')
    new_df = new_df.withColumnRenamed('adj_close', 'last_close')
    # Use the last price as the sell_price
    df = F.broadcast(new_df).join(df, 'ticker')
    # check if there is any null in the date
    # df=df.filter(df.date.isNotNull())

    # function to calculate number of seconds from number of days
    w = Window.orderBy(df.date.cast("timestamp").cast("long")).rowsBetween(-mvw, 0)

    # find the moving average price mvw days
    df = df.withColumn("ma", F.avg(df.adj_close).over(w))
    df = df.withColumn('previous_day', F.lag(df.adj_close, 1,0).over(Window.orderBy(df.date)))
    df = df.withColumn('month', F.month(df.date))
    df = df.withColumn('dayofmonth', F.dayofmonth(df.date))

    # condition1: first day of the month
    c1 = F.min(df.dayofmonth).over(Window.partitionBy(df.month))
    df = df.withColumn('purchase', F.when(c1 == df.dayofmonth, df.adj_close))

    # condition2: moving avg is less than previous day close price
    df = df.filter(df.purchase.isNotNull())
    df = df.withColumn('purchase_price', F.when(df.ma < df.previous_day, df.adj_close).cast('double'))
    df = df.withColumn('purchase_vol',
                         F.when(df.ma < df.previous_day, target_amount/df.adj_close))
    df = df.filter(df.purchase_price.isNotNull())
    df = df.withColumn('pnl', (df.last_close - df.purchase_price) * df.purchase_vol)

    # Data cleaning: remove the unnecessary
    df = df.drop('adj_close', 'volume', 'ma', 'previous_day', 'month', 'dayofmonth', 'purchase')
    df = df.withColumn('maxN', F.when((df.pnl > 100000000) | (df.purchase_price > 100000000) | (df.purchase_vol > 100000000), 0).otherwise(1))
    df = df.filter(df['maxN'] == 1).drop('maxN')
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    df = df.withColumn('create_date', F.unix_timestamp(F.lit(timestamp), 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
    df = df.withColumnRenamed('date', 'purchase_date')
    # Col_names: ticker, last_close, date, sector, purchase_price, purchase_vol, pnl
    # df = df.coalesce(50)
    # Export to DB

    df = df.select('ticker','sector','purchase_date','purchase_price','purchase_vol','last_close','pnl','create_date')

    url = 'postgresql://10.0.0.9:5432/'
    properties = {'user': db_user_name, 'password': db_password, 'driver': 'org.postgresql.Driver'}

    df.write.jdbc(url='jdbc:%s' % url, table=tbl_name, mode=write_mode, properties=properties)


if __name__ == '__main__':
    # bucket_parquet contains SIO, SIB, SIC
    strategy_1_all('transform SIB parquet to db', bucket_parquet, "simulate_SIB.parquet", 'tbl_sib', 'overwrite')
