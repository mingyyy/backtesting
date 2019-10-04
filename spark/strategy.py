from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from secrete import db_password, db_user_name, bucket_parquet
from pyspark.sql.types import StructType, StructField, DateType, StringType
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
        .config("spark.sql.broadcastTimeout", 1000) \
        .config('spark.sql.shuffle.partitions', 930)\
        .config('spark.sql.autoBroadcastJoinThreshold', 10485760*4 )\
        .config('spark.sql.files.maxPartitionBytes', 1024*1024*128)\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.catalog.clearCache()

    # Load files
    if file_name.split('.')[1] == 'parquet':
        df = spark.read.option("inferSchema", "true").parquet("s3a://" + bucket_name + "/" + file_name)
    elif file_name.split('.')[1] == 'csv':
        df = spark.read.csv("s3a://" + bucket_name + "/" + file_name, header=True)

    # Data cleaning: remove the columns not needed
    df = df.drop('open', 'close', 'volume', 'high', 'low')

    # Data cleaning: remove the unrealistic prices
    df = df.withColumn("adj_close", df.adj_close.cast("double"))
    df = df.withColumn('maxN', F.when((df.adj_close < 0.1) | (df.adj_close > 1000), 0).otherwise(1))
    df = df.filter(df['maxN'] == 1).drop('maxN')

    # Get the first day of each month
    df_d = df.select('date').drop_duplicates()
    df_d = df_d.withColumn('yearmonth', F.concat(F.year(df_d.date),F.month(df_d.date)))
    df_d = df_d.withColumn('dayofmonth', F.dayofmonth(df_d.date))
    c1 = F.min(df_d.dayofmonth).over(Window.partitionBy(df_d.yearmonth))
    df_d = df_d.withColumn('first_price_Mth', F.when(c1 == df_d.dayofmonth, 1))
    df_d = df_d.filter(df_d.first_price_Mth.isNotNull())
    df_d = df_d.select('date')
    # cache first day table
    df_d.persist()

    # Get the last adj_close price for each ticker as last price
    w = Window.partitionBy(df.ticker).orderBy(df.date).rangeBetween(-sys.maxsize, sys.maxsize)
    new_df = df.select(df.ticker,F.max(df.date).over(w).alias('max_date')).dropDuplicates()
    new_df = new_df.withColumnRenamed('max_date', 'date')
    new_df = F.broadcast(new_df).join(df, ['ticker', 'date'], 'inner').select('ticker', 'adj_close')
    new_df = new_df.withColumnRenamed('adj_close', 'last_close')
    # cache last prices table
    new_df.persist()

    # Finding moving average and previous day prices
    w = Window.partitionBy(df.ticker).orderBy(df.date).rowsBetween(-mvw, 0)
    df = df.withColumn("ma", F.avg(df.adj_close).over(w))
    df = df.withColumn('previous_day', F.lag(df.adj_close, 1, 0).over(Window.partitionBy(df.ticker).orderBy(df.date)))

    # condition1: only buy at first day of the month
    df = df.join(F.broadcast(df_d), 'date', 'inner')

    # condition2: moving avg is less than previous day close price
    df = df.withColumn('purchase_price', F.when(df.ma < df.previous_day, df.adj_close))
    df = df.withColumn('purchase_vol',
                         F.when(df.ma < df.previous_day, target_amount/df.adj_close))
    df = df.filter(df.purchase_price.isNotNull())

    # Use the last price as the sell_price
    df = new_df.join(df, 'ticker').dropDuplicates()
    df = df.withColumn('pnl', (df.last_close - df.purchase_price) * df.purchase_vol)

    # Data cleaning: remove the unnecessary
    df = df.drop('adj_close', 'volume', 'ma', 'previous_day', 'month', 'dayofmonth', 'purchase')

    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    df = df.withColumn('create_date', F.unix_timestamp(F.lit(timestamp), 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
    df = df.withColumnRenamed('date', 'purchase_date')
    df = df.withColumn("purchase_date", df['purchase_date'].cast(DateType()))

    url = 'postgresql://10.0.0.9:5432/'
    properties = {'user': db_user_name, 'password': db_password, 'driver': 'org.postgresql.Driver'}
    df.write.jdbc(url='jdbc:%s' % url, table=tbl_name, mode=write_mode, properties=properties)

    new_df.unpersist()
    df_d.unpersist()
    spark.catalog.clearCache()


if __name__ == '__main__':
    strategy_1_all('testing flow using SIO parquet to DB -0.1-1000, 470', bucket_parquet, "simulate_SIO.parquet", 'test','append')