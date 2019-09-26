from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from secrete import db_password, end_point, db_name, db_user_name, bucket_parquet
import psycopg2
import sys


def write_to_db(records):
    # use our connection values to establish a connection
    conn = psycopg2.connect(
        database=db_name,
        user=db_user_name,
        password=db_password,
        host=end_point,
        port='5432'
    )
    # create a psycopg2 cursor that can execute queries
    cursor = conn.cursor()
    tbl_name = 'test_sic'
    # create a new table to store results
    cursor.execute('''CREATE TABLE IF NOT EXISTS {}(
                                            id serial PRIMARY KEY,
                                            strategy_name VARCHAR(50),
                                            ticker VARCHAR(6),
                                            last_price NUMERIC(18,2),
                                            purchase_date DATE,
                                            purchase_price NUMERIC(18,2),
                                            purchase_vol NUMERIC(18,2),
                                            PnL NUMERIC(18,2)
                    );'''.format(tbl_name))
    conn.commit()

    # Convert Unicode to plain Python string: "encode"
    ticker = records[0]
    last_price = records[1]
    purchase_date = records[2]
    purchase_price = records[3]
    purchase_vol = records[4]
    PnL = records[5]

    # cursor.execute('''DELETE FROM  results;''')
    # conn.commit()

    cursor.execute("INSERT INTO {} (strategy_name, ticker, last_price, purchase_date, purchase_price, purchase_vol, PnL)"
                   " VALUES ('first_month_ma', '{}', '{}', '{}', {}, {}, {});".format(tbl_name, ticker, last_price, purchase_date, purchase_price, purchase_vol, PnL))
    cursor.execute("""SELECT * from {} LIMIT 5;""".format(tbl_name))
    conn.commit()

    # rows = cursor.fetchall()
    # print(rows)

    cursor.close()
    conn.close()


def strategy_1_all(bucket_name, file_name, target_amount = 100, mvw=7):
    '''
    A naïve trading approach: buy at the beginning of each month if moving average price is less than previous close,
    PnL is calculated with the last close and purchase price
    :param target_amount: the amount purchase each Month
    :param mvw: moving average window
    :return: Output to postgres
    '''
    spark = SparkSession.builder \
        .master("spark://ip-10-0-0-5:7077") \
        .appName("Transform SIC parquet") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    # load parquet file
    df = spark.read.option("inferSchema", "true").parquet("s3a://" + bucket_name + "/" + file_name)

    # load csv file
    # bucket_name = bucket_prices
    # file_name = 'combine_csv_0.csv'
    # df = spark.read.option("inferSchema", "true").csv("s3a://" + bucket_name + "/" + file_name, header=True)

    df = df.drop('open', 'close', 'volume', 'high', 'low','sector')

    df = df.withColumn("adj_close", df.adj_close.cast("double"))
    df.withColumn('maxN', F.when((df.adj_close < 0.001) | (df.adj_close > 100000000), 0)).drop('maxN')

    # get the last adj_close price for each ticker in the series
    w = Window.partitionBy(df.ticker).orderBy(df.date).rangeBetween(-sys.maxsize, sys.maxsize)
    new_df = df.select(df.ticker,F.max(df.date).over(w).alias('max_date')).dropDuplicates()
    new_df = new_df.withColumnRenamed('max_date', 'date')
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
    df = df.withColumn('PnL', (df.last_close - df.purchase_price) * df.purchase_vol)

    df = df.drop('adj_close', 'volume', 'ma', 'previous_day', 'month', 'dayofmonth', 'purchase' )
    # df.printSchema()
    df.withColumn('maxN', F.when((df.PnL > 100000000)|(df.purchase_price>100000000)|(df.purchase_vol>100000000), 0)).drop('maxN')
    # Col_names: ticker, last_close, date, price, vol, pnl
    tbl_name = 'test_sic'
    url = 'postgresql://10.0.0.9:5432/'
    properties = {'user': db_user_name, 'password': db_password, 'driver': 'org.postgresql.Driver','numpartition': 10000}
    df.write.jdbc(url='jdbc:%s' % url, table=tbl_name, mode='overwrite', properties=properties)



if __name__ == '__main__':
    # bucket_parquet contains SIO, SIB, SIC
    strategy_1_all(bucket_parquet, "simulate_SIC.parquet")