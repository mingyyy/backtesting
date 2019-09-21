from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from secrete import db_password,end_point, db_name,db_user_name
import psycopg2


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

    # Convert Unicode to plain Python string: "encode"
    ticker = records[0].encode("utf-8")
    purchase_date = records[1]
    purchase_price = records[2]
    purchase_vol = records[3]
    PnL = records[4]

    # cursor.execute('''DELETE FROM  results;''')
    # conn.commit()

    cursor.execute("INSERT INTO results (strategy_name, ticker, purchase_date, purchase_price, purchase_vol, PnL)"
                   " VALUES ('first_month_ma', '{}', '{}', {}, {}, {});".format(ticker, purchase_date, purchase_price, purchase_vol, PnL))
    cursor.execute("""SELECT * from results;""")
    conn.commit()

    rows = cursor.fetchall()
    # print(rows)

    cursor.close()
    conn.close()


def strategy_1(target_ticker='AAPL',target_price=200, target_purchase=100, profit_perc=0.1, mvw=7):
    '''

    :param target_ticker: The ticker of the targeted stock
    :param target_price:
    :param profit_perc: sell if the profit is 10% above the buying price
    :return:
    '''
    spark = SparkSession.builder \
                 .master("spark://ip-10-0-0-13:7077") \
                 .appName("historical prices") \
                 .config("spark.some.config.option", "some-value") \
                 .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    bucket_name = "hist-price"
    file_name = "historical_stock_prices.csv"
    df = spark.read.csv("s3a://" + bucket_name + "/" + file_name, header=True)
    # get the df for targeted stock only
    df= df.drop('open', 'close', 'low', 'high').filter(df.ticker == target_ticker)

    # function to calculate number of seconds from number of days
    w = Window.orderBy(df.date.cast("timestamp").cast("long")).rowsBetween(-mvw, 0)
    # find the moving average price 100 days
    df = df.withColumn("ma100", F.avg(df.adj_close).over(w))
    df = df.withColumn('previous_day', F.lag(df.adj_close, 1,0).over(Window.orderBy(df.date)))
    df = df.withColumn('month', F.month(df.date))
    df = df.withColumn('dayofmonth', F.dayofmonth(df.date))
    # condition1: first day of the month
    c1 = F.min(df.dayofmonth).over(Window.partitionBy(df.month))
    df = df.withColumn('buy', F.when(c1 == df.dayofmonth, df.adj_close))

    # df_temp=df.filter((df.month.isin(4,7))).orderBy(df.date.desc())
    # df_temp.show(45)
    # print(df.dtypes, c1.dtypes)
    # condition2: moving avg is less than previous day close price
    df = df.filter(df.buy.isNotNull())
    df = df.withColumn('purchase_price', F.when(df.ma100 < df.previous_day, df.adj_close))
    df = df.withColumn('buy_vol',
                         F.when(df.ma100 < df.previous_day, target_purchase/df.adj_close))
    df = df.filter(df.purchase_price.isNotNull())
    df = df.withColumn('PnL', (target_price - df.purchase_price) * df.buy_vol)

    df = df.drop('adj_close', 'volume', 'ma100', 'previous_day', 'month', 'dayofmonth', 'buy' )
    # df.show(10)
    # ticker, date, price, vol, pnl

    def get_val(row):
        return (row.ticker, row.purchase_date, row.purchase_price, row.purchase_vol, row.PnL)

    for row in df.collect():

        write_to_db(row)


if __name__ == '__main__':

    strategy_1()