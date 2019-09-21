from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameReader
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import col, avg
from pyspark.sql.window import Window
from secrete import db_password, end_point, db_name, db_user_name
import psycopg2
# import configparser


def to_postgres(Tbl_name, row):
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

    def append_to_table(row):
        cursor.execute("INSERT INTO" + Tbl_name + "(strategy_name, ticker, purchase_date, purchase_price, purchase_vol, PnL) "
                                                  "VALUES('first_month_ma', %s, %s, %s, %s, %s)",
                       (row.ticker, row.purchase_date, row.purchase_price, row.purchase_vol, row.PnL))


    cursor.execute('SELECT * from ' + Tbl_name + ';')
    conn.commit()

    rows = cursor.fetchall()
    print(rows)

    # cursor.execute('''DROP TABLE temp;''')
    # conn.commit()

    cursor.close()
    conn.close()


def strategy_1(target_ticker='AAPL', target_price=100, profit_perc=0.1):
    spark = SparkSession.builder \
                 .master("spark://ip-10-0-0-13:7077") \
                 .appName("historical prices") \
                 .config("spark.jars", "postgresql-42.2.5.jar")\
                 .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    bucket_name = "hist-price"
    file_name = "historical_stock_prices.csv"
    df = spark.read.csv("s3a://" + bucket_name + "/" + file_name, header=True)
    df_base = df.drop('open', 'close', 'low', 'high')

    # get the list of stocks with more than 1 year records
    df_stock_list = df_base.groupBy('ticker').agg({'date': 'count'})\
      .select(col('ticker'), col('count(date)').alias('num_of_rec')).orderBy('count(date)')
    df_stock_list = df_stock_list.filter(df_stock_list.num_of_rec > 250).drop(df_stock_list.num_of_rec)
    # release memory of the intermediate df
    # del df_stock_list
    # gc.collect()

    # show the df_stock_list:
    # df = df_stock_list.join(df, df_stock_list.ticker == df.ticker).drop(df_stock_list.ticker)
    df = df_stock_list.join(df_base, "ticker")
    # find the moving average price 100 days
    df_movAvg = df.withColumn("ma100", avg(df.adj_close)\
                                   .over(Window.partitionBy(df.date).rowsBetween(-10, 1)))
    df_movAvg=df_movAvg.filter(df_movAvg.ticker == target_ticker).orderBy(df_movAvg.ticker, df_movAvg.date.desc())
    df_movAvg.sample(False, 0.1, 1).show()

    # Create the Database properties
    db_url='jdbc:postgresql://10.0.0.9:5432/postgres'
    db_properties = {'username':'postgres',
                    'password':db_password,
                    'url':db_url,
                    'driver':'org.postgresql.Driver'
                    }
    # config = configparser.ConfigParser()
    # config.read("db_properties.ini")
    # db_prop = config['postgres']
    # db_url = db_prop['url']+db_prop['database']
    # db_properties['username'] = db_prop['username']
    # db_properties['password'] = db_prop['properties']
    # db_properties['url'] = db_url
    # db_properties['driver'] = db_prop['driver']

    # Save the dataframe to the table. mode='append'
    df_movAvg.write.jdbc(url=db_url, table='postgres.movAvg', mode='overwrite', properties=db_properties)



if __name__ == '__main__':
    strategy_1()

    # url = 'postgresql://ec2-3-229-236-236.compute-1.amazonaws.com:5432/dbname'
    # properties = {'user': 'postgres', 'password': db_password}
    # df = DataFrameReader(SQLContext).jdbc(
    #     url='jdbc:%s' % url, table='Tbl_test', properties=properties
    # )
    # # To append to existing table "questions"
    # df.write.format("jdbc").mode("append") .option("url", "jdbc:postgresql://hostname/ls?user=xxx&password=xxx").option("dbtable", "questions").option("user", "postgres").option("password", "xxx").save()

