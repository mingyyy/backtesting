from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql import DataFrameReader
from pyspark.sql.context import SQLContext
from secrete import db_password
import gc
from pyspark.sql.window import Window


def strategy_1(target_price=100, profit_perc=0.1):
    spark = SparkSession.builder \
                 .master("spark://ip-10-0-0-13:7077") \
                 .appName("historical prices") \
                 .config("spark.some.config.option", "some-value") \
                 .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    bucket_name = "hist-price"
    file_name = "historical_stock_prices.csv"
    df = spark.read.csv("s3a://" + bucket_name + "/" + file_name, header=True)
    df = df.drop('open', 'close', 'low', 'high')

    # get the list of stocks with more than 1 year records
    df_gp_ticker = df.groupBy('ticker').agg({'date': 'count'})\
      .select(col('ticker'), col('count(date)').alias('num_of_rec')).orderBy('count(date)')
    df_stock_list = df_gp_ticker.filter(df_gp_ticker.num_of_rec > 250).drop(df_gp_ticker.num_of_rec)
    # release memory of the intermediate df
    # del df_gp_ticker
    # gc.collect()

    # show the df_stock_list
    df_new = df_stock_list.join(df, df_stock_list.ticker == df.ticker).drop(df_stock_list.ticker)
    # find the moving average price 100 days
    df_movAvg = df_new.withColumn("ma100", avg(df_new.adj_close)
                                   .over(Window.partitionBy(df_new.date).rowsBetween(-100, 0)))
    df_movAvg.filter(df_movAvg.ticker== 'AHH').rderBy(df_movAvg.ticker).show()


if __name__ == '__main__':
    strategy_1()

    # url = 'postgresql://ec2-3-229-236-236.compute-1.amazonaws.com:5432/dbname'
    # properties = {'user': 'postgres', 'password': db_password}
    # df = DataFrameReader(SQLContext).jdbc(
    #     url='jdbc:%s' % url, table='Tbl_test', properties=properties
    # )
    # # To append to existing table "questions"
    # df.write.format("jdbc").mode("append") .option("url", "jdbc:postgresql://hostname/ls?user=xxx&password=xxx").option("dbtable", "questions").option("user", "postgres").option("password", "xxx").save()

