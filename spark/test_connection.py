from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import DataFrameReader
from pyspark.sql.context import SQLContext
from secrete import db_password


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

df_gp_ticker = df.groupBy('ticker').agg({'date': 'count'})\
  .select(col('ticker'), col('count(date)').alias('num_of_rec')).orderBy('count(date)')
print(df_gp_ticker.take(10))


# url = 'postgresql://ec2-3-229-236-236.compute-1.amazonaws.com:5432/dbname'
# properties = {'user': 'postgres', 'password': db_password}
# df = DataFrameReader(SQLContext).jdbc(
#     url='jdbc:%s' % url, table='Tbl_test', properties=properties
# )
# # To append to existing table "questions"
# df.write.format("jdbc").mode("append") .option("url", "jdbc:postgresql://hostname/ls?user=xxx&password=xxx").option("dbtable", "questions").option("user", "postgres").option("password", "xxx").save()

