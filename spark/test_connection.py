
# from secrete import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
#
#
# if __name__ == "__main__":
#     spark = SparkSession.builder \
#         .appName("my_app") \
#         .config('spark.sql.codegen.wholeStage', False) \
#         .getOrCreate()
#
#     spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
#     spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
#     spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#     spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
#     spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
#                                          "org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
#     spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "us-east-1.amazonaws.com")
#
#     df = spark.read.option("delimiter", ",").csv("s3a://hist-price/historical_prices.csv", header=True)
#     print(df.take(3))
#
#
# works with pyspark shell in EC2 Master Node
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

df.map()

df_gp_ticker = df.reduceBy('ticker').agg({'date': 'count'})\
  .select(col('ticker'), col('count(date)').alias('num_of_rec')).orderBy('ticker')
print(df_gp_ticker.take(10))


url = 'postgresql://ec2-3-229-236-236.compute-1.amazonaws.com:5432/dbname'
properties = {'user': 'postgres', 'password': db_password}
df = DataFrameReader(SQLContext).jdbc(
    url='jdbc:%s' % url, table='Tbl_test', properties=properties
)
# To append to existing table "questions"
df.write.format("jdbc").mode("append") .option("url", "jdbc:postgresql://hostname/ls?user=xxx&password=xxx").option("dbtable", "questions").option("user", "postgres").option("password", "xxx").save()

