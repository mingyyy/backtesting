from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from secrete import db_password, end_point, db_name, db_user_name, bucket_parquet, bucket_prices


def strategy_1_all(bucket_name, file_name, sectors, target_no = 5):
    '''
    A naïve trading approach: buy at the beginning of each month if moving average price is less than previous close,
    PnL is calculated with the last close and purchase price
    :param target_no: the number of stocks purchased each Month
    :param sectors: the name of the sectors chosen
    :return: Output to postgres
    '''
    spark = SparkSession.builder \
        .master("spark://ip-10-0-0-5:7077") \
        .appName("testing JDBC") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    # load parquet file
    df = spark.read.option("inferSchema", "true").parquet("s3a://" + bucket_name + "/" + file_name)

    print((df.count(), len(df.columns)))
    df = df.drop('open', 'close', 'volume', 'high', 'low')
    df = df.withColumn("adj_close", df.adj_close.cast("double"))
    df.withColumn('maxN', F.when((df.adj_close < 0.001) | (df.adj_close > 100000000), 0)).drop('maxN')

    df = df.filter(df.sector.isin(sectors))
    df_output = df.select('ticker', 'adj_close')
    df_output.show(5)
    tbl_name = 'test'

    url = 'postgresql://10.0.0.9:5432/'
    properties = {'user': db_user_name, 'password': db_password, 'driver': 'org.postgresql.Driver'}
    df_output.write.jdbc(url='jdbc:%s' % url, table=tbl_name, mode='overwrite', properties=properties)


if __name__ == '__main__':
    # bucket_parquet contains SIO, SIB, SIC
    strategy_1_all(bucket_parquet, "simulate_G.parquet", ['HEALTH CARE'])

# spark-submit --master spark://<DNS>:7077  --jars postgresql-42.2.8.jar <path to file>
