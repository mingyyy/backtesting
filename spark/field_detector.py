from pyspark.sql import SparkSession
from secrete import host, db_password, db_user_name, bucket_prices
from pyspark.sql.types import *
import pyspark.sql.functions as F
import datetime, time
from connect_s3 import save_to_bucket
from api_18080 import check_jobs, check_stages
from create_table import connect_DB


def quiet_logs(spark):
  logger = spark._jvm.org.apache.log4j
  logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def load_csv(from_bucket, app_name, file_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config('spark.sql.files.maxPartitionBytes', 1024 * 1024 * 128) \
        .config('spark.sql.shuffle.partitions', 200) \
        .getOrCreate()
    quiet_logs(spark)

    # read in all csv files from this bucket to a single df
    df = spark.read.option("inferSchema", "true").csv("s3a://" + from_bucket + "/" + file_name, header=True)

    url = 'postgresql://10.0.0.9:5432/'
    properties = {'user': db_user_name, 'password': db_password, 'driver': 'org.postgresql.Driver'}

    # create table in postgres and insert into the tale
    export_tbl(df, file_name, url, properties)

    # update the schema table
    tbl_name = 'tbl_schema'
    export_schema(spark, df, tbl_name, file_name, url, properties)


def export_schema(spark, df, tbl_name, file_name, url, properties):
    df = get_schema(spark, get_suggested_dict(df), file_name)
    df.write.jdbc(url='jdbc:%s' % url, table=tbl_name, mode='overwrite', properties=properties)

    app_id = spark.sparkContext.applicationId
    get_history(app_id)


def export_tbl(df, file_name, url, properties):
    tbl_name = 'Tbl_' + file_name.replace(' ', '_').split('.')[0]
    # create table in postgres
    connect_DB(tbl_name, get_suggested_dict(df))
    # write to the table
    df.write.jdbc(url='jdbc:%s' % url, table=tbl_name, mode='overwrite', properties=properties)


def get_schema(spark, dict, file_name):
    '''
    :param spark: entry point
    :param dict: dictionary of ticker and type
    :return:
    '''
    l=[]
    for name, field_type in dict.items():
        t = (file_name, name, field_type, field_type, 'admin001' )
        l.append(t)
    df = spark.createDataFrame(l, ['file_name','col_name','col_type_suggest', 'col_type_final', 'user_id'])
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    df = df.withColumn('create_date', F.unix_timestamp(F.lit(timestamp), 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
    return df


def get_suggested_dict(df):
    '''
    :param df: data frame
    :return: dictionary of suggested types in Postgres
    '''
    # ArrayType, BinaryType are not handled yet
    suggested = {}

    for f in df.schema.fields:
        if isinstance(f.dataType, DateType):
            suggested[f.name] = 'date'
        elif isinstance(f.dataType, StringType):
            df = df.withColumn('length', F.length(F.col(f.name)))
            x = df.agg(F.max(df.length)).collect()[0][0]
            # 20% extra length based on the longest string
            suggested[f.name] = 'varchar({})'.format(int(x*1.2))
        elif isinstance(f.dataType, DoubleType) or isinstance(f.dataType, DecimalType):
            suggested[f.name] = 'numeric(18,2)'
        elif isinstance(f.dataType, LongType):
            suggested[f.name] = 'int8'
        elif isinstance(f.dataType, FloatType):
            suggested[f.name] = 'float8'
        elif isinstance(f.dataType, ShortType):
            suggested[f.name] = 'integer'
        elif isinstance(f.dataType, BooleanType):
            suggested[f.name] = 'Bool'
        elif isinstance(f.dataType, TimestampType):
            suggested[f.name] = 'timestamptz'
    return suggested


def get_history(app_id):
    path = 'http://{}:18080/api/v1/applications/'.format(host)
    if check_jobs(path, app_id,'jobs') == 'ok':
        sms = 'All jobs succeeded!'
        print(sms)
    else:
        print('Ohh, sorry! Something went wrong, please check the useful info below:')
        save_to_bucket(check_stages(path, app_id, 'stages'), "log_"+app_id)


if __name__ == '__main__':
    load_csv(bucket_prices, 'test schema search using historical stocks', 'historical_stocks.csv')

