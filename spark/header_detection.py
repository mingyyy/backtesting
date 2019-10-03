
import sys
print(sys.path)


from pyspark.sql import SparkSession
from secrete import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
import subprocess
import datetime, time
# from create_table import connect_DB
from connect_s3 import *
from api_18080 import *



def quiet_logs(spark):
  logger = spark._jvm.org.apache.log4j
  logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def csv_to_tbl(from_bucket, app_name, file_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config('spark.sql.files.maxPartitionBytes', 1024 * 1024 * 128) \
        .config('spark.sql.shuffle.partitions', 200) \
        .getOrCreate()
    quiet_logs(spark)

    url = 'postgresql://10.0.0.9:5432/'
    properties = {'user': db_user_name, 'password': db_password, 'driver': 'org.postgresql.Driver'}

    # read in all csv files from this bucket to a single df
    df = spark.read.option("inferSchema", "true").csv("s3a://" + from_bucket + "/" + file_name, header=True)

    # tbl_name = 'Tbl_' + file_name.replace(' ', '_').split('.')[0]
    # # create table in postgres and insert into the tale
    # connect_DB(tbl_name, get_suggested_dict(df))
    # df.write.jdbc(url='jdbc:%s' % url, table=tbl_name, mode='overwrite', properties=properties)

    # update the schema table
    df = get_schema(spark, get_suggested_dict(df), file_name)
    tbl_name = 'tbl_schema'
    df.write.jdbc(url='jdbc:%s' % url, table=tbl_name, mode='overwrite', properties=properties)

    app_id = spark.sparkContext.applicationId
    print(app_id)
    get_history(app_id)

    # if check_jobs(app_id,'jobs') == 'ok':
    #     sms = 'All jobs succeeded!'
    #     print(sms)
    # else:
    #     print('Ohh, sorry! Something went wrong, please check the useful info below:')
    #     # save_to_bucket(check_stages(app_id, 'stages'), "log"+str(app_id))
    #     sms = check_stages(app_id, 'stages')
    # df_sms = spark.sparkContext.parallelize(sms).toDF("message")
    # df_sms = df_sms.withColumn('app_id', app_id)
    # tbl_name = 'tbl_sms'
    # df_sms.write.jdbc(url='jdbc:%s' % url, table=tbl_name, mode='append', properties=properties)


def get_history(app_id):

    if check_jobs(app_id,'jobs') == 'ok':
        sms = 'All jobs succeeded!'
        print(sms)
    else:
        print('Ohh, sorry! Something went wrong, please check the useful info below:')
        save_to_bucket(check_stages(app_id, 'stages'), "log_"+app_id)
    #     sms = check_stages(app_id, 'stages')
    # return sms


def get_schema(spark, dict, file_name):
    '''
    :param spark: entry point
    :param dict: dictionary of ticker and type
    :return:
    '''
    l=[]
    for name, type in dict.items():
        t = (file_name, name, type, type, 'admin001' )
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
    # first_n = df.limit(n).toPandas().to_dict(orient='list')
    original = {}
    suggested = {}

    for f in df.schema.fields:
        original[f.name] = f.dataType
        if isinstance(f.dataType, DateType):
            suggested[f.name] = 'date'
        elif isinstance(f.dataType, StringType):
            df = df.withColumn('length', F.length(F.col(f.name)))
            x = df.agg(F.max(df.length)).collect()[0][0]
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



    # command = 'cat helloworld'
    # process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    # output, error = process.communicate()


# def schema_finder(original,  rows):
#     final = {}
#     for name, type in original.items():
#         if type == DateType:
#             final[name] = check_date(rows[name], name)
#         elif type == StringType:
#             final[name] = check_string(rows[name], name)
#         else:
#             final[name] = type
#     return final
#
#
# def check_date(cols, name):
#     type_dict={}
#     for item in cols:
#         print(item)
#         print(isinstance(item, datetime.date))
#         if isinstance(item, datetime.date):
#             t = DateType
#         elif isinstance(item, datetime.datetime):
#             t = TimestampType
#         else:
#             t = check_string([item], name)
#         # type as key, count of items as value
#         if t in type_dict:
#             type_dict[t] += 1
#         else:
#             type_dict[t] = 1
#     # sort the types based on value
#     sorted_d = sorted(type_dict.items(), key=lambda kv: kv[1], reverse=True)
#     return sorted_d[0]
#
#
# def check_string(col, name):
#     type_list = []
#     for s in col:
#         if is_number_tryexcept(s):
#             type_list.append(FloatType)
#         else:
#             if isinstance(s, datetime.date):
#                 return DateType
#             elif isinstance(s, datetime.datetime):
#                 return TimestampType
#
#
# def is_number_tryexcept(s):
#     """ Returns True is string is a number. """
#     try:
#         float(s)
#         return True
#     except ValueError:
#         return False


if __name__ == '__main__':

    csv_to_tbl(bucket_prices, 'test schema search using historical prices', 'historical_stock_prices.csv')

