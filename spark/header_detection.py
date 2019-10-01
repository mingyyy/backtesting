from pyspark.sql import SparkSession
from secrete import bucket_simulation, bucket_parquet, bucket_prices, bucket_large, bucket_larger, db_user_name, db_password
from pyspark.sql.types import StructType, StructField, \
    DateType, StringType, DoubleType, IntegerType,FloatType, NumericType, LongType, ShortType, \
    TimestampType, ArrayType, BinaryType, BooleanType, DecimalType
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import subprocess
import datetime
import operator
from create_table import connect_DB


def quiet_logs(spark):
  logger = spark._jvm.org.apache.log4j
  logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def load_files(from_bucket, app_name, file_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config('spark.sql.files.maxPartitionBytes', 1024 * 1024 * 128) \
        .config('spark.sql.shuffle.partitions', 200) \
        .getOrCreate()
    quiet_logs(spark)

    # read in all csv files from this bucket to a single df
    df = spark.read.option("inferSchema", "true").csv("s3a://" + from_bucket + "/" + file_name, header=True)
    # types = [f.dataType for f in df.schema.fields]
    # types = [DateType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType]
    # fieldnames = [f.name for f in df.schema.fields]
    # fieldnames = ['date', 'ticker', 'sector', 'adj_close', 'high', 'low', 'open', 'close', 'volume']

    tbl_name = 'TBL_SCHEMA_' + file_name.replace(' ','_').split('.')[0]
    connect_DB(tbl_name, get_suggested(df))

    url = 'postgresql://10.0.0.9:5432/'
    properties = {'user': db_user_name, 'password': db_password, 'driver': 'org.postgresql.Driver'}
    df.write.jdbc(url='jdbc:%s' % url, table=tbl_name, mode='overwrite', properties=properties)


def get_suggested(df):
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
        if f.dataType == DateType:
            suggested[f.name] = 'date'
        elif f.dataType == StringType:
            df = df.withColumn('length', F.length(F.col(f.name)))
            x = df.agg(F.max(df.length)).collect()[0][0]
            suggested[f.name] = 'varchar({})'.format(x)
        elif f.dataType == DoubleType or f.dataType == DecimalType or f.dataType == NumericType:
            suggested[f.name] = 'numeric(18,2)'
        elif f.dataType == LongType:
            suggested[f.name] = 'float4'
        elif f.dataType == FloatType:
            suggested[f.name] = 'float8'
        elif f.dataType == ShortType:
            suggested[f.name] = 'integer'
        elif f.dataType == BooleanType:
            suggested[f.name] = 'Bool'
        elif f.dataType == TimestampType:
            suggested[f.name] = 'timestamp'
    return suggested

    # original = {'ticker': StringType, 'open': DoubleType, 'close': DoubleType, 'adj_close': DoubleType, 'low': DoubleType,
    #  'high': DoubleType, 'volume': LongType, 'date': TimestampType}
    # first_n ={'ticker': ['AHH', 'AHH'], 'open': [11.5, 11.6599998474121], 'close': [11.5799999237061, 11.5500001907349],
    #  'adj_close': [8.49315452575684, 8.47115135192871], 'low': [11.25, 11.5],
    #  'high': [11.6800003051758, 11.6599998474121], 'volume': [4633900, 275800],
    #  'date': [Timestamp('2013-05-08 00:00:00'), Timestamp('2013-05-09 00:00:00')]}



    # command = 'cat helloworld'
    # process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    # output, error = process.communicate()


    # tbl_name = 'test_header'

    # pushdown_query = "create table {} ()".format(tbl_name)
    # spark.read.jdbc(url='jdbc:%s' % url, table=pushdown_query, properties=properties)

    # return types, fieldnames


def schema_finder(original,  rows):
    final = {}
    for name, type in original.items():
        if type == DateType:
            final[name] = check_date(rows[name], name)
        elif type == StringType:
            final[name] = check_string(rows[name], name)
        else:
            final[name] = type

    return final


def check_date(cols, name):
    type_dict={}
    for item in cols:
        print(item)
        print(isinstance(item, datetime.date))
        if isinstance(item, datetime.date):
            t = DateType
        elif isinstance(item, datetime.datetime):
            t = TimestampType
        else:
            t = check_string([item], name)
        # type as key, count of items as value
        if t in type_dict:
            type_dict[t] += 1
        else:
            type_dict[t] = 1
    # sort the types based on value
    sorted_d = sorted(type_dict.items(), key=lambda kv: kv[1], reverse=True)
    return sorted_d[0]


def check_string(col, name):
    type_list = []
    for s in col:
        if is_number_tryexcept(s):
            type_list.append(FloatType)
        else:
            if isinstance(s, datetime.date):
                return DateType
            elif isinstance(s, datetime.datetime):
                return TimestampType


def is_number_tryexcept(s):
    """ Returns True is string is a number. """
    try:
        float(s)
        return True
    except ValueError:
        return False



if __name__ == '__main__':
    s = load_files(bucket_prices, 'test schema search using historcial prices', 'historical_stocks.csv')
    print(s)
