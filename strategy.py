import boto3, os, sys
import pandas as pd
from datetime import datetime as dt

# connect to S3, with boto3: high-level object-oriented API
# s3 = boto3.client('s3')
# resource = boto3.resource('s3')

# connects to S3 using the default profile credentials and lists all the S3 buckets
# buckets = s3.list_buckets()
# for bucket in buckets['Buckets']:
#     print(bucket['CreationDate'].ctime(), bucket['Name'])

# if sys.version_info[0] < 3:
#     from StringIO import StringIO # Python 2.x
# else:
#     from io import StringIO # Python 3.x
#
# bucket_name = 'hist-price'
# my_bucket = resource.Bucket(bucket_name)
# files = list(my_bucket.objects.all())
#
# for f in files:
#     file_name = f.key
#     object_key = file_name
#     csv_obj = s3.get_object(Bucket=bucket_name, Key=object_key)
#     body = csv_obj['Body']
#     csv_string = body.read().decode('utf-8')
path = "./daily-historical-stock-prices-1970-2018/"

df_price = pd.read_csv(path+'historical_stock_prices.csv', nrows=200)
df_stock = pd.read_csv(path+'historical_stocks.csv')


def s_buy_hold(stocks, start_date, end_date, invest=100, frequency=30):
    # strategy 1. buy invest amount at start_date, calculate the PnL at end_date
    pnl = 0
    if dt.strptime(end_date, "%Y-%m-%d") <= dt.strptime(start_date, "%Y-%m-%d"):
        return 'You end date is before your start date!'
    if len(stocks) == 0:
        return 'You stock ticker is out of scope.'
    for stock in stocks:
        select = df_price[df_price['ticker'] == stock][['date', 'adj_close']].copy()
        if start_date in select['date'].values:
            p0=select['adj_close'][select['date']==start_date].values[0]
            if p0 == 0:
                return "Starting price is zero."
            else:
                q = invest/p0
        else:
            return 'start date or ticker doesn\'t exist'
        if end_date in select['date'].values:
            p1=select['adj_close'][select['date']==end_date].values[0]
            pnl = q * (p1 - p0)
        else:
            return 'end date out of scope.'
    return pnl


if __name__ == "__main__":
    # print(df_price.head())
    
    pnl = s_buy_hold(['AHH'], '2013-05-08', '2013-05-14')
    if type(pnl) is float:
        print(f'Pnl of your trade is {pnl:.2f}')
    else:
        print(pnl)

