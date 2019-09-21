import pandas as pd
import boto3
import os
import merton_jump as mj
import numpy as np
import random


def price_generator(number_of_tickers=1, number_of_prices=100):

    for i in range(0, number_of_tickers):
        vol = max(np.random.normal(loc=0.3, scale=0.2, size=1), 0.05)
        mp = mj.ModelParameters(all_s0=random.randint(1, 1000)/10,
                             all_r0=0.5,
                             all_time=number_of_prices,
                             all_delta=0.004,
                             all_sigma=vol,
                             gbm_mu=0.058,
                             jumps_lamda=0.00125,
                             jumps_sigma=0.01,
                             jumps_mu=0.2)
        # create a list of stock with the random start and vol
        p_list = mj.geometric_brownian_motion_jump_diffusion_levels(mp)
        return p_list
        # p = jump_diffusion_examples.append(mj.geometric_brownian_motion_jump_diffusion_levels(mp))


def date_generator(start='1970-01-05', end='2018-12-20'):
    # get business days betwen 1970-01-05 and 2018-12-20
    days = pd.date_range(start, end, freq='B')
    list_days = []
    for day in days:
        x = day.strftime("%Y-%m-%d")
        list_days.append(x)
    return list_days


def ticker_generator(number_of_tickers):
    # Generate a list of unique 6 digits tickers, all start with SIM
    dict_ticker = {}
    for i in range(0, number_of_tickers):
        flag = False
        while flag is False:
            ticker = 'SIM'+ \
                     random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')+\
                     random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')+\
                     random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
            if ticker not in dict_ticker:
                dict_ticker[ticker] = 1
                flag = True
    list_tickers = []
    for k in dict_ticker:
        list_tickers.append(k)
    return list_tickers


def combine_files(list1, list2, list3, file_name):
    data_tuples = list(zip(list1, list2, list3))
    df = pd.DataFrame(data_tuples, columns=['date', 'adj_close', 'ticker'])

    # export to a csv file to a local folder
    df.to_csv(r'{}{}'.format('../output/', file_name), index=None, header=True)


def send2S3(file_name):
    # send to S3
    bucket_name = 'simulated-bucket'
    resource = boto3.resource('s3')
    resource.Bucket(bucket_name).upload_file(Filename="../output/"+file_name, Key=file_name)

    # delete file
    os.remove("../output/"+file_name)
    print(file_name+' is removed!')


if __name__ == '__main__':
    number_of_prices = 12776
    number_of_tickers=15000
    for x in ticker_generator(number_of_tickers):
        file_name = 'si_{}.csv'.format(x)
        combine_files(date_generator(),
                      price_generator(number_of_tickers, number_of_prices),
                      [x]*number_of_prices,
                      file_name)
        send2S3(file_name)

