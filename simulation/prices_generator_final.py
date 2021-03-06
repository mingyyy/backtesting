import pandas as pd
import boto3
import os
import glob
import merton_jump as mj
import numpy as np
import random
from secrete import bucket_simulation
import time


def price_generator(number_of_tickers=1, number_of_prices=100):
    for i in range(0, number_of_tickers):
        vol = max(np.random.normal(loc=0.3, scale=0.2, size=1), 0.05)
        mp = mj.ModelParameters(all_s0=random.randint(1, 1000)/10,
                             all_r0=0.52,
                             all_time=number_of_prices,
                             all_delta=0.04,
                             all_sigma=vol,
                             gbm_mu=0.058,
                             jumps_lamda=0.00325,
                             jumps_sigma=0.01,
                             jumps_mu=0.2)
        # create a list of stock with the random start and vol
        p_list = mj.geometric_brownian_motion_jump_diffusion_levels(mp)
        return p_list


def date_generator(start='1910-01-06', end='2019-09-20'):
    # get business days betwen 1910 and 2019
    days = pd.date_range(start, end, freq='B')
    list_days = []
    for day in days:
        x = day.strftime("%Y-%m-%d")
        list_days.append(x)
    return list_days


def ticker_generator(number_of_tickers):
    list_tickers = []
    i = 0
    a1='M'
    for a2 in list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'):
        for a3 in list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'):
            for a4 in list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'):
                ticker = 'SI' + a1 + a2 + a3 + a4
                list_tickers.append(ticker)
                i = i+1
                if i == number_of_tickers:
                    return list_tickers


def combine_files_large(list1, list2, list3, list4, list5, list6, list7, list8, list9, file_name):
    data_tuples = list(zip(list1, list2, list3, list4, list5, list6, list7, list8, list9))
    df = pd.DataFrame(data_tuples, columns=['date',
                                            'ticker',
                                            'sector',
                                            'adj_close', 'high', 'low', 'open', 'close',
                                            'volume'])
    # export to a csv file to a local folder
    df.to_csv(r'{}{}'.format('/home/ubuntu/output/', file_name), index=None, header=True)


def send2S3(file_name):
    # send to S3
    bucket_name = bucket_simulation
    resource = boto3.resource('s3')
    resource.Bucket(bucket_name).upload_file(Filename="/home/ubuntu/output/"+file_name, Key=file_name)

    # delete file in the folder
    files = glob.glob('/home/ubuntu/output/*')
    # n = 0
    for f in files:
       os.remove(f)


if __name__ == '__main__':
    timer_start = time.time()

    number_of_prices = 31186
    number_of_tickers = 17576
    t = 10000

    sector = ['FINANCE', 'CONSUMER SERVICES',
            'HEALTH CARE', 'TECHNOLOGY',
            'CAPITAL GOODS', 'ENERGY',
            'PUBLIC UTILITIES', 'BASIC INDUSTRIES',
            'CONSUMER NON-DURABLES', 'CONSUMER DURABLES',
            'MISCELLANEOUS', 'TRANSPORTATION']
    list_ticker = ticker_generator(number_of_tickers)
    for n in range(0, t):
        # timer_start = time.time()
        name_list = []
        slicer_start = (number_of_tickers/t)*n
        slicer_end = (number_of_tickers/t)*(n+1)
        for x in list_ticker[int(slicer_start) : int(slicer_end)]:
            file_name = 'si_{}.csv'.format(x)
            name_list.append(str(file_name))
            price_list = price_generator(number_of_tickers, number_of_prices)
            sector_list = number_of_prices*[random.choice(sector)]
            combine_files_large(date_generator(),
                          [x]*number_of_prices,
                          sector_list,
                          price_list,
                          price_list,
                          price_list,
                          price_list,
                          price_list,
                          [1000000]*number_of_prices,
                          file_name)

        combined_csv = pd.concat([pd.read_csv("/home/ubuntu/output/" + f) for f in name_list])
        combined_csv.to_csv(r'/home/ubuntu/output/SIM_' + str(n)+'.csv', index=False)
        send2S3('SIM_'+ str(n)+'.csv')

        # end = time.time() - timer_start
        # print(end)