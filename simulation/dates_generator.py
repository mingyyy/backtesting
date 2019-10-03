import pandas as pd
import boto3
import uuid
import os
from secrete import bucket_simulation


def create_dates(start='1970-01-05', end='2018-12-20'):
    # get business days betwen 1970-01-05 and 2018-12-20
    days = pd.date_range(start, end, freq='B')
    list_days = []
    for day in days:
        x = day.strftime("%Y-%m-%d")
        list_days.append(x)
    df = pd.DataFrame(list_days, columns=['Date'])

    # export to a csv file to a local folder
    file_name = 'si_dates.csv'
    export_csv = df.to_csv(r'{}{}'.format('../output/', file_name), index=None, header=True)

    # send to S3
    bucket_name = bucket_simulation
    resource = boto3.resource('s3')
    resource.Bucket(bucket_name).upload_file(Filename="../output/"+file_name, Key=file_name)

    # delete file
    os.remove("../output/"+file_name)
    print(file_name+'is removed!')


def create_bucket_name(bucket_prefix):
    # The bucket name must be (3,63) char long and must be unqiue
    return ''.join([bucket_prefix, str(uuid.uuid4())])


def create_bucket(bucket_prefix, s3_connection):
    session = boto3.session.Session()
    current_region = session.region_name
    bucket_name = create_bucket_name(bucket_prefix)
    bucket_response = s3_connection.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
        'LocationConstraint': current_region})
    print(bucket_name, current_region)
    return bucket_name, bucket_response


if __name__ =='__main__':
    create_dates()