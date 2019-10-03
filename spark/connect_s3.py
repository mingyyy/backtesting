import boto3
from secrete import *


def s3_loader(bucket_name):
    # connect to S3, with boto3: high-level object-oriented API
    s3 = boto3.client('s3')
    resource = boto3.resource('s3')

    # connects to S3 using the default profile credentials and lists all the S3 buckets
    # buckets = s3.list_buckets()
    # for bucket in buckets['Buckets']:
    #     print(bucket['CreationDate'].ctime(), bucket['Name'])

    my_bucket = resource.Bucket(bucket_name)
    files = list(my_bucket.objects.all())

    for f in files:
        # get all file names in this bucket
        object_key = f.key
        csv_obj = s3.get_object(Bucket=bucket_name, Key=object_key)
        body = csv_obj['Body']
        # read in as string
        # df=body.read().decode('utf-8')
        # read in as 'bytes'
        df = body.read()
        print(type(df))



def s3_file_sizes(bucket_name):
    ''' Get the size of each file in a bucket, print a list'''
    s3 = boto3.client('s3')
    resource = boto3.resource('s3')
    my_bucket = resource.Bucket(bucket_name)
    files = list(my_bucket.objects.all())
    # csv_string = []
    total_size=0

    i=0
    for f in files:
        object_key = f.key
        csv_obj = s3.get_object(Bucket=bucket_name, Key=object_key)
        # get the size of the files
        response = s3.head_object(Bucket=bucket_name, Key=object_key)
        size = response['ContentLength']
    #     csv_string.append(size)
    # return csv_string
        total_size += size
        i += 1
        if i >=10:
            return total_size
    return total_size


def copy_to_bucket(bucket_from_name, bucket_to_name, file_name):
    copy_source = {
        'Bucket': bucket_from_name,
        'Key': file_name
    }
    resource = boto3.resource('s3')
    resource.Object(bucket_to_name, file_name).copy(copy_source)


def delete_from_bucket(bucket_name, file_name):
    resource = boto3.resource('s3')
    resource.Object(bucket_name, file_name).delete()


def save_to_bucket(data,filename):
    s3 = boto3.resource('s3')
    # turn to binary data
    if data is None:
        data = 'Succeeded'
    #     binary_data = ''.join(format(ord(x), 'b') for x in data)
    # else:
    #     binary_data = ''.join(format(ord(x), 'b') for x in "Succeeded")
    object = s3.Object(bucket_hist, filename+'.txt')
    object.put(Body=data)


if __name__ == '__main__':
    # print(s3_loader("strategy-upload"))
    # s = time.time()
    # print(s3_file_sizes(bucket_simulation))
    # e = time.time()
    # p = str(e-s)
    # print(p)

    # copy from bucket 1 to bucket 2
    copy_to_bucket('simulated-bucket', 'simulated-parquet', 'simulated01.parquet')
