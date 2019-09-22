import boto3
from secrete import bucket_simulation, bucket_upload
import time

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


    for f in files:
        object_key = f.key
        csv_obj = s3.get_object(Bucket=bucket_name, Key=object_key)
        # get the size of the files
        response = s3.head_object(Bucket=bucket_name, Key=object_key)
        size = response['ContentLength']
    #     csv_string.append(size)
    # return csv_string
        total_size += size
    return total_size


if __name__ == '__main__':
    # print(s3_loader("strategy-upload"))
    s = time.time()
    print(s3_file_sizes(bucket_simulation))
    e = time.time()
    p = str(e-s)
    print(p)
