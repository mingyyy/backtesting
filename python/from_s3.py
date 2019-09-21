import boto3


def s3_loader(bucket_name='hist-price'):
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
        body.read().decode('utf-8')


def s3_file_sizes(bucket_name):
    s3 = boto3.client('s3')
    resource = boto3.resource('s3')
    my_bucket = resource.Bucket(bucket_name)
    files = list(my_bucket.objects.all())
    csv_string = []

    for f in files:
        object_key = f.key
        csv_obj = s3.get_object(Bucket=bucket_name, Key=object_key)
        # get the size of the files
        response = s3.head_object(Bucket=bucket_name, Key=object_key)
        size = response['ContentLength']
        csv_string.append(size)
    return csv_string

if __name__ == '__main__':
    # print(s3_loader("strategy-upload"))
    print(s3_file_sizes('simulated-bucket'))
