import boto3, os, sys
import pandas as pd

# connect to S3
s3 = boto3.client('s3')

# connects to S3 using the default profile credentials and lists all the S3 buckets
# buckets = s3.list_buckets()
# for bucket in buckets['Buckets']:
#     print(bucket['CreationDate'].ctime(), bucket['Name'])

# # select bucket
# my_bucket = s3.Bucket('testing_ming')
# # download file into current directory
# for s3_object in my_bucket.objects.all():
#     # Need to split s3_object.key into path and file name, else it will give error file not found.
#     path, filename = os.path.split(s3_object.key)
#     my_bucket.download_file(s3_object.key, filename)

if sys.version_info[0] < 3:
    from StringIO import StringIO # Python 2.x
else:
    from io import StringIO # Python 3.x


bucket_name = 'testing-ming'

object_key = 'historical_stocks.csv'
csv_obj = s3.get_object(Bucket=bucket_name, Key=object_key)
body = csv_obj['Body']
csv_string = body.read().decode('utf-8')

df = pd.read_csv(StringIO(csv_string))
print(df.head())
