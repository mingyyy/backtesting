import boto3
import pandas as pd
import pyarrow as pa
from s3fs import S3FileSystem
import pyarrow.parquet as pq


def csv2parquet(bucket_name='hist-price'):
    s3 = boto3.client('s3',region_name='us-east-1')

    files = list(boto3.resource('s3').Bucket(bucket_name).objects.all())
    for f in files:
        object_key = f.key
        obj = s3.get_object(Bucket=bucket_name, Key=object_key)
        df = pd.read_csv(obj['Body'])

        table = pa.Table.from_pandas(df)

        output_file = "s3://"+bucket_name+"/"+object_key
        s3 = S3FileSystem()

        pq.write_to_dataset(table=table,
                            root_path=output_file,partition_cols=['date'],
                            filesystem=s3)

        print("File converted from CSV to parquet completed")