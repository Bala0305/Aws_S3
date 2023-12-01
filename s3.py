import io
import os
import sys
import boto3
import pandas as pd
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

UPLOAD_CONFIG = TransferConfig(
    multipart_threshold= 100 * 1024 * 1024,
    max_concurrency = 10,
    num_download_attempts = 10,
    multipart_chunksize = 25 * 1024 * 1024,
    max_io_queue = 10000
)


class S3Manager:
  
    AwsAccesskeyId = ""
    AwsSecretAccessKey = ""
  
    def __init__(self):
        self.session = boto3.Session(region_name='ap-south-1')
        self.resource = boto3.resource('s3')
        self.client = self.connect()
        self.bucket_name = 'alab-project-x'

    def connect(self):
        from botocore.client import Config
        return self.session.client('s3',aws_access_key_id=AwsAccesskeyId,aws_secret_access_key=AwsSecretAccessKey,region_name='ap-south-1')

    def get_all_buckets(self):
        for bucket in self.resource.buckets.all():
            print(bucket.name)

    def upload_file_to_s3(self,file_name,s3_file_name):
        try:
            self.client.upload_file(Filename = file_name,Bucket = self.bucket_name,Key = s3_file_name,Config = UPLOAD_CONFIG)
        except ClientError as ex:
            print(ex)

    def download_file_from_s3(self,s3_file_name,file_name):
        try:
            self.client.download_file(self.bucket_name,s3_file_name,file_name,Config = UPLOAD_CONFIG)
        except ClientError as ex:
            print(ex)

    def list_objects_from_s3_bucket(self):
        objs =  self.client.list_objects(Bucket=self.bucket_name)
        for obj in objs['Contents']:
            print(obj)

    def get_s3_object(self,file_name):
        fileobj = self.client.get_object(Bucket = self.bucket_name,Key = file_name)
        return fileobj
    
    def read_csv_from_s3(self,file_name):
        fileobj = self.client.get_object(Bucket = self.bucket_name,Key = file_name)
        df = pd.read_csv(io.BytesIO(fileobj['Body'].read()))
        return df
    
    def read_excel_from_s3(self,file_name):
        fileobj = self.client.get_object(Bucket = self.bucket_name,Key = file_name)
        df = pd.read_excel(io.BytesIO(fileobj['Body'].read()))
        return df
    
    def delete_s3_file(self,file_name):
        self.client.delete_object(Bucket = self.bucket_name, Key = file_name)


if __name__ == "__main__":
    try:

        s3m = S3Manager()

        s3m.get_all_buckets()
       
        local_file_name = 'student.csv'
        s3_file_name = 'new_student.csv'

        s3m.upload_file_to_s3(local_file_name,s3_file_name)

        s3m.download_file_from_s3(s3_file_name,'download_test.csv')

        s3m.list_objects_from_s3_bucket()

        fileobj = s3m.get_s3_object(s3_file_name)

        csv_df = s3m.read_csv_from_s3(s3_file_name)

        s3m.delete_s3_file(s3_file_name)
        
        pass

        #createas a bucket
        # location = {'LocationConstraint': 'ap-south-1'}
        # s3m.client.create_bucket(Bucket = 'alab-project-x' , CreateBucketConfiguration=location)

    except Exception as e:
        print(str(e))


    pass
