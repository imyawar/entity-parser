import boto3
from botocore.exceptions import ClientError
import logging
import os


# S3 Utils class for handling S3 operations
class S3Utils:
    def __init__(self, bucket_name):
        self.s3_client = boto3.client('s3')
        self.bucket_name = bucket_name

    def list(self, s3_path):
        keys = []

        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket_name, Prefix=s3_path)

        for page in pages:
            for obj in page['Contents']:
                keys.append(obj['Key'].replace(s3_path+"/", ''))

        return keys

    def file_exists(self, s3_path, file_name):
        try:
            key = os.path.join(s3_path, file_name)
            self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError:
            return False

    def read_file(self, s3_path, file_name):
        key = os.path.join(s3_path, file_name)
        obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
        return obj['Body'].read().decode('utf-8')

    def upload_object(self, file_name, s3_path):
        try:
            response = self.s3_client.upload_file(file_name, self.bucket_name, s3_path)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def download_object(self, s3_path, file_name):
        try:
            os.makedirs(os.path.dirname(file_name), exist_ok=True)
            self.s3_client.download_file(self.bucket_name, s3_path, file_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def write_file(self, s3_path, file_name, content):
        try:
            key = os.path.join(s3_path, file_name)
            self.s3_client.put_object(Bucket=self.bucket_name, Key=key, Body=content)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def append_to_file(self, s3_path, file_name, content):
        try:
            content_old = self.read_file(s3_path, file_name)
            content_old = content_old + "\n"
        except ClientError as e:
            content_old = ""
        self.write_file(s3_path, file_name, content_old+content)
